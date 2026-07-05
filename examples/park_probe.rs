//! Parked-connection memory-floor spike.
//!
//! Answers one question with a hard number: if an idle connection dropped its
//! per-connection async task + io_uring read `Source` (the ~1.9 KiB glommio floor
//! the `alloc_probe` histogram attributes to them) and instead lived as a minimal
//! struct on a *shared* readiness ring, what would it cost?
//!
//! It does exactly that — no glommio, no task per connection: accepts N sockets,
//! hands each to a minimal [`ParkedConn`], and arms one `IORING_OP_POLL_ADD` per
//! fd on a single shared ring. Then it measures live heap + RSS per parked
//! connection, and proves the wake path: when a client sends a byte, the ring
//! delivers a completion naming that connection.
//!
//! Run: `cargo run --release --example park_probe -- 2000`
//!
//! This is a feasibility measurement, not production code — it deliberately uses
//! `std::thread` for the client side, so the crate-wide thread-per-core lints
//! don't apply.
#![allow(clippy::disallowed_methods)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::{IntoRawFd, RawFd};
use std::sync::atomic::{AtomicIsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};

// --- live-heap probe (same technique as alloc_probe) -------------------------

struct Histo {
	live_bytes: AtomicIsize,
	live_allocs: AtomicIsize,
}
static HISTO: Histo = Histo {
	live_bytes: AtomicIsize::new(0),
	live_allocs: AtomicIsize::new(0),
};

struct Prober;
// SAFETY: delegates to `System`; bookkeeping is atomic counters only (no alloc).
unsafe impl GlobalAlloc for Prober {
	unsafe fn alloc(&self, l: Layout) -> *mut u8 {
		let p = unsafe { System.alloc(l) };
		if !p.is_null() {
			HISTO.live_bytes.fetch_add(l.size() as isize, Relaxed);
			HISTO.live_allocs.fetch_add(1, Relaxed);
		}
		p
	}
	unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
		unsafe { System.dealloc(p, l) };
		HISTO.live_bytes.fetch_add(-(l.size() as isize), Relaxed);
		HISTO.live_allocs.fetch_add(-1, Relaxed);
	}
	unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
		let q = unsafe { System.realloc(p, l, n) };
		if !q.is_null() {
			HISTO
				.live_bytes
				.fetch_add(n as isize - l.size() as isize, Relaxed);
		}
		q
	}
}
#[global_allocator]
static PROBER: Prober = Prober;

fn rss_kb() -> usize {
	std::fs::read_to_string("/proc/self/status")
		.unwrap_or_default()
		.lines()
		.find(|l| l.starts_with("VmRSS:"))
		.and_then(|l| l.split_whitespace().nth(1))
		.and_then(|v| v.parse().ok())
		.unwrap_or(0)
}

/// The minimal resident state a real broker would keep for a *parked* (online but
/// idle) connection: its fd, identity, session-takeover generation, and
/// keep-alive deadline. Subscriptions already live in the shared trie keyed by
/// client id, so they are not duplicated here.
// `generation` and `deadline` are unread in the spike but are part of the real
// footprint a broker would keep, so they stay in the struct we measure.
#[allow(dead_code)]
struct ParkedConn {
	fd: RawFd,
	client_id: Box<str>,
	generation: u64,
	deadline: Option<Instant>,
}

fn main() {
	let n: usize = std::env::args()
		.nth(1)
		.and_then(|a| a.parse().ok())
		.unwrap_or(2000);
	let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
	let port = listener.local_addr().unwrap().port();

	// One shared readiness ring for *all* parked fds — the whole point.
	let mut ring = IoUring::new(8192).expect("io_uring");

	// Client side: one thread opens N connections and holds them, then (on signal)
	// sends a byte on each so we can prove the ring wakes for the right fd.
	let (tx, rx) = std::sync::mpsc::channel::<()>();
	let client = std::thread::spawn(move || {
		// 50 extra for the server's warm-up accepts, then n to be parked.
		let mut socks: Vec<TcpStream> = (0..n + 50)
			.map(|_| TcpStream::connect(("127.0.0.1", port)).expect("connect"))
			.collect();
		rx.recv().ok(); // wait until the server has parked everyone
		for s in &mut socks {
			let _ = s.write_all(b"x");
		}
		std::thread::sleep(Duration::from_millis(500));
		socks
	});

	// Warm up so lazily-faulted allocations are in before the baseline.
	let mut warm = Vec::new();
	for _ in 0..50 {
		let (s, _) = listener.accept().expect("accept warm");
		warm.push(s);
	}
	drop(warm);
	std::thread::sleep(Duration::from_millis(100));

	let base_bytes = HISTO.live_bytes.load(Relaxed);
	let base_allocs = HISTO.live_allocs.load(Relaxed);
	let base_rss = rss_kb();

	// Accept N, park each: extract the fd (freeing the std stream wrapper, keeping
	// the socket open), store the minimal struct, and arm one POLL_ADD on the ring.
	let mut parked: Vec<ParkedConn> = Vec::with_capacity(n);
	for i in 0..n {
		let (stream, _) = listener.accept().expect("accept");
		stream.set_nonblocking(true).ok();
		let fd = stream.into_raw_fd();
		parked.push(ParkedConn {
			fd,
			client_id: format!("sensor-{i}").into_boxed_str(),
			generation: i as u64,
			deadline: Some(Instant::now() + Duration::from_secs(60)),
		});
		let poll = opcode::PollAdd::new(types::Fd(fd), libc::POLLIN as u32)
			.build()
			.user_data(i as u64);
		// Submit in batches so the submission queue never overflows.
		if unsafe { ring.submission().push(&poll) }.is_err() {
			ring.submit().expect("submit");
			unsafe { ring.submission().push(&poll).expect("push after drain") };
		}
	}
	ring.submit().expect("final submit");

	let bytes = HISTO.live_bytes.load(Relaxed);
	let allocs = HISTO.live_allocs.load(Relaxed);
	let rss = rss_kb();

	println!("== parked-connection floor ({n} connections, one shared readiness ring) ==");
	println!(
		"live heap:   {:+.2} KiB/conn   ({:+.2} allocations/conn)",
		(bytes - base_bytes) as f64 / n as f64 / 1024.0,
		(allocs - base_allocs) as f64 / n as f64,
	);
	println!(
		"process RSS: {:+.2} KiB/conn   (incl. kernel socket + poll state via page delta)",
		(rss - base_rss) as f64 / n as f64,
	);
	println!(
		"sizeof(ParkedConn) = {} B (struct only; client_id heap is separate)",
		size_of::<ParkedConn>()
	);

	// Prove the wake path: signal the clients to send, then reap completions.
	tx.send(()).ok();
	let mut woken = 0usize;
	let deadline = Instant::now() + Duration::from_secs(5);
	while woken < n && Instant::now() < deadline {
		if ring.submit_and_wait(1).is_err() {
			break;
		}
		for cqe in ring.completion() {
			let idx = cqe.user_data() as usize;
			// Drain the byte so the socket is quiescent again (a real broker would
			// now spawn a task to parse the frame, then re-park).
			let mut buf = [0u8; 8];
			let _ = unsafe { libc::read(parked[idx].fd, buf.as_mut_ptr().cast(), buf.len()) };
			woken += 1;
		}
	}
	println!("wake proof: {woken}/{n} parked fds delivered a readiness completion naming their connection");

	for p in &parked {
		unsafe { libc::close(p.fd) };
	}
	client.join().ok();
}
