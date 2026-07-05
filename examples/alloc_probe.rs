//! Idle-connection heap decomposition probe — a root-less DHAT substitute.
//!
//! Wraps the global allocator in a size-class histogram, boots a real broker
//! in-process, opens N idle MQTT connections against it, and prints the
//! per-connection *live-heap* delta broken down by allocation size class —
//! separating true heap cost from allocator/page overhead (the RSS gap).
//!
//! Run: `cargo run --release --example alloc_probe -- 2000`

// This probe is a deliberately multi-threaded test harness — it hosts the broker
// on one thread and drives clients from the main thread — so the crate-wide
// thread-per-core lints (which forbid `std::thread`) don't apply to it.
#![allow(clippy::disallowed_methods)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicIsize, Ordering::Relaxed};
use std::time::Duration;

/// Size-class upper bounds; the last bucket is a catch-all.
const BOUNDS: [usize; 20] =
	[16, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024, 1536, 2048, 3072, 4096, 6144, 8192, 16384, usize::MAX];
const NB: usize = BOUNDS.len();

struct Histo {
	live_bytes: AtomicIsize,
	live_allocs: AtomicIsize,
	count: [AtomicIsize; NB],
	bytes: [AtomicIsize; NB],
}

static HISTO: Histo = Histo {
	live_bytes: AtomicIsize::new(0),
	live_allocs: AtomicIsize::new(0),
	count: [const { AtomicIsize::new(0) }; NB],
	bytes: [const { AtomicIsize::new(0) }; NB],
};

fn bucket(size: usize) -> usize {
	BOUNDS.iter().position(|b| size <= *b).unwrap_or(NB - 1)
}

fn record(size: usize, sign: isize) {
	let b = bucket(size);
	HISTO.live_bytes.fetch_add(sign * size as isize, Relaxed);
	HISTO.live_allocs.fetch_add(sign, Relaxed);
	HISTO.count[b].fetch_add(sign, Relaxed);
	HISTO.bytes[b].fetch_add(sign * size as isize, Relaxed);
}

struct Prober;

// SAFETY: delegates to `System` verbatim; the bookkeeping is atomic counters
// only (no allocation), so there is no reentrancy.
unsafe impl GlobalAlloc for Prober {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		let p = unsafe { System.alloc(layout) };
		if !p.is_null() {
			record(layout.size(), 1);
		}
		p
	}
	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		unsafe { System.dealloc(ptr, layout) };
		record(layout.size(), -1);
	}
	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		let p = unsafe { System.realloc(ptr, layout, new_size) };
		if !p.is_null() {
			record(layout.size(), -1);
			record(new_size, 1);
		}
		p
	}
}

#[global_allocator]
static PROBER: Prober = Prober;

fn snapshot() -> (isize, isize, [isize; NB], [isize; NB]) {
	let mut count = [0isize; NB];
	let mut bytes = [0isize; NB];
	for i in 0..NB {
		count[i] = HISTO.count[i].load(Relaxed);
		bytes[i] = HISTO.bytes[i].load(Relaxed);
	}
	(
		HISTO.live_bytes.load(Relaxed),
		HISTO.live_allocs.load(Relaxed),
		count,
		bytes,
	)
}

fn rss_kb() -> usize {
	let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
	status
		.lines()
		.find(|l| l.starts_with("VmRSS:"))
		.and_then(|l| l.split_whitespace().nth(1))
		.and_then(|v| v.parse().ok())
		.unwrap_or(0)
}

/// A minimal MQTT 5 CONNECT for `client_id` (clean start, keep-alive 300 s).
fn connect_packet(client_id: &str) -> Vec<u8> {
	let mut vh = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x01, 0x2c, 0x00];
	vh.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
	vh.extend_from_slice(client_id.as_bytes());
	let mut pkt = vec![0x10, vh.len() as u8]; // remaining length < 128 for short ids
	pkt.extend_from_slice(&vh);
	pkt
}

fn open_idle_conn(port: u16, i: usize) -> TcpStream {
	let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
	s.write_all(&connect_packet(&format!("p{i}")))
		.expect("send CONNECT");
	let mut ack = [0u8; 64];
	s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
	let n = s.read(&mut ack).expect("read CONNACK");
	assert!(n >= 4 && ack[0] >> 4 == 2, "expected CONNACK");
	s
}

fn main() {
	let n: usize = std::env::args()
		.nth(1)
		.and_then(|a| a.parse().ok())
		.unwrap_or(1000);
	let port = 1897;

	let mut config = rusquitto::config::Config::default();
	config.server.bind = "127.0.0.1".parse().unwrap();
	config.server.port = port;
	config.server.websocket = false;
	config.runtime.cores = Some(1);
	config.logging.level = "error".into();
	config.logging.dir = std::env::temp_dir().join("allocprobe-logs");

	std::thread::spawn(move || rusquitto::run(config).expect("broker run"));

	// Wait until the broker accepts, then warm up so lazily-initialised broker
	// state (interner, registries, reactor pools) is faulted in before baseline.
	std::thread::sleep(Duration::from_millis(500));
	let warm: Vec<TcpStream> = (0..50).map(|i| open_idle_conn(port, 900_000 + i)).collect();
	drop(warm);
	std::thread::sleep(Duration::from_millis(800));

	let (base_bytes, base_allocs, base_count, base_size) = snapshot();
	let base_rss = rss_kb();

	let _conns: Vec<TcpStream> = (0..n).map(|i| open_idle_conn(port, i)).collect();
	std::thread::sleep(Duration::from_millis(1500));

	let (bytes, allocs, count, size) = snapshot();
	let rss = rss_kb();

	println!("== idle heap decomposition ({n} connections) ==");
	println!(
		"live heap:  {:+.1} KiB/conn   ({:+.1} allocations/conn)",
		(bytes - base_bytes) as f64 / n as f64 / 1024.0,
		(allocs - base_allocs) as f64 / n as f64,
	);
	println!(
		"process RSS: {:+.1} KiB/conn  (RSS - heap = allocator/page overhead)",
		(rss - base_rss) as f64 / n as f64,
	);
	println!("\nper-connection live delta by size class:");
	println!(
		"{:>10}  {:>12}  {:>14}",
		"class <=", "allocs/conn", "bytes/conn"
	);
	for i in 0..NB {
		let dc = (count[i] - base_count[i]) as f64 / n as f64;
		let db = (size[i] - base_size[i]) as f64 / n as f64;
		if dc.abs() > 0.005 || db.abs() > 4.0 {
			let label = if BOUNDS[i] == usize::MAX {
				">16384".to_string()
			} else {
				format!("{}", BOUNDS[i])
			};
			println!("{label:>10}  {dc:>12.2}  {db:>14.1}");
		}
	}
}
