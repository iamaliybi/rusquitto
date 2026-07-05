//! rusquitto throughput hammer — dependency-free (std only).
//!
//! Compile:  rustc -O stresser.rs -o stresser
//! Run:      ./stresser 127.0.0.1:1883 --connections 2000 --duration 15 --qos 1
//!
//! Each connection runs on its own thread with a small stack and hammers PUBLISH
//! as fast as the broker will take it. A compiled, many-threaded client is the
//! right tool to expose cross-core contention, starvation, or head-of-line
//! blocking in a thread-per-core broker that a single-threaded async client can't.
//!
//! Built as a Cargo example (`cargo build --release --example stresser`) so it
//! gets `fmt`/`clippy`/CI coverage, though it stays dependency-free and also
//! compiles standalone with `rustc -O stress/stresser.rs`.

// This is deliberately a many-threaded load generator — the crate-wide
// thread-per-core lints (which forbid `std::thread`) are the broker's rule, not
// this client's.
#![allow(clippy::disallowed_methods)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

// --- minimal MQTT 5 encoding -------------------------------------------------

fn varint(mut n: usize, out: &mut Vec<u8>) {
	loop {
		let mut b = (n % 128) as u8;
		n /= 128;
		if n > 0 {
			b |= 0x80;
		}
		out.push(b);
		if n == 0 {
			break;
		}
	}
}

fn mqtt_str(s: &[u8], out: &mut Vec<u8>) {
	out.extend_from_slice(&(s.len() as u16).to_be_bytes());
	out.extend_from_slice(s);
}

fn connect(client_id: &str) -> Vec<u8> {
	let mut vh = Vec::new();
	mqtt_str(b"MQTT", &mut vh);
	vh.push(5); // protocol level
	vh.push(0x02); // clean start
	vh.extend_from_slice(&0u16.to_be_bytes()); // keep-alive 0
	vh.push(0); // properties length
	mqtt_str(client_id.as_bytes(), &mut vh); // payload: client id

	let mut pkt = vec![0x10];
	varint(vh.len(), &mut pkt);
	pkt.extend_from_slice(&vh);
	pkt
}

fn publish(topic: &str, payload: &[u8], qos: u8, pkid: u16) -> Vec<u8> {
	let mut vh = Vec::new();
	mqtt_str(topic.as_bytes(), &mut vh);
	if qos > 0 {
		vh.extend_from_slice(&pkid.to_be_bytes());
	}
	vh.push(0); // properties length
	vh.extend_from_slice(payload);

	let mut pkt = vec![0x30 | (qos << 1)];
	varint(vh.len(), &mut pkt);
	pkt.extend_from_slice(&vh);
	pkt
}

/// Read one packet's fixed header + body; returns the packet type nibble.
fn read_packet(stream: &mut TcpStream) -> std::io::Result<u8> {
	let mut b0 = [0u8; 1];
	stream.read_exact(&mut b0)?;
	let mut mult = 1usize;
	let mut len = 0usize;
	for _ in 0..4 {
		let mut eb = [0u8; 1];
		stream.read_exact(&mut eb)?;
		len += (eb[0] & 0x7F) as usize * mult;
		if eb[0] & 0x80 == 0 {
			break;
		}
		mult *= 128;
	}
	let mut body = vec![0u8; len];
	stream.read_exact(&mut body)?;
	Ok(b0[0] >> 4)
}

// --- worker ------------------------------------------------------------------

fn worker(addr: String, qos: u8, payload: Arc<Vec<u8>>, stop: Arc<AtomicBool>, count: Arc<AtomicU64>, id: usize) {
	let mut stream = match TcpStream::connect(&addr) {
		Ok(s) => s,
		Err(_) => return,
	};
	let _ = stream.set_nodelay(true);
	if stream.write_all(&connect(&format!("hammer-{id}"))).is_err() {
		return;
	}
	if read_packet(&mut stream).is_err() {
		return; // CONNACK
	}

	let mut pkid: u16 = 1;
	let mut local: u64 = 0;
	while !stop.load(Ordering::Relaxed) {
		if stream
			.write_all(&publish("bench/topic", &payload, qos, pkid))
			.is_err()
		{
			break;
		}
		if qos > 0 {
			match read_packet(&mut stream) {
				Ok(5) => {
					// QoS 2: got PUBREC -> send PUBREL, await PUBCOMP.
					let mut pubrel = vec![0x62, 0x02];
					pubrel.extend_from_slice(&pkid.to_be_bytes());
					if stream.write_all(&pubrel).is_err() || read_packet(&mut stream).is_err() {
						break;
					}
				}
				Ok(_) => {} // PUBACK
				Err(_) => break,
			}
		}
		local += 1;
		pkid = pkid.wrapping_add(1).max(1);
		if local.is_multiple_of(1024) {
			count.fetch_add(1024, Ordering::Relaxed);
			local = 0;
		}
	}
	count.fetch_add(local, Ordering::Relaxed);
}

// --- main --------------------------------------------------------------------

fn arg(args: &[String], key: &str, default: &str) -> String {
	args.iter()
		.position(|a| a == key)
		.and_then(|i| args.get(i + 1))
		.cloned()
		.unwrap_or_else(|| default.to_string())
}

fn main() {
	let args: Vec<String> = std::env::args().collect();
	let addr = args
		.get(1)
		.cloned()
		.unwrap_or_else(|| "127.0.0.1:1883".to_string());
	let connections: usize = arg(&args, "--connections", "1000").parse().unwrap_or(1000);
	let duration: u64 = arg(&args, "--duration", "10").parse().unwrap_or(10);
	let qos: u8 = arg(&args, "--qos", "0").parse().unwrap_or(0);
	let payload_bytes: usize = arg(&args, "--payload", "64").parse().unwrap_or(64);

	println!("hammer -> {addr}: {connections} conns, QoS {qos}, {payload_bytes}B payload, {duration}s");

	let payload = Arc::new(vec![0x5Au8; payload_bytes]);
	let stop = Arc::new(AtomicBool::new(false));
	let count = Arc::new(AtomicU64::new(0));

	let start = Instant::now();
	let mut handles = Vec::with_capacity(connections);
	for id in 0..connections {
		let (addr, payload, stop, count) = (addr.clone(), payload.clone(), stop.clone(), count.clone());
		let builder = std::thread::Builder::new().stack_size(256 * 1024);
		if let Ok(h) = builder.spawn(move || worker(addr, qos, payload, stop, count, id)) {
			handles.push(h);
		}
	}

	// Live progress once a second.
	for _ in 0..duration {
		std::thread::sleep(Duration::from_secs(1));
		let n = count.load(Ordering::Relaxed);
		let secs = start.elapsed().as_secs_f64();
		eprint!(
			"\r  {:.0}s: {:>12} msgs  ({:>10.0} msg/s)",
			secs,
			n,
			n as f64 / secs
		);
	}
	stop.store(true, Ordering::Relaxed);
	for h in handles {
		let _ = h.join();
	}

	let total = count.load(Ordering::Relaxed);
	let secs = start.elapsed().as_secs_f64();
	let rate = total as f64 / secs;
	let mib = rate * payload_bytes as f64 / (1024.0 * 1024.0);
	eprintln!();
	println!("\nTOTAL {total} msgs in {secs:.1}s = {rate:.0} msg/s (~{mib:.1} MiB/s payload) at QoS {qos}");
}
