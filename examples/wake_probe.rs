//! Per-wake cost floor of the glommio runtime — a diagnostic echo server.
//!
//! Answers one question: of the broker's per-message CPU cost in a ping-pong
//! workload, how much is the *runtime* (io_uring park/unpark, task wake,
//! reactor bookkeeping) and how much is the broker's own event loop? This
//! serves the smallest possible glommio TCP loop — read, echo, repeat — so any
//! cost measured here is the floor no connection-engine change can go below.
//!
//! Run: `cargo run --release --example wake_probe` (listens on 127.0.0.1:1899),
//! then drive it with a 1-in-flight ping-pong client and compare wall/CPU per
//! message against the real broker under the same client.

// Diagnostic harness, not broker code: the crate-wide thread-per-core lints
// don't apply.
#![allow(clippy::disallowed_methods)]

use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpListener;
use glommio::{Latency, LocalExecutorBuilder, Placement, Shares};

fn main() {
	LocalExecutorBuilder::new(Placement::Unbound)
		.make()
		.expect("executor")
		.run(async {
			let listener = TcpListener::bind("127.0.0.1:1899").expect("bind");
			println!("echo on 127.0.0.1:1899");
			// Mirror the broker's queue setup so scheduling costs are comparable.
			let _tq = glommio::executor().create_task_queue(Shares::Static(200), Latency::NotImportant, "maintenance");
			loop {
				let mut stream = match listener.accept().await {
					Ok(s) => s,
					Err(_) => break,
				};
				let _ = stream.set_nodelay(true);
				glommio::spawn_local(async move {
					let mut buf = [0u8; 512];
					loop {
						match stream.read(&mut buf).await {
							Ok(0) | Err(_) => break,
							Ok(n) => {
								if stream.write_all(&buf[..n]).await.is_err() {
									break;
								}
							}
						}
					}
				})
				.detach();
			}
		});
}
