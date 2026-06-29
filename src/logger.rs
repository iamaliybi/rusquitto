//! Production logging for rusquitto.
//!
//! Built on the `tracing` ecosystem and tuned for the thread-per-core model:
//!
//! - **Non-blocking I/O.** Every sink is wrapped in [`tracing_appender::non_blocking`]
//!   in *lossy* mode. Worker executors only push formatted lines into an in-memory
//!   channel; a single dedicated background thread performs the actual disk writes.
//!   If that channel ever fills, lines are dropped rather than blocking a pinned
//!   core — a stalled logger must never stall the broker.
//! - **Layered output.** A human-friendly stdout layer for local development (or
//!   JSON stdout for containers), plus a daily-rotating JSON file for telemetry and
//!   a separate daily-rotating file that captures `ERROR` events only.
//! - **Dynamic filtering.** Verbosity is driven by an [`EnvFilter`] (so `RUST_LOG`
//!   directives give per-module control) and can be changed at runtime through the
//!   returned [`ReloadHandle`].
//! - **Redaction.** Sensitive packet data is never handed to the logger. See
//!   [`redact`] and the instrumentation example in `server::connection`.

use std::path::Path;

use tracing::level_filters::LevelFilter;
use tracing_appender::non_blocking::{NonBlocking, NonBlockingBuilder, WorkerGuard};
use tracing_appender::rolling;
use tracing_subscriber::{
	filter::EnvFilter, fmt, layer::SubscriberExt, reload, util::SubscriberInitExt, Layer, Registry,
};

/// Handle to the global verbosity filter. Call [`ReloadHandle::set`] to change log
/// levels at runtime without a restart.
pub type ReloadHandle = reload::Handle<EnvFilter, Registry>;

/// Output format for the stdout layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
	/// Human-readable, colourised — for local development.
	Pretty,
	/// One JSON object per line — for production / container stdout collection.
	Json,
}

/// Logging configuration.
pub struct Config<'a> {
	/// Directory for the rotating log files (created if missing).
	pub dir: &'a Path,
	/// Default filter directive, e.g. `"info,rusquitto=debug"`. Overridden by the
	/// `RUST_LOG` environment variable when present.
	pub default_filter: &'a str,
	/// Format for the stdout layer.
	pub stdout_format: Format,
}

impl Default for Config<'_> {
	fn default() -> Self {
		Self {
			dir: Path::new("logs"),
			default_filter: "info,rusquitto=debug",
			stdout_format: Format::Pretty,
		}
	}
}

/// Keeps the background writer threads alive. **Must** be held for the lifetime of
/// the program (typically a local in `main`) — dropping it flushes and stops the
/// non-blocking appenders, after which logs are lost.
pub struct Guards {
	_app: WorkerGuard,
	_err: WorkerGuard,
	/// Runtime verbosity control — operator-facing API (e.g. wire to a signal
	/// handler or admin endpoint), so not necessarily called from within the broker.
	#[allow(dead_code)]
	pub reload: ReloadHandle,
}

/// Initialises the global subscriber. Call once, early in `main`.
pub fn init(config: Config<'_>) -> std::io::Result<Guards> {
	std::fs::create_dir_all(config.dir)?;

	// Dynamic, per-module verbosity. RUST_LOG wins; otherwise the configured default.
	let env_filter = EnvFilter::try_from_default_env()
		.unwrap_or_else(|_| EnvFilter::new(config.default_filter));
	let (env_filter, reload) = reload::Layer::new(env_filter);

	// --- Main rotating JSON log (all levels permitted by the filter) ---
	let (app_writer, app_guard) = non_blocking(rolling::daily(config.dir, "rusquitto.log"));
	let file_layer = fmt::layer()
		.json()
		.with_ansi(false)
		.with_current_span(true)
		.with_span_list(true)
		.with_writer(app_writer);

	// --- Dedicated ERROR-only rotating JSON log ---
	let (err_writer, err_guard) = non_blocking(rolling::daily(config.dir, "rusquitto.error.log"));
	let error_layer = fmt::layer()
		.json()
		.with_ansi(false)
		.with_current_span(true)
		.with_span_list(true)
		.with_writer(err_writer)
		.with_filter(LevelFilter::ERROR);

	// --- Stdout layer: pretty for dev, JSON for prod ---
	let stdout_layer = match config.stdout_format {
		Format::Pretty => fmt::layer().pretty().with_writer(std::io::stdout).boxed(),
		Format::Json => fmt::layer()
			.json()
			.with_current_span(true)
			.with_span_list(true)
			.with_writer(std::io::stdout)
			.boxed(),
	};

	Registry::default()
		.with(env_filter) // global, runtime-reloadable verbosity
		.with(stdout_layer)
		.with(file_layer)
		.with(error_layer)
		.init();

	Ok(Guards {
		_app: app_guard,
		_err: err_guard,
		reload,
	})
}

/// Wraps a rolling appender in a lossy, non-blocking writer.
///
/// Lossy mode is deliberate: a pinned worker core must never park waiting on disk.
fn non_blocking<W>(appender: W) -> (NonBlocking, WorkerGuard)
where
	W: std::io::Write + Send + 'static,
{
	NonBlockingBuilder::default()
		.lossy(true)
		.buffered_lines_limit(16_384)
		.finish(appender)
}

impl Guards {
	/// Changes the global filter at runtime, e.g. `guards.set_filter("debug")` or
	/// `guards.set_filter("rusquitto::server=trace,info")`.
	#[allow(dead_code)]
	pub fn set_filter(&self, directive: &str) -> Result<(), String> {
		let filter = EnvFilter::try_new(directive).map_err(|e| e.to_string())?;
		self.reload.reload(filter).map_err(|e| e.to_string())
	}
}

/// Helpers for keeping sensitive MQTT data out of the logs.
///
/// The cardinal rule is to never pass secrets to a logging macro in the first
/// place. These helpers make the safe representation explicit at the call site.
pub mod redact {
	/// Placeholder for any value that must never be written to logs.
	pub const REDACTED: &str = "[REDACTED]";

	/// Summarises a payload as its byte length instead of its (possibly sensitive)
	/// contents. Use as `payload = %redact::payload(&publish.payload)`.
	pub fn payload(bytes: &[u8]) -> String {
		format!("<{} bytes>", bytes.len())
	}

	/// Renders an optional username for logging, masking only the presence of a
	/// password — the password itself is never accepted by this function.
	pub fn credentials(username: Option<&str>, has_password: bool) -> String {
		match (username, has_password) {
			(Some(u), true) => format!("{u} (+password {REDACTED})"),
			(Some(u), false) => u.to_string(),
			(None, _) => "<anonymous>".to_string(),
		}
	}
}
