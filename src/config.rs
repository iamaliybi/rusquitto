//! Command-line interface and configuration management.
//!
//! The broker takes exactly one argument — the path to a TOML config file, given
//! positionally (`rusquitto <CONFIG>`) — parsed with `clap`'s derive API
//! ([`Cli`]). The file is decoded with `serde` + `toml` into the strongly-typed
//! [`Config`] tree, then validated.
//!
//! Every section and field has a sensible default (see the `Default` impls and
//! `rusquitto.toml`), so a minimal config — or even an empty file — is valid.
//! `deny_unknown_fields` is enabled throughout to catch typos in production.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};

use clap::Parser;
use mqttbytes::QoS;
use serde::Deserialize;

/// rusquitto command-line interface.
#[derive(Debug, Parser)]
#[command(
	name = "rusquitto",
	version,
	about = "A thread-per-core MQTT 5 broker built on glommio"
)]
pub struct Cli {
	/// Path to the TOML configuration file.
	#[arg(value_name = "CONFIG")]
	pub config: PathBuf,
}

impl Cli {
	/// Parses the process arguments (handles `--help` / `--version` and exits on
	/// malformed input, courtesy of clap).
	pub fn parse_args() -> Self {
		Self::parse()
	}
}

// ===========================================================================
// Configuration tree
// ===========================================================================

/// Top-level broker configuration. Maps 1:1 to the sections of `rusquitto.toml`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
	pub server: ServerConfig,
	pub runtime: RuntimeConfig,
	pub logging: LoggingConfig,
	pub limits: LimitsConfig,
	pub auth: AuthConfig,
}

/// `[server]` — network ingress.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
	/// Address to bind every shard's listener to (all shards share it via
	/// `SO_REUSEPORT`). IPv4 or IPv6.
	pub bind: IpAddr,
	/// TCP port to listen on.
	pub port: u16,
	/// `listen(2)` backlog passed to each shard's socket.
	pub listen_backlog: i32,
}

/// `[runtime]` — thread-per-core / glommio settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
	/// Explicit number of shards (executors) to spawn. When omitted, it is
	/// derived from the online core count and [`cpu_fraction`](Self::cpu_fraction).
	pub shards: Option<usize>,
	/// Fraction of online cores to use when `shards` is not set (e.g. `0.75`).
	pub cpu_fraction: f64,
	/// CPU placement / affinity strategy for the executor pool.
	pub placement: Placement,
	/// Per-link capacity of the inter-shard channel mesh.
	pub mesh_capacity: usize,
}

/// CPU placement strategy, mapped onto glommio's `PoolPlacement`.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Placement {
	/// Spread shards across physical cores / NUMA nodes (best latency).
	MaxSpread,
	/// Pack shards onto as few NUMA nodes as possible (best locality).
	MaxPack,
	/// No pinning — let the OS schedule the executor threads.
	Unbound,
}

/// `[logging]` — integrates with the non-blocking `tracing` setup in `logger.rs`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
	/// Filter directive (`EnvFilter` syntax), e.g. `"info,rusquitto=debug"`.
	/// `RUST_LOG` overrides this at startup.
	pub level: String,
	/// Directory for the rotating log files (created if missing).
	pub dir: PathBuf,
	/// File name for the main daily-rotating log.
	pub file: String,
	/// File name for the dedicated daily-rotating ERROR log.
	pub error_file: String,
	/// Whether to attach a terminal (stdout) layer. Off by default so the broker
	/// is silent in the terminal for maximum performance; file logging is always
	/// active. Enable for live monitoring.
	pub enable_terminal: bool,
	/// Format for the stdout layer (only used when `enable_terminal` is true).
	pub format: LogFormat,
}

/// Stdout log format.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
	/// Human-readable, colourised — for local development.
	Pretty,
	/// One JSON object per line — for production telemetry.
	Json,
}

/// `[limits]` — broker / MQTT resource limits. All fields are plain scalars so
/// this struct is `Copy` and can be handed to each connection by value.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
	/// Maximum concurrent connections accepted on a single shard.
	pub max_connections_per_shard: usize,
	/// Maximum accepted MQTT packet size, in bytes.
	pub max_payload_size: usize,
	/// Initial per-connection assembly buffer capacity, in bytes.
	pub initial_read_buffer: usize,
	/// Outbound QoS 1/2 in-flight window per connection (informational ceiling).
	pub max_inflight: u16,
	/// Maximum QoS the broker grants to subscribers (0, 1, or 2).
	pub max_qos: u8,
	/// Server keep-alive, in seconds, advertised to clients in CONNACK
	/// (`0` disables the override).
	pub keep_alive: u16,
	/// Whether retained messages are accepted/served.
	pub retain_available: bool,
}

/// `[auth]` — connection authentication. When `allow_anonymous` is true and no
/// users are defined (the default), authentication is a no-op and any client may
/// connect.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
	/// Whether clients may connect without supplying a username.
	pub allow_anonymous: bool,
	/// Known users. A client that presents a username must match one of these.
	pub users: Vec<UserConfig>,
}

/// A single `[[auth.users]]` entry.
///
/// Passwords are stored in plaintext in the config file, so protect it with file
/// permissions and treat it as a secret. (Hashed passwords are a planned
/// enhancement.)
///
/// `publish` / `subscribe` are optional topic-filter allow-lists (each may use
/// `+` / `#` wildcards). When a list is omitted the user is unrestricted for
/// that operation; when present, only matching topics are permitted.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserConfig {
	pub username: String,
	pub password: String,
	#[serde(default)]
	pub publish: Option<Vec<String>>,
	#[serde(default)]
	pub subscribe: Option<Vec<String>>,
}

impl LimitsConfig {
	/// The `max_qos` ceiling as a typed [`QoS`].
	pub fn max_qos(&self) -> QoS {
		match self.max_qos {
			0 => QoS::AtMostOnce,
			1 => QoS::AtLeastOnce,
			_ => QoS::ExactlyOnce,
		}
	}
}

// ===========================================================================
// Defaults
// ===========================================================================

impl Default for Config {
	fn default() -> Self {
		Self {
			server: ServerConfig::default(),
			runtime: RuntimeConfig::default(),
			logging: LoggingConfig::default(),
			limits: LimitsConfig::default(),
			auth: AuthConfig::default(),
		}
	}
}

impl Default for AuthConfig {
	fn default() -> Self {
		Self {
			allow_anonymous: true,
			users: Vec::new(),
		}
	}
}

impl Default for ServerConfig {
	fn default() -> Self {
		Self {
			bind: IpAddr::V4(Ipv4Addr::LOCALHOST),
			port: 1883,
			listen_backlog: 1024,
		}
	}
}

impl Default for RuntimeConfig {
	fn default() -> Self {
		Self {
			shards: None,
			cpu_fraction: 0.75,
			placement: Placement::MaxSpread,
			mesh_capacity: 1024,
		}
	}
}

impl Default for LoggingConfig {
	fn default() -> Self {
		Self {
			level: "info,rusquitto=debug".to_string(),
			dir: PathBuf::from("logs"),
			file: "rusquitto.log".to_string(),
			error_file: "rusquitto.error.log".to_string(),
			enable_terminal: false,
			format: LogFormat::Pretty,
		}
	}
}

impl Default for LimitsConfig {
	fn default() -> Self {
		Self {
			max_connections_per_shard: 16_384,
			max_payload_size: 64 * 1024,
			initial_read_buffer: 4 * 1024,
			max_inflight: 128,
			max_qos: 2,
			keep_alive: 60,
			retain_available: true,
		}
	}
}

// ===========================================================================
// Loading, validation, and derived values
// ===========================================================================

impl Config {
	/// Reads, decodes, and validates the TOML config at `path`. The file must
	/// have a `.toml` extension.
	pub fn load(path: &Path) -> Result<Self, ConfigError> {
		match path.extension().and_then(|e| e.to_str()) {
			Some("toml") => {}
			_ => {
				return Err(ConfigError::Validation(format!(
					"configuration file must have a .toml extension: {}",
					path.display()
				)));
			}
		}

		let text = std::fs::read_to_string(path).map_err(ConfigError::Io)?;
		let config: Config = toml::from_str(&text).map_err(ConfigError::Parse)?;
		config.validate()?;
		Ok(config)
	}

	/// Enforces invariants the type system can't.
	fn validate(&self) -> Result<(), ConfigError> {
		let invalid = |msg: &str| Err(ConfigError::Validation(msg.to_string()));

		if self.server.port == 0 {
			return invalid("server.port must be non-zero");
		}
		if self.server.listen_backlog <= 0 {
			return invalid("server.listen_backlog must be positive");
		}
		if let Some(0) = self.runtime.shards {
			return invalid("runtime.shards must be at least 1 when set");
		}
		if !(self.runtime.cpu_fraction > 0.0 && self.runtime.cpu_fraction <= 1.0) {
			return invalid("runtime.cpu_fraction must be in the range (0.0, 1.0]");
		}
		if self.runtime.mesh_capacity == 0 {
			return invalid("runtime.mesh_capacity must be non-zero");
		}
		if self.limits.max_qos > 2 {
			return invalid("limits.max_qos must be 0, 1, or 2");
		}
		if self.limits.max_payload_size == 0 {
			return invalid("limits.max_payload_size must be non-zero");
		}
		if self.limits.initial_read_buffer == 0 {
			return invalid("limits.initial_read_buffer must be non-zero");
		}
		let mut seen = std::collections::HashSet::new();
		for user in &self.auth.users {
			if user.username.is_empty() {
				return invalid("auth.users entries must have a non-empty username");
			}
			if !seen.insert(user.username.as_str()) {
				return Err(ConfigError::Validation(format!(
					"duplicate auth user '{}'",
					user.username
				)));
			}
		}
		Ok(())
	}

	/// Number of shards to spawn given the count of online cores: the explicit
	/// `runtime.shards`, otherwise `online * cpu_fraction` (at least 1).
	pub fn resolved_shards(&self, online_cores: usize) -> usize {
		let n = match self.runtime.shards {
			Some(s) => s,
			None => ((online_cores as f64) * self.runtime.cpu_fraction).round() as usize,
		};
		n.max(1)
	}
}

/// Errors produced while loading a configuration file.
#[derive(Debug)]
pub enum ConfigError {
	/// The file could not be read.
	Io(std::io::Error),
	/// The TOML could not be decoded into [`Config`].
	Parse(toml::de::Error),
	/// The decoded config failed validation.
	Validation(String),
}

impl fmt::Display for ConfigError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			ConfigError::Io(e) => write!(f, "reading config file: {e}"),
			ConfigError::Parse(e) => write!(f, "parsing config file: {e}"),
			ConfigError::Validation(msg) => write!(f, "invalid configuration: {msg}"),
		}
	}
}

impl std::error::Error for ConfigError {}
