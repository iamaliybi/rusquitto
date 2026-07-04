//! Command-line interface and configuration management.
//!
//! The broker takes exactly one argument — the path to a TOML config file, given
//! positionally (`rusquitto <CONFIG>`) — parsed with `clap`'s derive API
//! ([`Cli`]). The file is decoded with `serde` + `toml` into the strongly-typed
//! [`Config`] tree, then validated.
//!
//! Every section and field has a sensible default (see the `Default` impls and
//! `rusquitto.config.toml`), so a minimal config — or even an empty file — is
//! valid. `deny_unknown_fields` is enabled throughout to catch typos in production.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};

use clap::Parser;
use mqttbytes::QoS;
use serde::Deserialize;

/// rusquitto command-line interface.
#[derive(Debug, Parser)]
#[command(name = "rusquitto", version, about = "A thread-per-core MQTT 5 broker built on glommio")]
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

/// Top-level broker configuration. Maps 1:1 to the sections of `rusquitto.config.toml`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
	pub server: ServerConfig,
	pub tls: TlsConfig,
	pub runtime: RuntimeConfig,
	pub logging: LoggingConfig,
	pub limits: LimitsConfig,
	pub auth: AuthConfig,
	pub sys: SysConfig,
}

/// `[server]` — network ingress.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
	/// Address to bind every shard's listener to (all shards share it via
	/// `SO_REUSEPORT`). IPv4 or IPv6.
	pub bind: IpAddr,
	/// TCP port for native MQTT.
	pub port: u16,
	/// Whether to also accept MQTT-over-WebSocket connections (for browser clients).
	pub websocket: bool,
	/// Port for the WebSocket listener (used only when `websocket` is true).
	pub websocket_port: u16,
	/// `listen(2)` backlog passed to each shard's socket.
	pub listen_backlog: i32,
}

impl ServerConfig {
	/// The WebSocket port when the listener is enabled, else `None`.
	pub fn websocket_port(&self) -> Option<u16> {
		self.websocket.then_some(self.websocket_port)
	}
}

/// `[tls]` — TLS termination via rustls. Disabled by default. When enabled, a
/// native MQTT-over-TLS (`mqtts`) listener runs on `port`; when `websocket` is
/// also set, a WebSocket-over-TLS (`wss`) listener runs on `websocket_port`. Both
/// present the same certificate. Only TLS 1.3 and 1.2 with strong AEAD cipher
/// suites are offered (see [`transport::tls`](crate::transport::tls)).
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsConfig {
	/// Master switch for all TLS listeners.
	pub enabled: bool,
	/// Port for native MQTT over TLS (`mqtts`). IANA-registered default is 8883.
	pub port: u16,
	/// Whether to also accept MQTT-over-WebSocket over TLS (`wss`) for browsers.
	pub websocket: bool,
	/// Port for the `wss` listener (used only when `enabled` and `websocket`).
	pub websocket_port: u16,
	/// PEM certificate chain, leaf certificate first. Required when `enabled`.
	pub cert_file: Option<PathBuf>,
	/// PEM private key (PKCS#8, PKCS#1, or SEC1). Required when `enabled`.
	pub key_file: Option<PathBuf>,
}

impl TlsConfig {
	/// The `mqtts` port when TLS is enabled, else `None`.
	pub fn mqtts_port(&self) -> Option<u16> {
		self.enabled.then_some(self.port)
	}

	/// The `wss` port when TLS and its WebSocket listener are both enabled, else `None`.
	pub fn wss_port(&self) -> Option<u16> {
		(self.enabled && self.websocket).then_some(self.websocket_port)
	}
}

/// `[runtime]` — thread-per-core / glommio settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
	/// Number of CPU cores to run on — the broker pins one shard (executor) per
	/// core, so this is also the shard count. When omitted, every online core is
	/// used; a value larger than the online core count is clamped down to it.
	pub cores: Option<usize>,
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

/// `[logging]` — integrates with the non-blocking `tracing` setup in
/// `telemetry/logging.rs`.
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
	/// Maximum concurrent connections one client IP may hold on a single shard
	/// (`0` = unlimited). Bounds single-source connection floods; per-shard, and
	/// only meaningful when clients connect directly (behind a proxy all
	/// connections share the proxy IP).
	pub max_connections_per_ip: usize,
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
	/// Seconds a new socket has to send a valid CONNECT before it is dropped, so a
	/// connection that opens but never authenticates can't tie up a slot.
	pub connect_timeout: u16,
	/// Upper bound on a client's negotiated Session Expiry Interval, in seconds
	/// (`0` = no cap). Stops a client from pinning a session forever.
	pub max_session_expiry: u32,
	/// Maximum active subscriptions a single client may hold (`0` = unlimited).
	pub max_subscriptions_per_client: usize,
	/// Maximum distinct retained messages stored per shard (`0` = unlimited).
	pub max_retained_messages: usize,
	/// Maximum inbound PUBLISH messages per second, per connection (`0` = unlimited).
	/// A client exceeding it is throttled (paced to the rate), not disconnected.
	/// Bounds how much CPU one noisy publisher can draw on its pinned core.
	pub max_message_rate: u32,
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

/// `[sys]` — `$SYS/broker/...` metrics topics. One shard periodically publishes
/// broker counters (uptime, client counts, message/byte throughput) as retained
/// messages that any `$SYS/#` subscriber can read.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SysConfig {
	/// Whether to publish `$SYS` metrics topics.
	pub enabled: bool,
	/// Interval between `$SYS` updates, in seconds.
	pub interval: u64,
}

/// A single `[[auth.users]]` entry.
///
/// A user carries exactly one credential: either a plaintext `password` or a
/// `password_hash` (a lowercase-hex SHA-256 of the password). Prefer the hash so
/// the config file doesn't hold the secret in the clear; either way protect the
/// file with restrictive permissions.
///
/// `publish` / `subscribe` are optional topic-filter allow-lists (each may use
/// `+` / `#` wildcards). When a list is omitted the user is unrestricted for
/// that operation; when present, only matching topics are permitted.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserConfig {
	pub username: String,
	/// Plaintext password. Mutually exclusive with `password_hash`.
	#[serde(default)]
	pub password: Option<String>,
	/// Lowercase-hex SHA-256 of the password. Mutually exclusive with `password`.
	#[serde(default)]
	pub password_hash: Option<String>,
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

impl Default for SysConfig {
	fn default() -> Self {
		Self { enabled: true, interval: 10 }
	}
}

impl Default for AuthConfig {
	fn default() -> Self {
		Self { allow_anonymous: true, users: Vec::new() }
	}
}

impl Default for ServerConfig {
	fn default() -> Self {
		Self {
			bind: IpAddr::V4(Ipv4Addr::LOCALHOST),
			port: 1883,
			websocket: true,
			websocket_port: 1884,
			listen_backlog: 1024,
		}
	}
}

impl Default for TlsConfig {
	fn default() -> Self {
		Self {
			enabled: false,
			port: 8883,
			websocket: true,
			websocket_port: 8884,
			cert_file: None,
			key_file: None,
		}
	}
}

impl Default for RuntimeConfig {
	fn default() -> Self {
		Self {
			cores: None,
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
			max_connections_per_ip: 0,
			max_payload_size: 64 * 1024,
			initial_read_buffer: 4 * 1024,
			max_inflight: 128,
			max_qos: 2,
			keep_alive: 60,
			retain_available: true,
			connect_timeout: 10,
			max_session_expiry: 86_400,
			max_subscriptions_per_client: 1024,
			max_retained_messages: 100_000,
			max_message_rate: 0,
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
		if self.server.websocket && self.server.websocket_port == 0 {
			return invalid("server.websocket_port must be non-zero when websocket is enabled");
		}
		if self.server.listen_backlog <= 0 {
			return invalid("server.listen_backlog must be positive");
		}
		if self.tls.enabled {
			if self.tls.cert_file.is_none() || self.tls.key_file.is_none() {
				return invalid("tls.cert_file and tls.key_file are required when tls.enabled is true");
			}
			if self.tls.port == 0 {
				return invalid("tls.port must be non-zero when tls.enabled is true");
			}
			if self.tls.websocket && self.tls.websocket_port == 0 {
				return invalid("tls.websocket_port must be non-zero when tls.websocket is enabled");
			}
		}
		// Every active listener binds the same address via SO_REUSEPORT, so their
		// ports must all differ. Collect the enabled ones and reject any collision.
		let active_ports = [
			Some(("server.port", self.server.port)),
			self.server
				.websocket_port()
				.map(|p| ("server.websocket_port", p)),
			self.tls.mqtts_port().map(|p| ("tls.port", p)),
			self.tls.wss_port().map(|p| ("tls.websocket_port", p)),
		];
		let mut seen = std::collections::HashMap::new();
		for (name, port) in active_ports.into_iter().flatten() {
			if let Some(other) = seen.insert(port, name) {
				return Err(ConfigError::Validation(format!(
					"listener port {port} is used by both {other} and {name}"
				)));
			}
		}
		if let Some(0) = self.runtime.cores {
			return invalid("runtime.cores must be at least 1 when set");
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
		if self.sys.enabled && self.sys.interval == 0 {
			return invalid("sys.interval must be non-zero when sys.enabled is true");
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
			// Each user carries exactly one credential.
			if user.password.is_some() == user.password_hash.is_some() {
				return Err(ConfigError::Validation(format!(
					"auth user '{}' must set exactly one of `password` or `password_hash`",
					user.username
				)));
			}
			if let Some(hash) = &user.password_hash
				&& (hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()))
			{
				return Err(ConfigError::Validation(format!(
					"auth user '{}' password_hash must be a 64-char hex SHA-256",
					user.username
				)));
			}
		}
		Ok(())
	}

	/// Number of shards to spawn (one per core) given the count of online cores:
	/// `runtime.cores` when set, otherwise every online core. A requested count
	/// above the online cores is clamped down to them, and the result is at least 1.
	pub fn resolved_shards(&self, online_cores: usize) -> usize {
		let online = online_cores.max(1);
		self.runtime.cores.unwrap_or(online).clamp(1, online)
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

#[cfg(test)]
mod tests {
	use super::*;

	/// A user with a single plaintext credential (the common valid case).
	fn user(name: &str) -> UserConfig {
		UserConfig {
			username: name.to_string(),
			password: Some("pw".to_string()),
			password_hash: None,
			publish: None,
			subscribe: None,
		}
	}

	#[test]
	fn default_config_passes_validation() {
		assert!(Config::default().validate().is_ok());
	}

	#[test]
	fn websocket_port_must_differ_from_tcp_port() {
		let mut c = Config::default();
		c.server.websocket = true;
		c.server.websocket_port = c.server.port;
		assert!(c.validate().is_err());
	}

	#[test]
	fn websocket_port_clash_ignored_when_websocket_disabled() {
		let mut c = Config::default();
		c.server.websocket = false;
		c.server.websocket_port = c.server.port;
		assert!(c.validate().is_ok());
	}

	#[test]
	fn max_qos_above_two_is_rejected() {
		let mut c = Config::default();
		c.limits.max_qos = 3;
		assert!(c.validate().is_err());
	}

	#[test]
	fn duplicate_usernames_are_rejected() {
		let mut c = Config::default();
		c.auth.users = vec![user("alice"), user("alice")];
		assert!(c.validate().is_err());
	}

	#[test]
	fn user_must_set_exactly_one_credential() {
		let mut c = Config::default();
		let mut both = user("bob");
		both.password_hash = Some("a".repeat(64));
		c.auth.users = vec![both];
		assert!(c.validate().is_err(), "both credentials set");

		let mut neither = user("bob");
		neither.password = None;
		c.auth.users = vec![neither];
		assert!(c.validate().is_err(), "no credential set");
	}

	#[test]
	fn password_hash_must_be_64_hex_chars() {
		let mut c = Config::default();
		let mut u = user("carol");
		u.password = None;
		u.password_hash = Some("not-hex".to_string());
		c.auth.users = vec![u];
		assert!(c.validate().is_err());
	}

	#[test]
	fn tls_enabled_requires_cert_and_key() {
		let mut c = Config::default();
		c.tls.enabled = true;
		assert!(c.validate().is_err(), "no cert/key set");

		c.tls.cert_file = Some(PathBuf::from("cert.pem"));
		c.tls.key_file = Some(PathBuf::from("key.pem"));
		assert!(c.validate().is_ok(), "cert and key set");
	}

	#[test]
	fn tls_disabled_ignores_its_ports_and_cert() {
		let mut c = Config::default();
		c.tls.enabled = false;
		c.tls.port = c.server.port; // would collide if TLS were active
		assert!(c.validate().is_ok());
	}

	#[test]
	fn listener_ports_must_be_unique() {
		let mut c = Config::default();
		c.tls.enabled = true;
		c.tls.cert_file = Some(PathBuf::from("cert.pem"));
		c.tls.key_file = Some(PathBuf::from("key.pem"));

		c.tls.port = c.server.port; // mqtts clashes with plain MQTT
		assert!(c.validate().is_err());

		c.tls.port = 8883;
		c.tls.websocket_port = c.server.websocket_port; // wss clashes with plain ws
		assert!(c.validate().is_err());

		c.tls.websocket_port = 8884;
		assert!(c.validate().is_ok(), "all four ports distinct");
	}

	#[test]
	fn tls_port_helpers_track_enablement() {
		let mut c = Config::default();
		assert_eq!(c.tls.mqtts_port(), None);
		assert_eq!(c.tls.wss_port(), None);

		c.tls.enabled = true;
		assert_eq!(c.tls.mqtts_port(), Some(8883));
		assert_eq!(c.tls.wss_port(), Some(8884));

		c.tls.websocket = false;
		assert_eq!(c.tls.wss_port(), None, "wss requires tls.websocket");
	}

	#[test]
	fn resolved_shards_clamps_to_online_cores() {
		let mut c = Config::default();
		c.runtime.cores = Some(8);
		assert_eq!(c.resolved_shards(4), 4, "requested above online is clamped");
		c.runtime.cores = Some(2);
		assert_eq!(
			c.resolved_shards(4),
			2,
			"requested below online is honoured"
		);
		c.runtime.cores = None;
		assert_eq!(c.resolved_shards(4), 4, "unset uses all online cores");
	}
}
