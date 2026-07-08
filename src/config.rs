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
	pub overload: OverloadConfig,
	pub parking: ParkingConfig,
	pub persistence: PersistenceConfig,
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
	/// `SO_RCVBUF` cap for client sockets, in bytes (`0` = kernel default).
	/// Set on the listeners, so accepted sockets inherit it. On memory-tight
	/// hosts this bounds *kernel-side* socket memory — which lives outside the
	/// broker's RSS — at high connection counts, and also caps the advertised
	/// TCP receive window. The kernel doubles the value it is given and
	/// enforces its own minimum (see `socket(7)`).
	pub socket_recv_buffer: usize,
	/// `SO_SNDBUF` cap for client sockets, in bytes (`0` = kernel default).
	/// Same inheritance and doubling semantics as `socket_recv_buffer`.
	pub socket_send_buffer: usize,
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
	/// PEM bundle of trusted client-CA certificates. When set, mutual TLS is on:
	/// a client presenting a certificate has it verified against this CA. Absent
	/// (the default) disables client-certificate authentication entirely.
	pub client_ca_file: Option<PathBuf>,
	/// Require every TLS client to present a certificate valid under
	/// `client_ca_file`. When true, a client without a trusted certificate fails
	/// the TLS handshake. When false, client certificates are verified if offered
	/// but not demanded. Ignored unless `client_ca_file` is set.
	pub require_client_cert: bool,
	/// Seconds between checking the certificate/key/CA files for changes and
	/// hot-reloading them into new connections (`0` = disabled). Existing
	/// connections keep the certificate they handshook with; only new handshakes
	/// pick up a rotated certificate. Each shard reloads its own acceptor, so the
	/// swap needs no cross-core coordination.
	pub reload_interval: u64,
	/// Use a verified client certificate's subject Common Name as the MQTT
	/// username, so `[[auth.users]]` ACLs apply per device. A client that also
	/// sends an explicit MQTT username is checked the usual way instead (the CN
	/// only stands in when no username is supplied). Ignored unless
	/// `client_ca_file` is set. Default `false` — a cert-verified client with no
	/// username is then treated as anonymous for ACLs.
	pub cert_cn_as_username: bool,
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
	/// io_uring registered-buffer pool per shard, in KiB. glommio pre-allocates
	/// this and **pins it** (`IORING_REGISTER_BUFFERS`) at startup, so it lands in
	/// resident memory for the shard's whole life — the dominant term in the
	/// empty-broker footprint (glommio's own default is 10 MiB *per core*). The
	/// network fast path (`recv`/`send`) does not draw from this pool; only DMA
	/// file I/O — i.e. persistence snapshots — does, and it falls back to the
	/// heap when the pool is exhausted, so a small pool costs at most slightly
	/// slower snapshots. `512` (the default) keeps a shard's baseline near a
	/// megabyte; raise it only for persistence-heavy deployments doing large,
	/// frequent snapshots. Clamped to glommio's 64 KiB floor.
	pub io_memory_kib: usize,
	/// Microseconds to busy-spin polling io_uring completions before parking the
	/// reactor. `0` (the default) parks immediately — the right trade for most
	/// deployments. A small non-zero value (e.g. `50`) removes the io_uring
	/// park/unpark round-trip from the single-message latency path, trading idle
	/// CPU for lower request/response latency; worthwhile only for latency-
	/// critical, steadily-busy shards. Has effect only under a CPU-pinning
	/// `placement` (`max-spread` / `max-pack`); glommio disables spinning under
	/// `unbound`.
	pub spin_before_park_us: u64,
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
	/// Initial per-connection assembly buffer capacity, in bytes. `0` (the
	/// default, recommended) allocates nothing up front: the buffer grows on
	/// demand from the first read and is trimmed when idle, so idle connections
	/// hold no read-buffer memory.
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

/// `[overload]` — per-shard overload handling. A lightweight probe tracks each
/// shard's *scheduling delay* (how far behind its reactor is on normal-priority
/// work, i.e. reactor saturation). Detection is always on and cheap; the
/// mitigations below act on that signal and are opt-in.
///
/// The thread-per-core model pins a connection to the shard that accepted it and
/// has no work-stealing, so these levers *prevent* and *rebalance* rather than
/// migrate compute: reject at the door, or shed so the client rehashes elsewhere.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OverloadConfig {
	/// Log a WARN while a shard's scheduling delay stays above this many
	/// milliseconds (`0` = never). A cheap stall detector.
	pub stall_warn_ms: u32,
	/// Reject new connections on a shard whose scheduling delay exceeds this many
	/// milliseconds (`0` = disabled). The rejected client's retry may hash onto a
	/// cooler core.
	pub admission_delay_ms: u32,
	/// Shed (close) existing connections on a shard whose scheduling delay exceeds
	/// this many milliseconds (`0` = disabled), up to `shed_batch` per second. A
	/// shed client reconnects from a new source port, which `SO_REUSEPORT` rehashes
	/// — usually onto a cooler core. Disruptive; off by default.
	pub shed_delay_ms: u32,
	/// Maximum connections shed per second per shard while shedding is active.
	pub shed_batch: usize,
}

/// `[parking]` — the parked-connection idle path. An idle plain-TCP connection
/// normally holds a glommio task and an io_uring read `Source` (~a few KiB) just
/// to wait for its next byte. When parking is enabled, a connection that has been
/// fully idle (no buffered bytes, no partial frame, no in-flight QoS state, no
/// queued deliveries) for `idle_grace_secs` is *parked*: its task is torn down and
/// only its fd — armed on a small per-shard io_uring readiness ring — plus a
/// minimal resume record remain (~0.1–0.5 KiB). Any inbound byte, or a routed
/// delivery targeting the parked client, resurrects it transparently; the client
/// never observes the difference. Plain TCP only: TLS and WebSocket streams carry
/// mid-stream codec state and are never parked.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ParkingConfig {
	/// Master switch for the parked-connection idle path.
	pub enabled: bool,
	/// Seconds a connection must stay fully idle before it is parked. A client
	/// pinging every `K` seconds is parked roughly `(K − grace)/K` of the time,
	/// so keep this well below the fleet's typical keep-alive interval.
	pub idle_grace_secs: u64,
}

/// `[persistence]` — disk-backed durability. Disabled by default; the broker is
/// otherwise entirely in-memory. When enabled, both the **retained-message** set
/// and **suspended sessions** (their subscriptions, in-flight QoS 1/2 state, and
/// offline queue) are snapshotted under `dir` periodically and on graceful
/// shutdown, and restored on startup — so retained "last known value" topics and
/// offline sessions survive a restart. Retained is replicated identically on every
/// shard (one snapshot file); sessions are shard-local (one file per shard).
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PersistenceConfig {
	/// Master switch for on-disk persistence.
	pub enabled: bool,
	/// Directory holding the snapshot files (created if missing).
	pub dir: PathBuf,
	/// File name for the retained-message snapshot.
	pub retained_file: String,
	/// Seconds between snapshots (`0` = snapshot only on graceful shutdown). A crash
	/// may lose retained updates made since the last snapshot.
	pub snapshot_interval: u64,
	/// Milliseconds between session write-ahead-log flushes (`0` = WAL disabled,
	/// snapshot-only). When set, a per-shard append-only log records session
	/// suspensions and offline-queue growth between snapshots, group-committed
	/// (`fdatasync`'d) at this cadence and replayed over the snapshot on startup —
	/// so a crash loses at most this window of durable *session* state instead of a
	/// whole `snapshot_interval`. Retained messages are snapshot-only (not WAL'd).
	pub wal_flush_ms: u64,
}

impl PersistenceConfig {
	/// Full path to the retained-message snapshot file.
	pub fn retained_path(&self) -> PathBuf {
		self.dir.join(&self.retained_file)
	}

	/// Full path to a shard's session snapshot file. Sessions are shard-local, so
	/// each shard (mesh peer) persists its own file.
	pub fn session_path(&self, peer_id: usize) -> PathBuf {
		self.dir.join(format!("sessions-{peer_id}.mqtt"))
	}

	/// Full path to a shard's session write-ahead log. Like the session snapshot,
	/// it is shard-local (one file per mesh peer).
	pub fn wal_path(&self, peer_id: usize) -> PathBuf {
		self.dir.join(format!("sessions-{peer_id}.wal"))
	}

	/// Whether the session WAL is enabled (persistence on and a non-zero flush).
	pub fn wal_enabled(&self) -> bool {
		self.enabled && self.wal_flush_ms > 0
	}
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
	/// Topic-filter allow-list for *anonymous* publishes. Omitted (the default)
	/// leaves anonymous clients unrestricted; an empty list denies all publishes.
	pub anonymous_publish: Option<Vec<String>>,
	/// Topic-filter allow-list for *anonymous* subscriptions. Omitted (the
	/// default) leaves anonymous clients unrestricted; an empty list denies all.
	pub anonymous_subscribe: Option<Vec<String>>,
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
		Self {
			allow_anonymous: true,
			users: Vec::new(),
			anonymous_publish: None,
			anonymous_subscribe: None,
		}
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
			socket_recv_buffer: 0,
			socket_send_buffer: 0,
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
			client_ca_file: None,
			require_client_cert: false,
			reload_interval: 0,
			cert_cn_as_username: false,
		}
	}
}

impl Default for RuntimeConfig {
	fn default() -> Self {
		Self {
			cores: None,
			placement: Placement::MaxSpread,
			mesh_capacity: 1024,
			io_memory_kib: 512,
			spin_before_park_us: 0,
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
			initial_read_buffer: 0,
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

impl Default for OverloadConfig {
	fn default() -> Self {
		Self {
			stall_warn_ms: 100,
			admission_delay_ms: 0,
			shed_delay_ms: 0,
			shed_batch: 8,
		}
	}
}

impl Default for ParkingConfig {
	fn default() -> Self {
		Self { enabled: true, idle_grace_secs: 30 }
	}
}

impl Default for PersistenceConfig {
	fn default() -> Self {
		Self {
			enabled: false,
			dir: PathBuf::from("data"),
			retained_file: "retained.mqtt".to_string(),
			snapshot_interval: 300,
			wal_flush_ms: 200,
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
			if self.tls.require_client_cert && self.tls.client_ca_file.is_none() {
				return invalid("tls.client_ca_file is required when tls.require_client_cert is true");
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
		if self.runtime.io_memory_kib < 64 {
			return invalid("runtime.io_memory_kib must be at least 64 (glommio's io_uring buffer floor)");
		}
		if self.limits.max_qos > 2 {
			return invalid("limits.max_qos must be 0, 1, or 2");
		}
		if self.limits.max_payload_size == 0 {
			return invalid("limits.max_payload_size must be non-zero");
		}
		if self.parking.enabled && self.parking.idle_grace_secs == 0 {
			return invalid("parking.idle_grace_secs must be non-zero when parking is enabled");
		}
		if self.persistence.enabled && self.persistence.retained_file.is_empty() {
			return invalid("persistence.retained_file must be set when persistence is enabled");
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
			// A `password_hash` is either an Argon2 PHC string (recommended: salted,
			// memory-hard) or a legacy 64-char hex SHA-256.
			if let Some(hash) = &user.password_hash {
				if hash.starts_with("$argon2") {
					// PHC parsing alone is lax (a bare salt parses); a usable
					// credential needs both its salt and its hash output present.
					let usable = argon2::password_hash::PasswordHash::new(hash)
						.map(|h| h.salt.is_some() && h.hash.is_some())
						.unwrap_or(false);
					if !usable {
						return Err(ConfigError::Validation(format!(
							"auth user '{}' password_hash is not a valid Argon2 PHC string",
							user.username
						)));
					}
				} else if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
					return Err(ConfigError::Validation(format!(
						"auth user '{}' password_hash must be a 64-char hex SHA-256 or an \
						 Argon2 PHC string (`$argon2id$...`)",
						user.username
					)));
				}
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
	fn password_hash_accepts_argon2_phc_and_rejects_malformed() {
		let mut c = Config::default();
		let mut u = user("dave");
		u.password = None;
		// A structurally valid Argon2id PHC string (salt/hash are base64).
		u.password_hash =
			Some("$argon2id$v=19$m=1024,t=1,p=1$dGVzdHNhbHQwMDE$3XOJivDKrqO2ryjLZ7RTLcLcvfKUmZlCzS36XX2ysVE".into());
		c.auth.users = vec![u.clone()];
		assert!(c.validate().is_ok(), "valid PHC accepted");

		u.password_hash = Some("$argon2id$garbage".into());
		c.auth.users = vec![u];
		assert!(c.validate().is_err(), "malformed PHC rejected");
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
	fn io_memory_below_floor_is_rejected() {
		let mut c = Config::default();
		assert_eq!(
			c.runtime.io_memory_kib, 512,
			"small io_memory default keeps the baseline lean"
		);
		c.runtime.io_memory_kib = 32;
		assert!(c.validate().is_err(), "below glommio's 64 KiB floor");
		c.runtime.io_memory_kib = 64;
		assert!(c.validate().is_ok(), "at the floor is fine");
	}

	#[test]
	fn parking_enabled_requires_nonzero_grace() {
		let mut c = Config::default();
		assert!(c.parking.enabled, "parking is on by default");
		c.parking.idle_grace_secs = 0;
		assert!(c.validate().is_err(), "zero grace rejected while enabled");
		c.parking.enabled = false;
		assert!(c.validate().is_ok(), "zero grace ignored when disabled");
	}

	#[test]
	fn persistence_enabled_requires_a_retained_file() {
		let mut c = Config::default();
		c.persistence.enabled = true;
		assert!(c.validate().is_ok(), "default retained_file is set");
		assert_eq!(
			c.persistence.retained_path(),
			PathBuf::from("data/retained.mqtt")
		);

		c.persistence.retained_file = String::new();
		assert!(c.validate().is_err());
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
