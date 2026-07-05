//! TLS termination via [rustls].
//!
//! A rustls server session layered over a glommio `TcpStream` is just another
//! [`ByteStream`], so the MQTT connection layer is reused verbatim over TLS — and,
//! because [`WsStream`](crate::transport::websocket::WsStream) is generic over its
//! inner stream, layering a WebSocket codec on top of a [`TlsStream`] yields
//! `wss://` with no extra protocol code.
//!
//! ## Security posture
//!
//! - **Protocol versions:** pinned to TLS 1.3 and TLS 1.2 only. rustls implements
//!   nothing older than 1.2 in the first place, so SSLv3 / TLS 1.0 / 1.1 are not
//!   merely disabled — they are absent from the codebase.
//! - **Cipher suites:** restricted to [`STRONG_CIPHER_SUITES`] — every suite is
//!   AEAD (AES-GCM or ChaCha20-Poly1305) with ECDHE forward secrecy. No CBC, no
//!   RSA key exchange, no 3DES/RC4.
//! - **Handshake timeout:** [`accept`] bounds the handshake so a client that opens
//!   the socket and stalls can't hold a connection slot (a TLS slow-loris).

use std::io::{Error, ErrorKind, Result};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures_lite::{AsyncReadExt, AsyncWriteExt, FutureExt};
use futures_rustls::pki_types::{CertificateDer, PrivateKeyDer};
use futures_rustls::rustls::crypto::{CryptoProvider, ring};
use futures_rustls::rustls::server::WebPkiClientVerifier;
use futures_rustls::rustls::{self, RootCertStore, ServerConfig, SupportedCipherSuite, SupportedProtocolVersion};
use futures_rustls::{TlsAcceptor, server};
use glommio::net::TcpStream;

use crate::transport::ByteStream;

/// The TLS-wrapped byte stream handed to the connection layer once the handshake
/// completes.
pub type TlsStream = server::TlsStream<TcpStream>;

/// The only protocol versions the broker offers: TLS 1.3 and TLS 1.2.
static PROTOCOL_VERSIONS: &[&SupportedProtocolVersion] = &[&rustls::version::TLS13, &rustls::version::TLS12];

/// The only cipher suites the broker offers, in preference order. Every entry is
/// an AEAD suite (AES-GCM or ChaCha20-Poly1305); the TLS 1.2 suites are all ECDHE,
/// so every negotiated connection has forward secrecy. There is deliberately no
/// CBC, static-RSA, 3DES, or RC4 suite here.
static STRONG_CIPHER_SUITES: &[SupportedCipherSuite] = &[
	// TLS 1.3
	ring::cipher_suite::TLS13_AES_256_GCM_SHA384,
	ring::cipher_suite::TLS13_AES_128_GCM_SHA256,
	ring::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
	// TLS 1.2 — ECDHE + AEAD only
	ring::cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	ring::cipher_suite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	ring::cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	ring::cipher_suite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	ring::cipher_suite::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
	ring::cipher_suite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
];

/// A rustls session over a TCP connection is a byte stream as-is.
impl ByteStream for TlsStream {
	async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
		AsyncReadExt::read(self, buf).await
	}

	async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
		AsyncWriteExt::write_all(self, buf).await?;
		// rustls buffers plaintext into TLS records; without a flush the trailing
		// record can sit unsent, stalling the peer (e.g. a CONNACK that never
		// arrives). Plain TCP writes go straight to the socket and need no flush.
		AsyncWriteExt::flush(self).await
	}
}

/// Client-certificate authentication policy (mutual TLS): the trusted client-CA
/// certificates and whether presenting a valid one is mandatory.
struct ClientAuth {
	ca: Vec<CertificateDer<'static>>,
	require: bool,
}

/// Loads the certificate chain and private key (and, for mutual TLS, the trusted
/// client-CA bundle) from PEM files and builds the hardened [`ServerConfig`].
/// Called at startup and again on each hot-reload; the resulting config is
/// immutable and shared (via `Arc`) for the connections that handshake under it.
pub fn load_server_config(
	cert_path: &Path,
	key_path: &Path,
	client_ca_path: Option<&Path>,
	require_client_cert: bool,
) -> Result<Arc<ServerConfig>> {
	let certs = load_certs(cert_path)?;
	let key = load_private_key(key_path)?;
	let client_auth = match client_ca_path {
		Some(path) => Some(ClientAuth { ca: load_certs(path)?, require: require_client_cert }),
		None => None,
	};
	build_server_config(certs, key, client_auth).map(Arc::new)
}

/// Outcome of mutual-TLS client authentication, handed to the connection so the
/// CONNECT handler can decide the client's MQTT identity.
#[derive(Clone, Debug, Default)]
pub enum TlsIdentity {
	/// No verified client certificate (plain TCP, or TLS without client auth).
	#[default]
	None,
	/// A client certificate was verified, but CN→username mapping is off: the
	/// client is authenticated by the certificate yet carries no MQTT identity
	/// (anonymous ACLs).
	Verified,
	/// A client certificate was verified and its subject Common Name is used as
	/// the MQTT username (per-device ACLs).
	Cn(String),
}

/// Resolves the mutual-TLS identity of a completed handshake. A present peer chain
/// means the certificate was trusted (the verifier ran during the handshake). When
/// `map_cn` is set, the leaf's subject Common Name becomes the identity; a
/// verified certificate with no usable CN falls back to [`TlsIdentity::Verified`].
pub fn client_tls_identity(stream: &TlsStream, map_cn: bool) -> TlsIdentity {
	let Some(leaf) = stream
		.get_ref()
		.1
		.peer_certificates()
		.and_then(|chain| chain.first())
	else {
		return TlsIdentity::None;
	};
	if !map_cn {
		return TlsIdentity::Verified;
	}
	match cert_common_name(leaf.as_ref()) {
		Some(cn) => TlsIdentity::Cn(cn),
		None => TlsIdentity::Verified,
	}
}

/// Extracts the subject Common Name from a DER-encoded certificate, if present.
fn cert_common_name(der: &[u8]) -> Option<String> {
	use x509_parser::prelude::{FromDer, X509Certificate};
	let (_, cert) = X509Certificate::from_der(der).ok()?;
	cert.subject()
		.iter_common_name()
		.next()
		.and_then(|attr| attr.as_str().ok())
		.map(str::to_string)
}

/// Completes the TLS handshake on an accepted TCP connection, bounded by `timeout`
/// so a peer that opens the socket but stalls mid-handshake can't hold a slot. A
/// zero timeout disables the bound.
pub async fn accept(acceptor: &TlsAcceptor, tcp: TcpStream, timeout: Duration) -> Result<TlsStream> {
	let handshake = acceptor.accept(tcp);
	if timeout.is_zero() {
		return handshake.await;
	}
	let deadline = async {
		glommio::timer::sleep(timeout).await;
		Err(Error::new(ErrorKind::TimedOut, "TLS handshake timed out"))
	};
	handshake.or(deadline).await
}

/// Builds a [`ServerConfig`] pinned to [`PROTOCOL_VERSIONS`] and
/// [`STRONG_CIPHER_SUITES`]. Without `client_auth`, clients authenticate at the
/// MQTT layer (username/password over the encrypted link); with it, mutual TLS is
/// enforced — a presented client certificate is verified against the configured
/// CA, and (when `require`) one is demanded of every client.
fn build_server_config(
	certs: Vec<CertificateDer<'static>>,
	key: PrivateKeyDer<'static>,
	client_auth: Option<ClientAuth>,
) -> Result<ServerConfig> {
	// Start from the ring provider but replace its suite list with our curated one,
	// so the security posture is explicit and auditable rather than default-derived.
	let provider = CryptoProvider {
		cipher_suites: STRONG_CIPHER_SUITES.to_vec(),
		..ring::default_provider()
	};

	let builder = ServerConfig::builder_with_provider(Arc::new(provider))
		.with_protocol_versions(PROTOCOL_VERSIONS)
		.map_err(|e| invalid(format!("configuring TLS versions: {e}")))?;
	let builder = match client_auth {
		Some(auth) => builder.with_client_cert_verifier(build_client_verifier(auth)?),
		None => builder.with_no_client_auth(),
	};
	builder
		.with_single_cert(certs, key)
		.map_err(|e| invalid(format!("loading server certificate/key: {e}")))
}

/// Builds the client-certificate verifier from the trusted client-CA bundle.
/// `require = false` verifies a certificate when offered but tolerates its
/// absence (`allow_unauthenticated`); `true` rejects any client that presents no
/// trusted certificate during the handshake.
fn build_client_verifier(auth: ClientAuth) -> Result<Arc<dyn rustls::server::danger::ClientCertVerifier>> {
	let mut roots = RootCertStore::empty();
	for cert in auth.ca {
		roots
			.add(cert)
			.map_err(|e| invalid(format!("adding client CA certificate: {e}")))?;
	}
	// Use a stock ring provider for signature verification (all algorithms present);
	// the curated suite list only governs the negotiated record-protection cipher.
	let builder = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), Arc::new(ring::default_provider()));
	let builder = if auth.require {
		builder
	} else {
		builder.allow_unauthenticated()
	};
	builder
		.build()
		.map_err(|e| invalid(format!("building client-certificate verifier: {e}")))
}

/// Reads and parses a PEM certificate chain from `path`.
fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
	let pem = std::fs::read(path).map_err(|e| context("reading certificate file", path, e))?;
	parse_certs(&pem).map_err(|e| context("parsing certificate file", path, e))
}

/// Reads and parses a PEM private key from `path`.
fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
	let pem = std::fs::read(path).map_err(|e| context("reading private key file", path, e))?;
	parse_private_key(&pem).map_err(|e| context("parsing private key file", path, e))
}

/// Parses a PEM certificate chain from raw bytes.
fn parse_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>> {
	let mut reader = std::io::BufReader::new(pem);
	let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>>>()?;
	if certs.is_empty() {
		return Err(invalid("no certificates found in PEM data".to_string()));
	}
	Ok(certs)
}

/// Parses the first PEM private key (PKCS#8, PKCS#1, or SEC1) from raw bytes.
fn parse_private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>> {
	let mut reader = std::io::BufReader::new(pem);
	rustls_pemfile::private_key(&mut reader)?.ok_or_else(|| invalid("no private key found in PEM data".to_string()))
}

fn invalid(msg: String) -> Error {
	Error::new(ErrorKind::InvalidData, msg)
}

/// Wraps an I/O error with the file path that produced it.
fn context(action: &str, path: &Path, e: Error) -> Error {
	Error::new(e.kind(), format!("{action} {}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
	use super::*;
	use futures_rustls::rustls::{ClientConfig, ClientConnection, ServerConnection};

	/// A fresh self-signed cert/key pair (PEM) for a `localhost` server.
	fn self_signed() -> (Vec<u8>, Vec<u8>) {
		let key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
		(
			key.cert.pem().into_bytes(),
			key.signing_key.serialize_pem().into_bytes(),
		)
	}

	fn test_server_config() -> Arc<ServerConfig> {
		let (cert_pem, key_pem) = self_signed();
		let certs = parse_certs(&cert_pem).unwrap();
		let key = parse_private_key(&key_pem).unwrap();
		Arc::new(build_server_config(certs, key, None).unwrap())
	}

	/// A test-only verifier that trusts any server certificate. This is confined to
	/// the test module and used solely to drive an in-memory handshake.
	#[derive(Debug)]
	struct TrustAny(Arc<CryptoProvider>);

	impl rustls::client::danger::ServerCertVerifier for TrustAny {
		fn verify_server_cert(
			&self,
			_end_entity: &CertificateDer<'_>,
			_intermediates: &[CertificateDer<'_>],
			_server_name: &futures_rustls::pki_types::ServerName<'_>,
			_ocsp: &[u8],
			_now: futures_rustls::pki_types::UnixTime,
		) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
			Ok(rustls::client::danger::ServerCertVerified::assertion())
		}

		fn verify_tls12_signature(
			&self,
			_message: &[u8],
			_cert: &CertificateDer<'_>,
			_dss: &rustls::DigitallySignedStruct,
		) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
			Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
		}

		fn verify_tls13_signature(
			&self,
			_message: &[u8],
			_cert: &CertificateDer<'_>,
			_dss: &rustls::DigitallySignedStruct,
		) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
			Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
		}

		fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
			self.0.signature_verification_algorithms.supported_schemes()
		}
	}

	/// Drives a full in-memory handshake and returns the negotiated protocol version.
	fn negotiate(client_versions: &[&'static SupportedProtocolVersion]) -> rustls::ProtocolVersion {
		let provider = Arc::new(ring::default_provider());
		let client_config = ClientConfig::builder_with_provider(provider.clone())
			.with_protocol_versions(client_versions)
			.unwrap()
			.dangerous()
			.with_custom_certificate_verifier(Arc::new(TrustAny(provider)))
			.with_no_client_auth();

		let mut client = ClientConnection::new(Arc::new(client_config), "localhost".try_into().unwrap()).unwrap();
		let mut server = ServerConnection::new(test_server_config()).unwrap();

		// Shuttle handshake records between the two ends until both are done.
		for _ in 0..16 {
			let mut buf = Vec::new();
			client.write_tls(&mut buf).unwrap();
			if !buf.is_empty() {
				server.read_tls(&mut buf.as_slice()).unwrap();
				server.process_new_packets().unwrap();
			}
			let mut buf = Vec::new();
			server.write_tls(&mut buf).unwrap();
			if !buf.is_empty() {
				client.read_tls(&mut buf.as_slice()).unwrap();
				client.process_new_packets().unwrap();
			}
			if !client.is_handshaking() && !server.is_handshaking() {
				break;
			}
		}
		assert!(!server.is_handshaking(), "handshake did not complete");
		server.protocol_version().expect("a version was negotiated")
	}

	#[test]
	fn default_client_negotiates_tls13() {
		assert_eq!(
			negotiate(&[&rustls::version::TLS13, &rustls::version::TLS12]),
			rustls::ProtocolVersion::TLSv1_3
		);
	}

	#[test]
	fn tls12_only_client_still_negotiates() {
		// Proves TLS 1.2 is genuinely enabled server-side (not just 1.3).
		assert_eq!(
			negotiate(&[&rustls::version::TLS12]),
			rustls::ProtocolVersion::TLSv1_2
		);
	}

	#[test]
	fn config_offers_only_the_curated_strong_suites() {
		let config = test_server_config();
		assert_eq!(config.crypto_provider().cipher_suites, STRONG_CIPHER_SUITES);
		// Every offered suite is AEAD with forward secrecy — no CBC anywhere.
		for suite in config.crypto_provider().cipher_suites.iter() {
			let name = format!("{:?}", suite.suite());
			assert!(!name.contains("CBC"), "weak CBC suite offered: {name}");
		}
	}

	#[test]
	fn builds_config_with_client_cert_verifier() {
		// Reuse a self-signed cert as both the server identity and the trusted client
		// CA; both the mandatory and optional (`allow_unauthenticated`) verifier paths
		// must build into a ServerConfig.
		let (cert_pem, key_pem) = self_signed();
		for require in [true, false] {
			let certs = parse_certs(&cert_pem).unwrap();
			let key = parse_private_key(&key_pem).unwrap();
			let ca = parse_certs(&cert_pem).unwrap();
			assert!(
				build_server_config(certs, key, Some(ClientAuth { ca, require })).is_ok(),
				"mutual-TLS config builds (require={require})"
			);
		}
	}

	#[test]
	fn parse_rejects_empty_pem() {
		assert!(parse_certs(b"not a pem").is_err());
		assert!(parse_private_key(b"not a pem").is_err());
	}

	#[test]
	fn parse_accepts_generated_pair() {
		let (cert_pem, key_pem) = self_signed();
		assert_eq!(parse_certs(&cert_pem).unwrap().len(), 1);
		assert!(parse_private_key(&key_pem).is_ok());
	}
}
