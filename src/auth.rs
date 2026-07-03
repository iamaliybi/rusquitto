//! Connection authentication.
//!
//! Validates the username/password a client supplies in CONNECT against the
//! `[auth]` configuration. The [`Authenticator`] is built once per shard from
//! the shared [`AuthConfig`](crate::config::AuthConfig) and shared between all
//! of that shard's connections via `Rc`.

use std::collections::HashMap;

use sha2::{Digest, Sha256};

use crate::broker::topics::filter_matches;
use crate::config::AuthConfig;

/// A configured user's credential: either a plaintext password or a SHA-256 hash
/// (lowercase hex) of it. Verification hashes the client-supplied password when
/// the stored form is a hash, so the plaintext is never kept in that case.
enum Credential {
	Plain(String),
	Sha256(String),
}

impl Credential {
	/// Whether `provided` (the client's password) matches this credential. The
	/// comparison is constant-time in the byte contents so a network attacker can't
	/// recover the secret from response timing.
	fn verify(&self, provided: &str) -> bool {
		match self {
			Credential::Plain(expected) => ct_eq(expected.as_bytes(), provided.as_bytes()),
			Credential::Sha256(hash) => ct_eq(sha256_hex(provided).as_bytes(), hash.as_bytes()),
		}
	}
}

/// Constant-time byte-string equality: folds every byte difference so the running
/// time depends only on the input length, not on where (or whether) they differ.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
	if a.len() != b.len() {
		return false;
	}
	let mut diff = 0u8;
	for (x, y) in a.iter().zip(b) {
		diff |= x ^ y;
	}
	diff == 0
}

/// Lowercase-hex SHA-256 of a string.
fn sha256_hex(s: &str) -> String {
	let digest = Sha256::digest(s.as_bytes());
	let mut out = String::with_capacity(64);
	for byte in digest {
		use std::fmt::Write;
		let _ = write!(out, "{byte:02x}");
	}
	out
}

/// Result of an authentication check. The caller maps each variant to the
/// matching CONNACK reason code.
#[derive(Debug, PartialEq, Eq)]
pub enum AuthResult {
	/// Credentials accepted (or auth is not required).
	Granted,
	/// A username was supplied but did not match a known user / password.
	BadUserNamePassword,
	/// No credentials were supplied and anonymous access is disabled.
	NotAuthorized,
}

/// A configured user's credentials and per-operation topic authorization.
struct UserEntry {
	credential: Credential,
	/// Topic-filter allow-list for publishing; `None` means unrestricted.
	publish_acl: Option<Vec<String>>,
	/// Topic-filter allow-list for subscribing; `None` means unrestricted.
	subscribe_acl: Option<Vec<String>>,
}

/// Returns whether `topic` is permitted by an optional allow-list: `None` is
/// unrestricted, otherwise the topic must match at least one filter rule.
fn acl_allows(acl: &Option<Vec<String>>, topic: &str) -> bool {
	match acl {
		None => true,
		Some(rules) => rules.iter().any(|rule| filter_matches(rule, topic)),
	}
}

/// Per-shard credential store, built from the broker configuration.
pub struct Authenticator {
	/// Whether clients may connect without a username.
	allow_anonymous: bool,
	/// Known users, indexed by name.
	users: HashMap<String, UserEntry>,
}

impl Authenticator {
	/// Builds an authenticator from configuration, indexing users by name.
	pub fn from_config(config: &AuthConfig) -> Self {
		let users = config
			.users
			.iter()
			.map(|u| {
				// Config validation guarantees exactly one credential is set.
				let credential = match &u.password_hash {
					Some(hash) => Credential::Sha256(hash.to_ascii_lowercase()),
					None => Credential::Plain(u.password.clone().unwrap_or_default()),
				};
				(
					u.username.clone(),
					UserEntry {
						credential,
						publish_acl: u.publish.clone(),
						subscribe_acl: u.subscribe.clone(),
					},
				)
			})
			.collect();
		Self { allow_anonymous: config.allow_anonymous, users }
	}

	/// Whether authentication is effectively a no-op: anonymous access is allowed
	/// and no users are configured, so every client is accepted.
	pub fn is_open(&self) -> bool {
		self.allow_anonymous && self.users.is_empty()
	}

	/// Checks a client's credentials.
	///
	/// - A supplied username must match a configured user with the same password.
	/// - With no username, access depends on `allow_anonymous`.
	///
	/// A configured empty password matches a client that sends no password.
	pub fn check(&self, username: Option<&str>, password: Option<&str>) -> AuthResult {
		match username {
			Some(name) => match self.users.get(name) {
				Some(entry) if entry.credential.verify(password.unwrap_or("")) => AuthResult::Granted,
				Some(_) => AuthResult::BadUserNamePassword,
				// Unknown user: run a throwaway hash so the response time doesn't
				// reveal whether the username exists (user-enumeration timing oracle).
				None => {
					let _ = sha256_hex(password.unwrap_or(""));
					AuthResult::BadUserNamePassword
				}
			},
			None if self.allow_anonymous => AuthResult::Granted,
			None => AuthResult::NotAuthorized,
		}
	}

	/// Whether `username` may publish to `topic`. Anonymous clients (no username)
	/// and users without a `publish` allow-list are unrestricted.
	pub fn authorize_publish(&self, username: Option<&str>, topic: &str) -> bool {
		match username.and_then(|u| self.users.get(u)) {
			Some(entry) => acl_allows(&entry.publish_acl, topic),
			None => true,
		}
	}

	/// Whether `username` may subscribe to `filter`. Anonymous clients and users
	/// without a `subscribe` allow-list are unrestricted.
	pub fn authorize_subscribe(&self, username: Option<&str>, filter: &str) -> bool {
		match username.and_then(|u| self.users.get(u)) {
			Some(entry) => acl_allows(&entry.subscribe_acl, filter),
			None => true,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::config::UserConfig;

	fn cfg(allow_anonymous: bool, users: &[(&str, &str)]) -> AuthConfig {
		AuthConfig {
			allow_anonymous,
			users: users
				.iter()
				.map(|(u, p)| UserConfig {
					username: u.to_string(),
					password: Some(p.to_string()),
					password_hash: None,
					publish: None,
					subscribe: None,
				})
				.collect(),
		}
	}

	#[test]
	fn open_when_anonymous_and_no_users() {
		let auth = Authenticator::from_config(&cfg(true, &[]));
		assert!(auth.is_open());
		assert_eq!(auth.check(None, None), AuthResult::Granted);
	}

	#[test]
	fn anonymous_rejected_when_disabled() {
		let auth = Authenticator::from_config(&cfg(false, &[("a", "b")]));
		assert_eq!(auth.check(None, None), AuthResult::NotAuthorized);
	}

	#[test]
	fn known_user_good_and_bad_password() {
		let auth = Authenticator::from_config(&cfg(false, &[("alice", "s3cret")]));
		assert_eq!(
			auth.check(Some("alice"), Some("s3cret")),
			AuthResult::Granted
		);
		assert_eq!(
			auth.check(Some("alice"), Some("wrong")),
			AuthResult::BadUserNamePassword
		);
		assert_eq!(
			auth.check(Some("bob"), Some("s3cret")),
			AuthResult::BadUserNamePassword
		);
	}

	fn acl_cfg(publish: Option<&[&str]>, subscribe: Option<&[&str]>) -> AuthConfig {
		let to_vec = |o: Option<&[&str]>| o.map(|s| s.iter().map(|x| x.to_string()).collect());
		AuthConfig {
			allow_anonymous: true,
			users: vec![UserConfig {
				username: "u".into(),
				password: Some("p".into()),
				password_hash: None,
				publish: to_vec(publish),
				subscribe: to_vec(subscribe),
			}],
		}
	}

	#[test]
	fn no_acl_is_unrestricted() {
		let auth = Authenticator::from_config(&acl_cfg(None, None));
		assert!(auth.authorize_publish(Some("u"), "any/topic"));
		assert!(auth.authorize_subscribe(Some("u"), "any/#"));
	}

	#[test]
	fn anonymous_is_unrestricted() {
		let auth = Authenticator::from_config(&acl_cfg(Some(&["only/this"]), Some(&["only/this"])));
		assert!(auth.authorize_publish(None, "something/else"));
		assert!(auth.authorize_subscribe(None, "something/else"));
	}

	#[test]
	fn publish_acl_enforced_with_wildcards() {
		let auth = Authenticator::from_config(&acl_cfg(Some(&["sensors/#"]), None));
		assert!(auth.authorize_publish(Some("u"), "sensors/kitchen/temp"));
		assert!(!auth.authorize_publish(Some("u"), "actuators/door"));
	}

	#[test]
	fn subscribe_acl_enforced() {
		let auth = Authenticator::from_config(&acl_cfg(None, Some(&["sensors/#"])));
		assert!(auth.authorize_subscribe(Some("u"), "sensors/+/temp"));
		assert!(!auth.authorize_subscribe(Some("u"), "commands/#"));
	}

	#[test]
	fn empty_acl_denies_everything() {
		let auth = Authenticator::from_config(&acl_cfg(Some(&[]), None));
		assert!(!auth.authorize_publish(Some("u"), "anything"));
	}

	#[test]
	fn sha256_hashed_password() {
		// SHA-256("s3cret") in lowercase hex.
		let hash = sha256_hex("s3cret");
		let auth = Authenticator::from_config(&AuthConfig {
			allow_anonymous: false,
			users: vec![UserConfig {
				username: "alice".into(),
				password: None,
				password_hash: Some(hash.to_ascii_uppercase()), // stored uppercase, normalized on load
				publish: None,
				subscribe: None,
			}],
		});
		assert_eq!(
			auth.check(Some("alice"), Some("s3cret")),
			AuthResult::Granted
		);
		assert_eq!(
			auth.check(Some("alice"), Some("wrong")),
			AuthResult::BadUserNamePassword
		);
	}
}
