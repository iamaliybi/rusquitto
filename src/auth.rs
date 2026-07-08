//! Connection authentication.
//!
//! Validates the username/password a client supplies in CONNECT against the
//! `[auth]` configuration. The [`Authenticator`] is built once per shard from
//! the shared [`AuthConfig`](crate::config::AuthConfig) and shared between all
//! of that shard's connections via `Rc`.

use std::collections::HashMap;

use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHashString, PasswordVerifier};
use sha2::{Digest, Sha256};

use crate::broker::topics::{filter_matches, filter_subsumes};
use crate::config::AuthConfig;

/// A configured user's credential: a plaintext password, a SHA-256 hash
/// (lowercase hex) of it, or an Argon2 PHC string (`$argon2id$...` — salted and
/// memory-hard, the recommended form). Verification hashes the client-supplied
/// password when the stored form is a hash, so the plaintext is never kept.
enum Credential {
	Plain(String),
	Sha256(String),
	/// A parsed PHC string; the salt and parameters ride along, so each user may
	/// use different Argon2 settings.
	Argon2(PasswordHashString),
}

impl Credential {
	/// Whether `provided` (the client's password) matches this credential. The
	/// comparison is constant-time in the byte contents so a network attacker can't
	/// recover the secret from response timing.
	fn verify(&self, provided: &str) -> bool {
		match self {
			Credential::Plain(expected) => ct_eq(expected.as_bytes(), provided.as_bytes()),
			Credential::Sha256(hash) => ct_eq(sha256_hex(provided).as_bytes(), hash.as_bytes()),
			Credential::Argon2(phc) => Argon2::default()
				.verify_password(provided.as_bytes(), &phc.password_hash())
				.is_ok(),
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

/// Returns whether a *concrete publish topic* is permitted by an optional
/// allow-list: `None` is unrestricted, otherwise the topic must match at least
/// one filter rule. The topic is concrete (validated no-wildcard), so ordinary
/// filter matching is correct here.
fn acl_allows_publish(acl: &Option<Vec<String>>, topic: &str) -> bool {
	match acl {
		None => true,
		Some(rules) => rules.iter().any(|rule| filter_matches(rule, topic)),
	}
}

/// Returns whether a requested *subscription filter* is permitted by an optional
/// allow-list. The request is itself a filter (may contain `+`/`#`), so it must
/// be **subsumed** by an allow rule — not merely "matched". Using plain filter
/// matching here would let a client granted `home/+` escalate to `home/#` (the
/// whole subtree), because matching reads the request's `#` as a literal level.
fn acl_allows_subscribe(acl: &Option<Vec<String>>, filter: &str) -> bool {
	match acl {
		None => true,
		Some(rules) => rules.iter().any(|rule| filter_subsumes(rule, filter)),
	}
}

/// Per-shard credential store, built from the broker configuration.
pub struct Authenticator {
	/// Whether clients may connect without a username.
	allow_anonymous: bool,
	/// Known users, indexed by name.
	users: HashMap<String, UserEntry>,
	/// Topic-filter allow-list for anonymous publishes; `None` = unrestricted.
	anonymous_publish_acl: Option<Vec<String>>,
	/// Topic-filter allow-list for anonymous subscriptions; `None` = unrestricted.
	anonymous_subscribe_acl: Option<Vec<String>>,
	/// A throwaway Argon2 PHC hash, verified against for *unknown* usernames when
	/// any configured user has an Argon2 credential — so the response time for an
	/// unknown user matches a known one and doesn't leak which usernames exist.
	/// `None` when no user uses Argon2 (the cheap SHA-256 dummy suffices then).
	argon2_dummy: Option<PasswordHashString>,
}

impl Authenticator {
	/// Builds an authenticator from configuration, indexing users by name.
	pub fn from_config(config: &AuthConfig) -> Self {
		let users: HashMap<String, UserEntry> = config
			.users
			.iter()
			.map(|u| {
				// Config validation guarantees exactly one credential is set and
				// that a `password_hash` is well-formed (hex SHA-256 or Argon2 PHC).
				let credential = match &u.password_hash {
					Some(hash) if hash.starts_with("$argon2") => {
						let phc = PasswordHash::new(hash).expect("validated Argon2 PHC string");
						Credential::Argon2(phc.into())
					}
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
		// If any user is Argon2-hashed, unknown-user checks must burn a comparable
		// amount of work; reuse the first user's PHC string as the dummy target (the
		// dummy verify runs with a wrong password, so it always fails — only its
		// *cost* matters, and matching a real entry's parameters matches its cost).
		let argon2_dummy = users.values().find_map(|entry| match &entry.credential {
			Credential::Argon2(phc) => Some(phc.clone()),
			_ => None,
		});
		Self {
			allow_anonymous: config.allow_anonymous,
			users,
			anonymous_publish_acl: config.anonymous_publish.clone(),
			anonymous_subscribe_acl: config.anonymous_subscribe.clone(),
			argon2_dummy,
		}
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
				// Unknown user: burn the same work a known user would cost, so the
				// response time doesn't reveal which usernames exist. The verify
				// result is discarded — this arm always rejects.
				None => {
					match &self.argon2_dummy {
						Some(phc) => {
							let _ = Argon2::default()
								.verify_password(password.unwrap_or("").as_bytes(), &phc.password_hash());
						}
						None => {
							let _ = sha256_hex(password.unwrap_or(""));
						}
					}
					AuthResult::BadUserNamePassword
				}
			},
			None if self.allow_anonymous => AuthResult::Granted,
			None => AuthResult::NotAuthorized,
		}
	}

	/// Whether `username` may publish to `topic`. Users without a `publish`
	/// allow-list are unrestricted; anonymous clients (no username) are checked
	/// against the `[auth]` `anonymous_publish` allow-list (absent = unrestricted).
	pub fn authorize_publish(&self, username: Option<&str>, topic: &str) -> bool {
		match username.and_then(|u| self.users.get(u)) {
			Some(entry) => acl_allows_publish(&entry.publish_acl, topic),
			None => acl_allows_publish(&self.anonymous_publish_acl, topic),
		}
	}

	/// Whether `username` may subscribe to `filter`. Users without a `subscribe`
	/// allow-list are unrestricted; anonymous clients are checked against the
	/// `[auth]` `anonymous_subscribe` allow-list (absent = unrestricted).
	pub fn authorize_subscribe(&self, username: Option<&str>, filter: &str) -> bool {
		match username.and_then(|u| self.users.get(u)) {
			Some(entry) => acl_allows_subscribe(&entry.subscribe_acl, filter),
			None => acl_allows_subscribe(&self.anonymous_subscribe_acl, filter),
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
			..AuthConfig::default()
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
			..AuthConfig::default()
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
	fn subscribe_acl_blocks_wildcard_escalation() {
		// A user granted the single-level `home/+` must NOT be able to widen it to
		// the whole `home/#` subtree — the ACL-subsumption fix. (Plain filter
		// matching would wrongly allow this.)
		let auth = Authenticator::from_config(&acl_cfg(None, Some(&["home/+"])));
		assert!(
			auth.authorize_subscribe(Some("u"), "home/kitchen"),
			"the granted level is allowed"
		);
		assert!(
			!auth.authorize_subscribe(Some("u"), "home/#"),
			"escalation to # is blocked"
		);
		assert!(
			!auth.authorize_subscribe(Some("u"), "home/kitchen/camera"),
			"deeper is blocked"
		);
		// The same escalation via the anonymous allow-list.
		let anon = Authenticator::from_config(&AuthConfig {
			allow_anonymous: true,
			users: Vec::new(),
			anonymous_publish: None,
			anonymous_subscribe: Some(vec!["home/+".into()]),
		});
		assert!(!anon.authorize_subscribe(None, "home/#"));
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
			..AuthConfig::default()
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

	/// An Argon2id PHC string for `password` with small (fast) test parameters.
	/// Verification reads the parameters from the string itself, so a hash made
	/// with reduced cost still verifies through the default `Argon2` instance.
	fn argon2_phc(password: &str) -> String {
		use argon2::password_hash::{PasswordHasher, SaltString};
		use argon2::{Algorithm, Params, Version};
		let salt = SaltString::from_b64("dGVzdHNhbHQwMDE").unwrap();
		let params = Params::new(1024, 1, 1, None).unwrap(); // 1 MiB, 1 pass
		Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
			.hash_password(password.as_bytes(), &salt)
			.unwrap()
			.to_string()
	}

	#[test]
	fn argon2_hashed_password() {
		let auth = Authenticator::from_config(&AuthConfig {
			allow_anonymous: false,
			users: vec![UserConfig {
				username: "alice".into(),
				password: None,
				password_hash: Some(argon2_phc("s3cret")),
				publish: None,
				subscribe: None,
			}],
			..AuthConfig::default()
		});
		assert_eq!(
			auth.check(Some("alice"), Some("s3cret")),
			AuthResult::Granted
		);
		assert_eq!(
			auth.check(Some("alice"), Some("wrong")),
			AuthResult::BadUserNamePassword
		);
		// Unknown user goes through the Argon2 dummy path and still rejects.
		assert_eq!(
			auth.check(Some("mallory"), Some("s3cret")),
			AuthResult::BadUserNamePassword
		);
	}

	#[test]
	fn anonymous_acl_enforced_when_configured() {
		let auth = Authenticator::from_config(&AuthConfig {
			allow_anonymous: true,
			users: Vec::new(),
			anonymous_publish: Some(vec!["public/#".into()]),
			anonymous_subscribe: Some(vec!["public/#".into(), "status".into()]),
		});
		assert!(auth.authorize_publish(None, "public/chat"));
		assert!(!auth.authorize_publish(None, "private/secrets"));
		assert!(auth.authorize_subscribe(None, "public/#"));
		assert!(auth.authorize_subscribe(None, "status"));
		assert!(!auth.authorize_subscribe(None, "private/#"));
		// An empty allow-list denies everything.
		let deny_all = Authenticator::from_config(&AuthConfig {
			allow_anonymous: true,
			users: Vec::new(),
			anonymous_publish: Some(Vec::new()),
			anonymous_subscribe: Some(Vec::new()),
		});
		assert!(!deny_all.authorize_publish(None, "anything"));
		assert!(!deny_all.authorize_subscribe(None, "anything"));
	}
}
