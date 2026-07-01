//! Connection authentication.
//!
//! Validates the username/password a client supplies in CONNECT against the
//! `[auth]` configuration. The [`Authenticator`] is built once per shard from
//! the shared [`AuthConfig`](crate::config::AuthConfig) and shared between all
//! of that shard's connections via `Rc`.

use std::collections::HashMap;

use crate::config::AuthConfig;

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

/// Per-shard credential store, built from the broker configuration.
pub struct Authenticator {
	/// Whether clients may connect without a username.
	allow_anonymous: bool,
	/// Known `username -> password` pairs.
	users: HashMap<String, String>,
}

impl Authenticator {
	/// Builds an authenticator from configuration, indexing users by name.
	pub fn from_config(config: &AuthConfig) -> Self {
		let users = config
			.users
			.iter()
			.map(|u| (u.username.clone(), u.password.clone()))
			.collect();
		Self {
			allow_anonymous: config.allow_anonymous,
			users,
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
				Some(expected) if expected == password.unwrap_or("") => AuthResult::Granted,
				_ => AuthResult::BadUserNamePassword,
			},
			None if self.allow_anonymous => AuthResult::Granted,
			None => AuthResult::NotAuthorized,
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
					password: p.to_string(),
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
		assert_eq!(auth.check(Some("alice"), Some("s3cret")), AuthResult::Granted);
		assert_eq!(
			auth.check(Some("alice"), Some("wrong")),
			AuthResult::BadUserNamePassword
		);
		assert_eq!(
			auth.check(Some("bob"), Some("s3cret")),
			AuthResult::BadUserNamePassword
		);
	}
}
