//! SUBSCRIBE / UNSUBSCRIBE: shared-filter parsing, authorization, the per-client
//! subscription cap, and replay of matching retained messages.

use mqttbytes::v5::{self as mqtt_v5};
use std::io::Result;
use std::rc::Rc;
use tracing::{debug, warn};

use super::Connection;
use crate::broker::delivery::Delivery;
use crate::broker::topics::SubOptions;
use crate::protocol::{min_qos, parse_shared_filter, sub_reason_code, valid_subscribe_filter};
use crate::transport::ByteStream;

impl<S: ByteStream> Connection<S> {
	pub(super) async fn handle_subscribe(&mut self, subscribe: mqtt_v5::Subscribe) -> Result<()> {
		// Register each filter in this shard's subscription table, build the
		// per-filter SubAck reason codes, and collect any retained messages whose
		// topic matches (to replay to this client after the SubAck).
		let mut return_codes = Vec::with_capacity(subscribe.filters.len());
		let mut retained = Vec::new();

		// A single Subscription Identifier (if any) applies to every filter in this
		// SUBSCRIBE and is echoed on matching deliveries.
		let sub_id = subscribe.properties.as_ref().and_then(|p| p.id);

		for filter in &subscribe.filters {
			// A Shared Subscription filter is `$share/{group}/{topic-filter}`; the
			// effective filter used for matching, ACL, and retained replay is the
			// `{topic-filter}` part, and `group` load-balances delivery.
			let (effective, share_group) = match parse_shared_filter(&filter.path) {
				Ok(pair) => pair,
				Err(()) => {
					warn!(filter = %filter.path, "invalid shared subscription filter");
					return_codes.push(mqtt_v5::SubscribeReasonCode::TopicFilterInvalid);
					continue;
				}
			};

			// Deny unauthorized filters: no trie entry, no retained replay, and a
			// Not Authorized reason code in the SubAck for this filter.
			if !self
				.auth
				.authorize_subscribe(self.username.as_deref(), effective)
			{
				warn!(filter = %effective, "subscribe not authorized, rejecting");
				return_codes.push(mqtt_v5::SubscribeReasonCode::NotAuthorized);
				continue;
			}

			// Reject syntactically invalid filters before touching the trie.
			if !valid_subscribe_filter(effective) {
				warn!(filter = %effective, "invalid subscribe filter, rejecting");
				return_codes.push(mqtt_v5::SubscribeReasonCode::TopicFilterInvalid);
				continue;
			}

			// No Local on a Shared Subscription is a Protocol Error (MQTT 5
			// §3.8.3.1). Refusing it also keeps the cluster-wide shared-delivery
			// pick consistent: every shard must see the same candidate set for a
			// group, which a per-publisher exclusion would break.
			if share_group.is_some() && filter.nolocal {
				warn!(filter = %filter.path, "No Local set on a shared subscription, rejecting");
				return_codes.push(mqtt_v5::SubscribeReasonCode::TopicFilterInvalid);
				continue;
			}

			let granted = min_qos(filter.qos, self.limits.max_qos());

			{
				let mut state = self.shard.borrow_mut();
				let is_new = state.subscribe(
					effective,
					&self.client_id,
					SubOptions {
						qos: granted,
						nolocal: filter.nolocal,
						retain_as_published: filter.preserve_retain,
						share_group,
						sub_id,
					},
				);
				// Enforce the per-client subscription cap. A brand-new subscription
				// over quota is rolled back and refused; existing ones still update.
				let max = self.limits.max_subscriptions_per_client;
				if is_new && max > 0 && self.subscription_count >= max {
					state.unsubscribe(effective, &self.client_id, share_group);
					drop(state);
					warn!(max, "subscription quota exceeded, rejecting filter");
					return_codes.push(mqtt_v5::SubscribeReasonCode::QuotaExceeded);
					continue;
				}
				if is_new {
					self.subscription_count += 1;
				}
				// Retain Handling decides whether to replay retained messages now.
				// Shared subscriptions never receive retained messages on subscribe.
				let send_retained = share_group.is_none()
					&& match filter.retain_forward_rule {
						mqtt_v5::RetainForwardRule::OnEverySubscribe => true,
						mqtt_v5::RetainForwardRule::OnNewSubscribe => is_new,
						mqtt_v5::RetainForwardRule::Never => false,
					};
				if send_retained {
					for message in state.retained_matching(effective) {
						retained.push((message, granted));
					}
				}
			}

			debug!(filter = %effective, group = ?share_group, granted = ?granted, "subscribed");

			return_codes.push(sub_reason_code(granted));
		}

		let sub_ack = mqtt_v5::SubAck::new(subscribe.pkid, return_codes);
		self.send(|buf| sub_ack.write(buf))?;

		// Replay matching retained messages, delivered with the retain flag set and
		// downgraded to min(message QoS, granted QoS) for this subscription. Routed
		// through `deliver` so the in-flight window is respected. Each carries the
		// SUBSCRIBE's Subscription Identifier (if any).
		let sub_ids: Vec<usize> = sub_id.into_iter().collect();
		for (message, granted) in retained {
			let qos = min_qos(message.qos, granted);
			self.deliver(Delivery {
				publish: Rc::new(message),
				qos,
				retain: true,
				sub_ids: sub_ids.clone(),
			})?;
		}

		Ok(())
	}

	pub(super) async fn handle_unsubscribe(&mut self, unsubscribe: mqtt_v5::Unsubscribe) -> Result<()> {
		let mut reasons = Vec::with_capacity(unsubscribe.filters.len());

		for filter in &unsubscribe.filters {
			// Mirror the SUBSCRIBE parse so a `$share/{group}/{topic}` unsubscribe
			// removes the matching shared entry rather than a phantom literal filter.
			let (effective, share_group) = parse_shared_filter(filter).unwrap_or((filter, None));
			let removed = self
				.shard
				.borrow_mut()
				.unsubscribe(effective, &self.client_id, share_group);
			if removed {
				self.subscription_count = self.subscription_count.saturating_sub(1);
			}
			debug!(filter = %effective, group = ?share_group, "unsubscribed");
			reasons.push(mqtt_v5::UnsubAckReason::Success);
		}

		let mut unsub_ack = mqtt_v5::UnsubAck::new(unsubscribe.pkid);
		unsub_ack.reasons = reasons;
		self.send(|buf| unsub_ack.write(buf))
	}
}
