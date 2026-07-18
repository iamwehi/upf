//! The ntfy subscription wire format (server → subscriber).
//!
//! We speak ntfy's protocol so real UnifiedPush distributors (the ntfy Android
//! app, and anything else that subscribes to ntfy) work against UPF unmodified.
//! Field names and the `encoding` convention mirror ntfy's `model.Message`.

use serde::{Deserialize, Serialize};

use crate::model::Envelope;

pub const EVENT_OPEN: &str = "open";
pub const EVENT_KEEPALIVE: &str = "keepalive";
pub const EVENT_MESSAGE: &str = "message";

/// One line/frame delivered to a subscriber. Matches ntfy's JSON message object;
/// `open` and `keepalive` are control frames carrying no body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct NtfyMessage {
    pub id: String,
    pub time: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires: Option<i64>,
    pub event: String,
    pub topic: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// `""` (omitted) for UTF-8, `"base64"` for a binary `message`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub encoding: String,
}

impl NtfyMessage {
    /// The `open` control frame, sent once when a subscription is established.
    pub fn open(topic: &str, id: String, time: i64) -> Self {
        Self::control(EVENT_OPEN, topic, id, time)
    }

    /// A `keepalive` control frame, sent periodically to hold the connection.
    pub fn keepalive(topic: &str, id: String, time: i64) -> Self {
        Self::control(EVENT_KEEPALIVE, topic, id, time)
    }

    fn control(event: &str, topic: &str, id: String, time: i64) -> Self {
        Self {
            id,
            time,
            expires: None,
            event: event.to_string(),
            topic: topic.to_string(),
            message: None,
            title: None,
            priority: None,
            tags: Vec::new(),
            encoding: String::new(),
        }
    }

    /// A `message` frame carrying a queued envelope.
    pub fn message(topic: &str, id: String, env: &Envelope) -> Self {
        Self {
            id,
            time: env.received_at_secs as i64,
            expires: Some(env.expiry_secs as i64),
            event: EVENT_MESSAGE.to_string(),
            topic: topic.to_string(),
            message: Some(env.message.clone()),
            title: env.title.clone(),
            priority: env.priority,
            tags: env.tags.clone(),
            encoding: env.encoding.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::tests::arb_envelope;
    use proptest::prelude::*;

    fn arb_ntfy() -> impl Strategy<Value = NtfyMessage> {
        (
            any::<String>(),                                             // id
            any::<i64>(),                                                // time
            any::<Option<i64>>(),                                        // expires
            any::<String>(),                                             // event
            any::<String>(),                                             // topic
            any::<Option<String>>(),                                     // message
            any::<Option<String>>(),                                     // title
            any::<Option<u8>>(),                                         // priority
            any::<Vec<String>>(),                                        // tags
            prop_oneof![Just(String::new()), Just("base64".to_string())], // encoding
        )
            .prop_map(
                |(id, time, expires, event, topic, message, title, priority, tags, encoding)| {
                    NtfyMessage {
                        id,
                        time,
                        expires,
                        event,
                        topic,
                        message,
                        title,
                        priority,
                        tags,
                        encoding,
                    }
                },
            )
    }

    proptest! {
        /// Every frame we send is serialized to JSON on the wire, so it must
        /// survive a round-trip unchanged — a subscriber decoding it must see the
        /// message we built.
        #[test]
        fn ntfy_message_round_trips(m in arb_ntfy()) {
            let bytes = serde_json::to_vec(&m).unwrap();
            let back: NtfyMessage = serde_json::from_slice(&bytes).unwrap();
            prop_assert_eq!(back, m);
        }

        /// A `message` frame is a faithful projection of the queued envelope: the
        /// body, metadata and ntfy timestamps must all carry through exactly, or a
        /// delivered notification wouldn't match what was published.
        #[test]
        fn message_frame_mirrors_envelope(env in arb_envelope(), topic in ".*", id in ".*") {
            let m = NtfyMessage::message(&topic, id.clone(), &env);
            prop_assert_eq!(&m.event, EVENT_MESSAGE);
            prop_assert_eq!(&m.id, &id);
            prop_assert_eq!(&m.topic, &topic);
            prop_assert_eq!(m.time, env.received_at_secs as i64);
            prop_assert_eq!(m.expires, Some(env.expiry_secs as i64));
            prop_assert_eq!(&m.message, &Some(env.message.clone()));
            prop_assert_eq!(&m.title, &env.title);
            prop_assert_eq!(m.priority, env.priority);
            prop_assert_eq!(&m.tags, &env.tags);
            prop_assert_eq!(&m.encoding, &env.encoding);
        }

        /// The client's `since=` history replay and any future inbound framing
        /// feed untrusted bytes to the deserializer; it must only ever `Ok`/`Err`,
        /// never panic.
        #[test]
        fn deserialize_never_panics(s in ".*") {
            let _ = serde_json::from_str::<NtfyMessage>(&s);
        }
    }

    // ---- golden wire format -------------------------------------------------
    // These pin the exact JSON real ntfy/UnifiedPush clients parse. A stray
    // `#[serde(rename)]` or reordered field would break every subscriber but
    // survive the round-trip tests above — only a byte-for-byte golden catches it.

    #[test]
    fn open_frame_is_minimal_json() {
        let m = NtfyMessage::open("mytopic", "abc".into(), 100);
        let j = serde_json::to_string(&m).unwrap();
        assert_eq!(j, r#"{"id":"abc","time":100,"event":"open","topic":"mytopic"}"#);
    }

    #[test]
    fn keepalive_frame_is_minimal_json() {
        let m = NtfyMessage::keepalive("mytopic", "abc".into(), 100);
        let j = serde_json::to_string(&m).unwrap();
        assert_eq!(
            j,
            r#"{"id":"abc","time":100,"event":"keepalive","topic":"mytopic"}"#
        );
    }

    #[test]
    fn message_frame_json_matches_ntfy() {
        let env = Envelope {
            message: "hello".into(),
            encoding: String::new(),
            title: Some("Hi".into()),
            priority: Some(4),
            tags: vec!["a".into(), "b".into()],
            received_at_secs: 1000,
            expiry_secs: 2000,
        };
        let m = NtfyMessage::message("mytopic", "id123".into(), &env);
        let j = serde_json::to_string(&m).unwrap();
        assert_eq!(
            j,
            r#"{"id":"id123","time":1000,"expires":2000,"event":"message","topic":"mytopic","message":"hello","title":"Hi","priority":4,"tags":["a","b"]}"#
        );
    }

    #[test]
    fn binary_message_carries_base64_encoding() {
        let env = Envelope {
            message: "aGVsbG8".into(),
            encoding: "base64".into(),
            title: None,
            priority: None,
            tags: vec![],
            received_at_secs: 1000,
            expiry_secs: 2000,
        };
        let m = NtfyMessage::message("t", "id".into(), &env);
        let j = serde_json::to_string(&m).unwrap();
        assert_eq!(
            j,
            r#"{"id":"id","time":1000,"expires":2000,"event":"message","topic":"t","message":"aGVsbG8","encoding":"base64"}"#
        );
    }
}
