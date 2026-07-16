//! Parsing of the WebPush request headers we forward (RFC 8030 §5).

use axum::http::HeaderMap;

use crate::storage::MessageHeaders;

/// Extract the WebPush headers we care about from a request.
///
/// Lenient by design: malformed values are dropped rather than rejected, since
/// the walking skeleton forwards headers as hints for the distributor.
pub fn extract(headers: &HeaderMap) -> MessageHeaders {
    MessageHeaders {
        ttl: str_header(headers, "ttl").and_then(|v| v.parse().ok()),
        topic: str_header(headers, "topic"),
        urgency: str_header(headers, "urgency"),
        content_encoding: str_header(headers, "content-encoding"),
    }
}

fn str_header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}
