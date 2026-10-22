//! Opt-in, opaque gateway framing usable over any message transport.

use std::sync::Arc;

use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;

use crate::{
    GatewayRequestMetadata, JsonRpcProtocol, ProtocolFuture, TransportContext, TransportError,
};

const PREFIX: &str = "JRPC-GW/2 ";
const NONE: &[u8] = b"JRPC-GW/2 NONE";
const ERROR: &[u8] = b"JRPC-GW/2 ERROR";
const RESPONSE: &str = "JRPC-GW/2 RESPONSE ";
pub const GATEWAY_PING: &[u8] = b"JRPC-GW/2 PING";
pub const GATEWAY_PONG: &[u8] = b"JRPC-GW/2 PONG";

/// Maximum opaque payload, allowing space for base64 and metadata in a carrier frame.
pub const MAX_GATEWAY_PAYLOAD_BYTES: usize = (json_rpc_2_core::MAX_MESSAGE_BYTES - 512) / 4 * 3;

pub fn encode_request(
    payload: &[u8],
    connection_id: &str,
    source_transport: &str,
) -> Result<Bytes, TransportError> {
    if connection_id.len() > 128 || source_transport.len() > 128 {
        return Err(TransportError::new("gateway metadata exceeds field limit"));
    }
    if payload.len() > MAX_GATEWAY_PAYLOAD_BYTES {
        return Err(TransportError::new("gateway payload exceeds frame limit"));
    }
    let metadata = format!("JRPC-GW/1 {connection_id} {source_transport}");
    let parsed = GatewayRequestMetadata::parse(&metadata)
        .map_err(|error| TransportError::new(error.to_string()))?
        .ok_or_else(|| TransportError::new("missing gateway metadata"))?;
    if parsed.connection_id != connection_id
        || parsed.transport != source_transport
        || parsed.payload_length.is_some()
    {
        return Err(TransportError::new(
            "gateway metadata must contain single tokens",
        ));
    }
    Ok(Bytes::from(format!(
        "{PREFIX}{connection_id} {source_transport} {}",
        STANDARD.encode(payload)
    )))
}

pub fn decode_response(frame: &[u8]) -> Result<Option<Bytes>, TransportError> {
    if frame == NONE {
        return Ok(None);
    }
    let encoded = frame.strip_prefix(RESPONSE.as_bytes()).ok_or_else(|| {
        TransportError::new("invalid gateway response; enable gateway framing on backend")
    })?;
    decode_payload(encoded).map(Some)
}

fn decode_payload(encoded: &[u8]) -> Result<Bytes, TransportError> {
    if encoded.len() > MAX_GATEWAY_PAYLOAD_BYTES.div_ceil(3) * 4 {
        return Err(TransportError::new("gateway payload exceeds frame limit"));
    }
    let decoded = STANDARD
        .decode(encoded)
        .map_err(|error| TransportError::new(format!("invalid gateway base64 payload: {error}")))?;
    if decoded.len() > MAX_GATEWAY_PAYLOAD_BYTES {
        return Err(TransportError::new("gateway payload exceeds frame limit"));
    }
    Ok(Bytes::from(decoded))
}

/// Wrap a backend protocol explicitly; ordinary JSON-RPC messages are unchanged.
pub struct GatewayBackendProtocol {
    inner: Arc<dyn JsonRpcProtocol>,
}

impl GatewayBackendProtocol {
    pub fn new(inner: Arc<dyn JsonRpcProtocol>) -> Self {
        Self { inner }
    }

    async fn dispatch(&self, input: &[u8], context: Option<&TransportContext>) -> Option<Bytes> {
        if input == GATEWAY_PING {
            return Some(Bytes::from_static(GATEWAY_PONG));
        }
        let Some(rest) = input.strip_prefix(PREFIX.as_bytes()) else {
            return match context {
                Some(context) => self.inner.handle_message_with_context(input, context).await,
                None => self.inner.handle_message(input).await,
            };
        };
        let decoded = (|| {
            let text = std::str::from_utf8(rest)
                .map_err(|error| TransportError::new(error.to_string()))?;
            let mut fields = text.splitn(3, ' ');
            let connection_id = fields.next().unwrap_or_default();
            let transport = fields.next().unwrap_or_default();
            let payload = fields
                .next()
                .ok_or_else(|| TransportError::new("missing gateway payload"))?;
            let metadata =
                GatewayRequestMetadata::parse(&format!("JRPC-GW/1 {connection_id} {transport}"))
                    .map_err(|error| TransportError::new(error.to_string()))?
                    .ok_or_else(|| TransportError::new("missing gateway metadata"))?;
            Ok::<_, TransportError>((metadata.context(None), decode_payload(payload.as_bytes())?))
        })();
        let (context, payload) = match decoded {
            Ok(decoded) => decoded,
            Err(error) => {
                tracing::warn!(%error, "invalid gateway backend frame");
                return Some(Bytes::from_static(ERROR));
            }
        };
        match self
            .inner
            .handle_message_with_context(&payload, &context)
            .await
        {
            None => Some(Bytes::from_static(NONE)),
            Some(response) if response.len() <= MAX_GATEWAY_PAYLOAD_BYTES => Some(Bytes::from(
                format!("{RESPONSE}{}", STANDARD.encode(response)),
            )),
            Some(_) => {
                tracing::error!("backend response exceeds gateway frame limit");
                Some(Bytes::from_static(ERROR))
            }
        }
    }
}

impl JsonRpcProtocol for GatewayBackendProtocol {
    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(self.dispatch(input, None))
    }

    fn handle_message_with_context<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolFuture<'a> {
        Box::pin(self.dispatch(input, Some(context)))
    }

    fn session_opened(&self, context: &TransportContext) {
        self.inner.session_opened(context);
    }

    fn session_closed(&self, context: &TransportContext) {
        self.inner.session_closed(context);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Echo;
    impl JsonRpcProtocol for Echo {
        fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
            Box::pin(async move { (!input.is_empty()).then(|| Bytes::copy_from_slice(input)) })
        }

        fn handle_message_with_context<'a>(
            &'a self,
            input: &'a [u8],
            context: &'a TransportContext,
        ) -> ProtocolFuture<'a> {
            assert_eq!(context.connection_id, "client-1");
            assert_eq!(context.transport, "ws-tcp");
            self.handle_message(input)
        }
    }

    #[tokio::test]
    async fn preserves_opaque_binary_and_multiline_payload_and_metadata() {
        let protocol = GatewayBackendProtocol::new(Arc::new(Echo));
        let payload = b"\x00\xff\nunchanged";
        let request = encode_request(payload, "client-1", "ws-tcp").unwrap();
        assert!(!request.contains(&b'\n'));
        let response = protocol.handle_message(&request).await.unwrap();
        assert_eq!(
            decode_response(&response).unwrap().unwrap(),
            payload.as_slice()
        );
    }

    #[tokio::test]
    async fn acknowledges_no_response_and_preserves_direct_clients() {
        let protocol = GatewayBackendProtocol::new(Arc::new(Echo));
        let request = encode_request(b"", "client-1", "ws-tcp").unwrap();
        let response = protocol.handle_message(&request).await.unwrap();
        assert_eq!(decode_response(&response).unwrap(), None);
        assert_eq!(
            protocol.handle_message(b"direct").await.unwrap(),
            b"direct".as_slice()
        );
    }

    #[tokio::test]
    async fn malformed_frames_fail_explicitly() {
        let protocol = GatewayBackendProtocol::new(Arc::new(Echo));
        for request in [b"JRPC-GW/2 a".as_slice(), b"JRPC-GW/2 a tcp !!!"] {
            let response = protocol.handle_message(request).await.unwrap();
            assert!(decode_response(&response).is_err());
        }
        assert!(encode_request(b"x", "bad\nid", "tcp").is_err());
    }

    #[test]
    fn frame_limit_includes_base64_and_metadata() {
        let payload = vec![0; MAX_GATEWAY_PAYLOAD_BYTES];
        let frame = encode_request(&payload, &"a".repeat(128), &"b".repeat(128)).unwrap();
        assert!(frame.len() <= json_rpc_2_core::MAX_MESSAGE_BYTES);
        assert!(encode_request(&vec![0; MAX_GATEWAY_PAYLOAD_BYTES + 1], "a", "tcp").is_err());
        assert!(decode_response(b"JRPC-GW/2 RESPONSE !").is_err());
    }
}
