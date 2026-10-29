//! Backend-side implementation of the JSON-RPC-2-GATEWAY v3 wire contract.
//!
//! [`GatewayBackendProtocol`] wraps any [`JsonRpcProtocol`] so that a backend built on
//! `json-rpc-2-runtime` can sit behind the gateway; it knows nothing about the
//! wrapped application. [`GatewayFraming`] splits TCP/TLS streams into gateway frames.

use std::{io, sync::Arc};

use bytes::{BufMut, Bytes, BytesMut};
use json_rpc_2_transport::{
    FrameBoundary, JsonRpcProtocol, ProtocolFuture, ProtocolMessageFuture, StreamFraming,
    TransportContext, TransportMessage,
};

const MAGIC: &[u8; 8] = b"JRPC-GW\x03";
const HEADER: usize = 17;
const MAX_PAYLOAD: usize = json_rpc_2_core::MAX_MESSAGE_BYTES - HEADER - 256;

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid gateway v3 frame")
}

fn lengths(frame: &[u8]) -> io::Result<(usize, usize, usize)> {
    if frame.len() < HEADER || !frame.starts_with(MAGIC) {
        return Err(invalid());
    }
    let id = u16::from_be_bytes([frame[9], frame[10]]) as usize;
    let transport = u16::from_be_bytes([frame[11], frame[12]]) as usize;
    let payload = u32::from_be_bytes([frame[13], frame[14], frame[15], frame[16]]) as usize;
    let valid_kind = match frame[8] {
        1 => id > 0 && transport > 0,
        2 => id == 0 && transport == 0,
        3..=6 => id == 0 && transport == 0 && payload == 0,
        _ => false,
    };
    if !valid_kind || id > 128 || transport > 128 || payload > MAX_PAYLOAD {
        return Err(invalid());
    }
    Ok((id, transport, payload))
}

/// Stream codec shared by backend listeners and compatible TCP/TLS clients.
pub struct GatewayFraming;

impl StreamFraming for GatewayFraming {
    fn boundary(&self, prefix: &[u8]) -> io::Result<FrameBoundary> {
        let compared = prefix.len().min(MAGIC.len());
        if prefix[..compared] != MAGIC[..compared] {
            return Ok(FrameBoundary::Line);
        }
        if prefix.len() < HEADER {
            return Ok(FrameBoundary::Need(HEADER));
        }
        let (id, transport, payload) = lengths(prefix)?;
        Ok(FrameBoundary::Frame(HEADER + id + transport + payload))
    }
}

fn envelope(kind: u8, payload: Bytes) -> TransportMessage {
    let mut header = BytesMut::with_capacity(HEADER);
    header.extend_from_slice(MAGIC);
    header.put_u8(kind);
    header.put_u16(0);
    header.put_u16(0);
    header.put_u32(payload.len() as u32);
    TransportMessage {
        header: header.freeze(),
        payload,
        binary: true,
    }
}

fn token(bytes: &[u8]) -> io::Result<String> {
    if bytes.is_empty()
        || !bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'-')
    {
        return Err(invalid());
    }
    String::from_utf8(bytes.to_vec()).map_err(|_| invalid())
}

enum Request<'a> {
    Ping,
    Rpc(TransportContext, &'a [u8]),
}

fn decode(input: &[u8]) -> io::Result<Request<'_>> {
    let (id, transport, payload) = lengths(input)?;
    if input.len() != HEADER + id + transport + payload {
        return Err(invalid());
    }
    match input[8] {
        5 => Ok(Request::Ping),
        1 => {
            let end_id = HEADER + id;
            let end_transport = end_id + transport;
            Ok(Request::Rpc(
                TransportContext {
                    connection_id: token(&input[HEADER..end_id])?,
                    transport: token(&input[end_id..end_transport])?,
                    // A gateway connection is not the original client's push channel.
                    push_sender: None,
                },
                &input[end_transport..],
            ))
        }
        _ => Err(invalid()),
    }
}

/// Decodes gateway v3 frames, dispatches the payload to `inner` and encodes the reply.
pub struct GatewayBackendProtocol {
    inner: Arc<dyn JsonRpcProtocol>,
}

impl GatewayBackendProtocol {
    pub fn new(inner: Arc<dyn JsonRpcProtocol>) -> Self {
        Self { inner }
    }

    async fn dispatch(
        &self,
        input: &[u8],
        context: Option<&TransportContext>,
    ) -> Option<TransportMessage> {
        if !input.starts_with(b"JRPC-GW") {
            return match context {
                Some(context) => self.inner.handle_transport_message(input, context).await,
                None => self
                    .inner
                    .handle_message(input)
                    .await
                    .map(TransportMessage::from),
            };
        }
        let (context, payload) = match decode(input) {
            Ok(Request::Ping) => return Some(envelope(6, Bytes::new())),
            Ok(Request::Rpc(context, payload)) => (context, payload),
            Err(error) => {
                tracing::warn!(%error, "rejecting gateway frame");
                return Some(envelope(4, Bytes::new()));
            }
        };
        Some(
            match self
                .inner
                .handle_message_with_context(payload, &context)
                .await
            {
                None => envelope(3, Bytes::new()),
                Some(response) if response.len() <= MAX_PAYLOAD => envelope(2, response),
                Some(_) => {
                    tracing::error!("response exceeds gateway payload limit");
                    envelope(4, Bytes::new())
                }
            },
        )
    }
}

impl JsonRpcProtocol for GatewayBackendProtocol {
    fn stream_framing(&self) -> Option<&dyn StreamFraming> {
        Some(&GatewayFraming)
    }

    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.dispatch(input, None)
                .await
                .map(TransportMessage::into_bytes)
        })
    }

    fn handle_message_with_context<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.dispatch(input, Some(context))
                .await
                .map(TransportMessage::into_bytes)
        })
    }

    fn handle_transport_message<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolMessageFuture<'a> {
        Box::pin(self.dispatch(input, Some(context)))
    }

    fn session_opened(&self, context: &TransportContext) {
        self.inner.session_opened(context);
    }

    fn session_closed(&self, context: &TransportContext) {
        self.inner.session_closed(context);
    }

    fn session_cancelled<'a>(
        &'a self,
        context: &'a TransportContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        self.inner.session_cancelled(context)
    }

    fn session_close_requested(&self, context: &TransportContext) -> bool {
        self.inner.session_close_requested(context)
    }
}

#[cfg(test)]
mod tests;
