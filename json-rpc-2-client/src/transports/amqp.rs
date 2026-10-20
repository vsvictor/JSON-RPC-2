//! JSON-RPC over AMQP-style body frames on a persistent TCP stream.

use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

use super::{closed, transport_error};
use crate::{
    ClientCloseFuture, ClientPingFuture, ClientRecoverFuture, ClientTransportFuture,
    RpcClientTransport, TransportError, expects_response,
};
use json_rpc_2_core::{AmqpFrame, AmqpMessageKind, TcpTuningConfig, decode_amqp_frame, encode_amqp_frame};

/// AMQP-inspired client transport using framed body messages and heartbeats.
pub struct AmqpClientTransport {
    address: SocketAddr,
    tuning: TcpTuningConfig,
    next_correlation_id: AtomicU64,
    connection: Mutex<Option<TcpStream>>,
}

impl AmqpClientTransport {
    pub async fn connect(address: SocketAddr) -> Result<Self, TransportError> {
        Self::connect_with_tuning(address, TcpTuningConfig::default()).await
    }

    pub async fn connect_with_tuning(
        address: SocketAddr,
        tuning: TcpTuningConfig,
    ) -> Result<Self, TransportError> {
        Ok(Self {
            address,
            tuning: tuning.clone(),
            next_correlation_id: AtomicU64::new(1),
            connection: Mutex::new(Some(Self::open(address, &tuning).await?)),
        })
    }

    async fn open(address: SocketAddr, tuning: &TcpTuningConfig) -> Result<TcpStream, TransportError> {
        super::connect_tcp_stream(address, tuning).await
    }

    fn next_correlation_id(&self) -> u64 {
        self.next_correlation_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl RpcClientTransport for AmqpClientTransport {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let expects = expects_response(&message);
            self.send_with_expectation(message, expects).await
        })
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            let frame = if expects_response {
                AmqpFrame::request(self.next_correlation_id(), message.to_vec())
            } else {
                AmqpFrame::notification(message.to_vec())
            };
            let mut connection = self.connection.lock().await;
            let stream = connection.as_mut().ok_or_else(closed)?;
            write_frame(stream, &frame).await?;
            if !expects_response {
                return Ok(None);
            }
            loop {
                match read_frame(stream).await? {
                    AmqpFrame {
                        kind: Some(AmqpMessageKind::Response),
                        payload,
                        ..
                    } => return Ok(Some(Bytes::from(payload))),
                    AmqpFrame { kind: None, .. } => return Ok(None),
                    _ => continue,
                }
            }
        })
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move {
            let mut connection = self.connection.lock().await;
            let stream = connection.as_mut().ok_or_else(closed)?;
            write_frame(stream, &AmqpFrame::heartbeat()).await?;
            match read_frame(stream).await? {
                AmqpFrame { kind: None, .. } => Ok(()),
                other => Err(TransportError::new(format!(
                    "unexpected AMQP heartbeat response: {other:?}"
                ))),
            }
        })
    }

    fn kind(&self) -> &'static str {
        "amqp"
    }

    fn peer(&self) -> Option<String> {
        Some(self.address.to_string())
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let replacement = Self::open(self.address, &self.tuning).await?;
            let previous = self.connection.lock().await.replace(replacement);
            if let Some(mut stream) = previous {
                let _ = stream.shutdown().await;
            }
            Ok(())
        })
    }

    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move {
            let Some(mut stream) = self.connection.lock().await.take() else {
                return Ok(());
            };
            stream.shutdown().await.map_err(transport_error)
        })
    }
}

async fn write_frame(stream: &mut TcpStream, frame: &AmqpFrame) -> Result<(), TransportError> {
    let encoded = encode_amqp_frame(frame).map_err(TransportError::new)?;
    stream.write_all(&encoded).await.map_err(transport_error)
}

async fn read_frame(stream: &mut TcpStream) -> Result<AmqpFrame, TransportError> {
    let mut header = [0u8; 7];
    stream.read_exact(&mut header).await.map_err(transport_error)?;
    let payload_len = u32::from_be_bytes([header[3], header[4], header[5], header[6]]) as usize;
    let mut tail = vec![0u8; payload_len + 1];
    stream.read_exact(&mut tail).await.map_err(transport_error)?;
    let mut packet = header.to_vec();
    packet.extend_from_slice(&tail);
    decode_amqp_frame(&packet).map_err(TransportError::new)
}
