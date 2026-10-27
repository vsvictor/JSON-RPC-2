use std::{io, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use tokio::{net::UdpSocket, sync::mpsc};

pub type OwnedDatagram = (Bytes, SocketAddr);

/// Either the socket's sole reader or an owned packet lane from a demultiplexer.
#[derive(Clone)]
pub struct DatagramReceiver {
    socket: Arc<UdpSocket>,
    lane: Option<Arc<tokio::sync::Mutex<mpsc::Receiver<OwnedDatagram>>>>,
}

pub enum ReceivedDatagram<'a> {
    Socket(&'a [u8], SocketAddr),
    Owned(OwnedDatagram),
}

impl ReceivedDatagram<'_> {
    pub fn packet(&self) -> &[u8] {
        match self {
            Self::Socket(packet, _) => packet,
            Self::Owned((packet, _)) => packet,
        }
    }

    pub fn peer(&self) -> SocketAddr {
        match self {
            Self::Socket(_, peer) | Self::Owned((_, peer)) => *peer,
        }
    }
}

impl From<UdpSocket> for DatagramReceiver {
    fn from(socket: UdpSocket) -> Self {
        Self {
            socket: Arc::new(socket),
            lane: None,
        }
    }
}

impl DatagramReceiver {
    /// The dispatcher alone reads the socket; this lane sends replies directly.
    pub fn shared(socket: Arc<UdpSocket>, lane: mpsc::Receiver<OwnedDatagram>) -> Self {
        Self {
            socket,
            lane: Some(Arc::new(tokio::sync::Mutex::new(lane))),
        }
    }

    pub fn socket(&self) -> Arc<UdpSocket> {
        self.socket.clone()
    }

    /// Clear buffered packets when replacing a shared profile's session state.
    pub async fn discard_pending(&self) {
        if let Some(lane) = &self.lane {
            let mut lane = lane.lock().await;
            for _ in 0..lane.len() {
                if lane.try_recv().is_err() {
                    break;
                }
            }
        }
    }

    pub async fn receive<'a>(&mut self, buffer: &'a mut [u8]) -> io::Result<ReceivedDatagram<'a>> {
        if let Some(lane) = &mut self.lane {
            lane.lock()
                .await
                .recv()
                .await
                .map(ReceivedDatagram::Owned)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "UDP ingress lane closed")
                })
        } else {
            let (size, peer) = self.socket.recv_from(buffer).await?;
            Ok(ReceivedDatagram::Socket(&buffer[..size], peer))
        }
    }
}
