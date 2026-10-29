use bytes::{Bytes, BytesMut};

#[cfg(test)]
mod tests;

/// A message whose small envelope and opaque payload have separate ownership.
///
/// Stream adapters can write both segments without copying the payload. Adapters
/// requiring one contiguous buffer must call `into_bytes` at their boundary.
#[derive(Clone, Debug)]
pub struct TransportMessage {
    pub header: Bytes,
    pub payload: Bytes,
    /// Request binary carrier encoding even when the bytes happen to be UTF-8.
    pub binary: bool,
}

impl TransportMessage {
    pub fn binary(payload: Bytes) -> Self {
        Self {
            header: Bytes::new(),
            payload,
            binary: true,
        }
    }

    pub fn len(&self) -> usize {
        self.header.len() + self.payload.len()
    }

    pub fn is_empty(&self) -> bool {
        self.header.is_empty() && self.payload.is_empty()
    }

    pub fn into_bytes(self) -> Bytes {
        if self.header.is_empty() {
            return self.payload;
        }
        if self.payload.is_empty() {
            return self.header;
        }
        let mut bytes = BytesMut::with_capacity(self.len());
        bytes.extend_from_slice(&self.header);
        bytes.extend_from_slice(&self.payload);
        bytes.freeze()
    }
}

impl From<Bytes> for TransportMessage {
    fn from(payload: Bytes) -> Self {
        Self {
            header: Bytes::new(),
            payload,
            binary: false,
        }
    }
}

impl http_body::Body for TransportMessage {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        let next = if !self.header.is_empty() {
            Some(std::mem::take(&mut self.header))
        } else if !self.payload.is_empty() {
            Some(std::mem::take(&mut self.payload))
        } else {
            None
        };
        std::task::Poll::Ready(next.map(|bytes| Ok(http_body::Frame::data(bytes))))
    }

    fn is_end_stream(&self) -> bool {
        self.is_empty()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.len() as u64)
    }
}
