use std::io;

/// Incremental boundary of an application-defined frame on a mixed text stream.
pub enum FrameBoundary {
    /// This prefix belongs to the ordinary UTF-8 line protocol.
    Line,
    /// More header bytes are needed before the total size can be determined.
    Need(usize),
    /// Total frame size, including its header.
    Frame(usize),
}

/// Optional application framing for TCP/TLS. Implementations inspect only
/// framing headers; the transport enforces its own maximum before reading more.
pub trait StreamFraming: Send + Sync + 'static {
    fn boundary(&self, prefix: &[u8]) -> io::Result<FrameBoundary>;
}
