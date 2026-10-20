use serde::{Deserialize, Serialize};

/// Socket-level tuning for UDP-Lite (RFC 3828).
///
/// Coverage values are measured in bytes. `0` means "full packet coverage"
/// per RFC 3828, while values `>= 8` request partial checksum coverage that at
/// least protects the UDP-Lite header itself.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct UdpLiteConfig {
    /// Checksum coverage to request for outbound packets.
    pub send_checksum_coverage: u16,
    /// Minimum checksum coverage required for inbound packets.
    pub recv_checksum_coverage: u16,
}

impl Default for UdpLiteConfig {
    fn default() -> Self {
        Self {
            send_checksum_coverage: 8,
            recv_checksum_coverage: 8,
        }
    }
}

impl UdpLiteConfig {
    pub fn validate(&self) -> Result<(), String> {
        validate_coverage(self.send_checksum_coverage, "send_checksum_coverage")?;
        validate_coverage(self.recv_checksum_coverage, "recv_checksum_coverage")?;
        Ok(())
    }
}

fn validate_coverage(value: u16, field: &str) -> Result<(), String> {
    if value == 0 || value >= 8 {
        Ok(())
    } else {
        Err(format!(
            "UDP-Lite {field} must be 0 (full coverage) or at least 8 bytes"
        ))
    }
}
