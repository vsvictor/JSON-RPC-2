#[cfg(feature = "http3")]
pub mod http3;
#[cfg(feature = "quic")]
pub mod quic;

#[cfg(feature = "http3")]
pub(crate) fn uri_host(server_name: &str) -> String {
    match server_name.parse::<std::net::Ipv6Addr>() {
        Ok(ip) => format!("[{ip}]"),
        Err(_) => server_name.to_owned(),
    }
}
