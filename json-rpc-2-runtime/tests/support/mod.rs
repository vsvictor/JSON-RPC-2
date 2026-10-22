use std::net::IpAddr;

/// Matrix tests exercise available carriers, but must not hide real bind errors.
/// Dedicated DCCP CI sets JSON_RPC_REQUIRE_DCCP=1 to reject a missing kernel.
pub fn dccp_available(ip: IpAddr) -> bool {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        use socket2::{Domain, Protocol, Socket, Type};
        match Socket::new(
            Domain::for_address(std::net::SocketAddr::new(ip, 0)),
            Type::from(libc::SOCK_DCCP),
            Some(Protocol::from(libc::IPPROTO_DCCP)),
        ) {
            Ok(_) => true,
            Err(error)
                if matches!(
                    error.raw_os_error(),
                    Some(libc::EPROTONOSUPPORT | libc::ESOCKTNOSUPPORT | libc::EAFNOSUPPORT)
                ) =>
            {
                assert!(
                    std::env::var_os("JSON_RPC_REQUIRE_DCCP").is_none(),
                    "DCCP coverage is required but the kernel cannot provide it: {error}"
                );
                eprintln!(
                    "EXCLUDED DCCP/WS-DCCP for {ip}: kernel protocol unavailable ({error}); DCCP behavior was NOT tested"
                );
                false
            }
            Err(error) => panic!("DCCP capability probe failed for {ip}: {error}"),
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = ip;
        false
    }
}
