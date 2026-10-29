use std::io;

use json_rpc_2_core::UdpLiteConfig;
use socket2::Socket;

/// Apply UDP-Lite send and receive checksum coverage to a socket.
pub fn apply_udp_lite_coverage(socket: &Socket, config: &UdpLiteConfig) -> io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        use std::os::fd::AsRawFd;

        // Linux UDP-Lite socket option numbers are not exposed by libc.
        const UDPLITE_SEND_CSCOV: libc::c_int = 10;
        const UDPLITE_RECV_CSCOV: libc::c_int = 11;

        set_option(
            socket.as_raw_fd(),
            UDPLITE_SEND_CSCOV,
            config.send_checksum_coverage,
        )?;
        set_option(
            socket.as_raw_fd(),
            UDPLITE_RECV_CSCOV,
            config.recv_checksum_coverage,
        )
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = (socket, config);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "UDP-Lite is only supported on linux/android in this crate",
        ))
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn set_option(fd: std::os::fd::RawFd, name: libc::c_int, coverage: u16) -> io::Result<()> {
    let value: libc::c_int = coverage.into();
    let result = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_UDPLITE,
            name,
            (&value as *const libc::c_int).cast(),
            std::mem::size_of_val(&value) as libc::socklen_t,
        )
    };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
mod tests;
