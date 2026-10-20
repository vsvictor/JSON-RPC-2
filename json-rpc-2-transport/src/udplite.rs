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
mod tests {
    use std::os::fd::AsRawFd;

    use socket2::{Domain, Protocol, Type};

    use super::*;

    fn coverage(socket: &Socket, option: libc::c_int) -> libc::c_int {
        let mut value: libc::c_int = -1;
        let mut length = std::mem::size_of_val(&value) as libc::socklen_t;
        let result = unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                libc::IPPROTO_UDPLITE,
                option,
                (&mut value as *mut libc::c_int).cast(),
                &mut length,
            )
        };
        assert_eq!(result, 0, "{}", io::Error::last_os_error());
        value
    }

    #[test]
    fn applies_send_and_receive_coverage() {
        for domain in [Domain::IPV4, Domain::IPV6] {
            let socket = match Socket::new(
                domain,
                Type::DGRAM,
                Some(Protocol::from(libc::IPPROTO_UDPLITE)),
            ) {
                Ok(socket) => socket,
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EPROTONOSUPPORT | libc::EAFNOSUPPORT)
                    ) =>
                {
                    eprintln!("Skipping UDP-Lite {domain:?}: {error}");
                    continue;
                }
                Err(error) => panic!("UDP-Lite socket creation failed: {error}"),
            };
            for (send, recv) in [(0, 0), (8, 8), (16, 24), (u16::MAX, u16::MAX)] {
                apply_udp_lite_coverage(
                    &socket,
                    &UdpLiteConfig {
                        send_checksum_coverage: send,
                        recv_checksum_coverage: recv,
                    },
                )
                .unwrap();
                assert_eq!(coverage(&socket, 10), i32::from(send));
                assert_eq!(coverage(&socket, 11), i32::from(recv));
            }
        }
    }

    #[test]
    fn propagates_socket_option_errors() {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).unwrap();
        let error = apply_udp_lite_coverage(&socket, &UdpLiteConfig::default()).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ENOPROTOOPT));
    }
}
