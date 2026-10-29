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
