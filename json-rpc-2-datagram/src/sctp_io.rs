use std::{
    io,
    os::fd::{AsRawFd, OwnedFd},
};

use tokio::io::unix::AsyncFd;

use crate::MAX_MESSAGE_BYTES;

pub(crate) async fn recv_record(fd: &AsyncFd<OwnedFd>) -> io::Result<Vec<u8>> {
    let mut buffer = vec![0; MAX_MESSAGE_BYTES];
    let mut length = 0;
    loop {
        let mut guard = fd.readable().await?;
        match guard
            .try_io(|inner| recv_fragment(inner.get_ref().as_raw_fd(), &mut buffer[length..]))
        {
            Ok(result) => {
                let (read, complete) = result?;
                if read == 0 {
                    if length != 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incomplete SCTP record",
                        ));
                    }
                    buffer.clear();
                    return Ok(buffer);
                }
                length += read;
                if complete {
                    buffer.truncate(length);
                    return Ok(buffer);
                }
                if length == buffer.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "SCTP record exceeds maximum size",
                    ));
                }
            }
            Err(_would_block) => continue,
        }
    }
}

fn recv_fragment(fd: i32, buffer: &mut [u8]) -> io::Result<(usize, bool)> {
    let mut iov = libc::iovec {
        iov_base: buffer.as_mut_ptr().cast(),
        iov_len: buffer.len(),
    };
    // SAFETY: all-zero msghdr is valid; iov references the live writable buffer.
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    let read = unsafe { libc::recvmsg(fd, &mut message, 0) };
    if read < 0 {
        return Err(io::Error::last_os_error());
    }
    if message.msg_flags & libc::MSG_TRUNC != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated SCTP record",
        ));
    }
    Ok((read as usize, message.msg_flags & libc::MSG_EOR != 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use socket2::{Domain, Protocol, Socket, Type};
    use std::{net::SocketAddr, time::Duration};

    #[tokio::test]
    #[ignore = "requires SCTP kernel support"]
    async fn partial_delivery_preserves_record_boundaries_and_eof() {
        let listener = Socket::new(
            Domain::IPV4,
            Type::STREAM,
            Some(Protocol::from(libc::IPPROTO_SCTP)),
        )
        .unwrap();
        listener.set_recv_buffer_size(16 * 1024).unwrap();
        let point: libc::c_int = 4096;
        let result = unsafe {
            libc::setsockopt(
                listener.as_raw_fd(),
                libc::IPPROTO_SCTP,
                libc::SCTP_PARTIAL_DELIVERY_POINT,
                (&point as *const libc::c_int).cast(),
                std::mem::size_of_val(&point) as libc::socklen_t,
            )
        };
        assert_eq!(result, 0, "{}", io::Error::last_os_error());
        listener
            .bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into())
            .unwrap();
        listener.listen(1).unwrap();
        let sender = Socket::new(
            Domain::IPV4,
            Type::STREAM,
            Some(Protocol::from(libc::IPPROTO_SCTP)),
        )
        .unwrap();
        sender.set_send_buffer_size(1024 * 1024).unwrap();
        sender.connect(&listener.local_addr().unwrap()).unwrap();
        let (receiver, _) = listener.accept().unwrap();
        receiver.set_nonblocking(true).unwrap();
        let receiver = unsafe { AsyncFd::register(OwnedFd::from(receiver)) }.unwrap();
        let payload: Vec<u8> = (0..128 * 1024).map(|i| (i % 251) as u8).collect();
        let expected = payload.clone();
        let sending = tokio::task::spawn_blocking(move || {
            assert_eq!(sender.send(&payload).unwrap(), payload.len());
            assert_eq!(sender.send(b"next").unwrap(), 4);
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            assert_eq!(recv_record(&receiver).await.unwrap(), expected);
            assert_eq!(recv_record(&receiver).await.unwrap(), b"next");
            assert!(recv_record(&receiver).await.unwrap().is_empty());
            sending.await.unwrap();
        })
        .await
        .unwrap();
    }
}
