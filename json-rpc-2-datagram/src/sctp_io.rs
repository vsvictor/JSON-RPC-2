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
mod tests;
