"""Probe kernel socket support without binding server ports."""

import errno
import ipaddress
import socket
import sys


TRANSPORTS = (
    ("udplite", socket.SOCK_DGRAM, 136),
    ("dccp", 6, 33),
    ("sctp", socket.SOCK_SEQPACKET, 132),
)
UNSUPPORTED = {
    errno.EAFNOSUPPORT,
    errno.EPROTONOSUPPORT,
    errno.ESOCKTNOSUPPORT,
}


def probe(bind, socket_factory=socket.socket):
    addresses = [
        ipaddress.ip_address(item.strip().strip("[]"))
        for item in bind.split(",")
        if item.strip()
    ]
    families = {
        socket.AF_INET if address.version == 4 else socket.AF_INET6
        for address in addresses
    } or {socket.AF_INET, socket.AF_INET6}
    supported = []
    for name, kind, protocol in TRANSPORTS:
        for family in sorted(families):
            try:
                with socket_factory(family, kind, protocol):
                    pass
            except OSError as error:
                if error.errno not in UNSUPPORTED:
                    raise
                print(
                    f"Skipping {name} and ws-{name}: "
                    f"kernel socket support unavailable for {family.name}: {error}",
                    file=sys.stderr,
                )
                break
        else:
            supported.append(name)
    return supported


if __name__ == "__main__":
    print(",".join(probe(sys.argv[1])))
