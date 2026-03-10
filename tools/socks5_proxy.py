#!/usr/bin/env python3
"""
Minimal SOCKS5 proxy server (no auth, no external deps).

Usage:
    python3 socks5_proxy.py [--host 127.0.0.1] [--port 1080]

Designed to run on a local machine and be reverse-tunneled to a VPS
so the VPS can route traffic through the local machine's IP.
"""
import argparse
import logging
import select
import socket
import struct
import threading

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("socks5")

SOCKS5_VER = 0x05
RSV = 0x00
ATYP_IPV4 = 0x01
ATYP_DOMAIN = 0x03
ATYP_IPV6 = 0x04
CMD_CONNECT = 0x01
BUF = 65536


def _recv_exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("client disconnected")
        data += chunk
    return data


def _relay(client, remote):
    """Bidirectional relay until one side closes."""
    sockets = [client, remote]
    try:
        while True:
            readable, _, errored = select.select(sockets, [], sockets, 60)
            if errored:
                break
            for s in readable:
                data = s.recv(BUF)
                if not data:
                    return
                dst = remote if s is client else client
                dst.sendall(data)
    except (OSError, ConnectionError):
        pass
    finally:
        client.close()
        remote.close()


def handle_client(client, addr):
    try:
        # --- Greeting ---
        header = _recv_exact(client, 2)
        ver, nmethods = struct.unpack("!BB", header)
        if ver != SOCKS5_VER:
            client.close()
            return
        _recv_exact(client, nmethods)  # consume method list
        # Reply: no auth required
        client.sendall(struct.pack("!BB", SOCKS5_VER, 0x00))

        # --- Request ---
        req = _recv_exact(client, 4)
        ver, cmd, _, atyp = struct.unpack("!BBBB", req)
        if cmd != CMD_CONNECT:
            # Only CONNECT supported
            client.sendall(struct.pack("!BBBBIH", SOCKS5_VER, 0x07, RSV, ATYP_IPV4, 0, 0))
            client.close()
            return

        # Parse destination
        if atyp == ATYP_IPV4:
            raw = _recv_exact(client, 4)
            dst_addr = socket.inet_ntoa(raw)
        elif atyp == ATYP_DOMAIN:
            dlen = struct.unpack("!B", _recv_exact(client, 1))[0]
            dst_addr = _recv_exact(client, dlen).decode()
        elif atyp == ATYP_IPV6:
            raw = _recv_exact(client, 16)
            dst_addr = socket.inet_ntop(socket.AF_INET6, raw)
        else:
            client.sendall(struct.pack("!BBBBIH", SOCKS5_VER, 0x08, RSV, ATYP_IPV4, 0, 0))
            client.close()
            return

        dst_port = struct.unpack("!H", _recv_exact(client, 2))[0]
        log.info(f"{addr[0]}:{addr[1]} → {dst_addr}:{dst_port}")

        # Connect to target
        try:
            remote = socket.create_connection((dst_addr, dst_port), timeout=30)
        except Exception as e:
            log.warning(f"connect failed: {dst_addr}:{dst_port}: {e}")
            client.sendall(struct.pack("!BBBBIH", SOCKS5_VER, 0x05, RSV, ATYP_IPV4, 0, 0))
            client.close()
            return

        # Success reply
        bind = remote.getsockname()
        client.sendall(struct.pack(
            "!BBBB4sH", SOCKS5_VER, 0x00, RSV, ATYP_IPV4,
            socket.inet_aton(bind[0]), bind[1],
        ))

        # Relay data
        _relay(client, remote)

    except Exception as e:
        log.debug(f"handler error for {addr}: {e}")
        try:
            client.close()
        except OSError:
            pass


def main():
    parser = argparse.ArgumentParser(description="Minimal SOCKS5 proxy")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1080)
    args = parser.parse_args()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((args.host, args.port))
    srv.listen(128)
    log.info(f"SOCKS5 proxy listening on {args.host}:{args.port}")

    try:
        while True:
            client, addr = srv.accept()
            threading.Thread(target=handle_client, args=(client, addr), daemon=True).start()
    except KeyboardInterrupt:
        log.info("shutting down")
    finally:
        srv.close()


if __name__ == "__main__":
    main()
