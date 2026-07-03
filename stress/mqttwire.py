"""Minimal, dependency-free MQTT 5 wire codec.

Just enough of the protocol to build *both* valid and deliberately-malformed
packets by hand — the adversarial scenarios need byte-level control that a normal
client library (paho) hides. Everything here is stdlib only.
"""

from __future__ import annotations

import socket
import struct

# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------


def varint(n: int) -> bytes:
    """Encode an MQTT Variable Byte Integer (used for remaining/property length)."""
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        if n > 0:
            b |= 0x80
        out.append(b)
        if n == 0:
            return bytes(out)


def mqtt_str(s) -> bytes:
    """Encode a UTF-8 string / bytes as a 2-byte length prefix + data."""
    b = s.encode() if isinstance(s, str) else s
    return struct.pack("!H", len(b)) + b


# ---------------------------------------------------------------------------
# Control packets (valid by default; every field is overridable for fuzzing)
# ---------------------------------------------------------------------------


def connect(
    client_id: str = "",
    keepalive: int = 60,
    clean_start: bool = True,
    username: str | None = None,
    password: str | None = None,
    properties: bytes = b"",
    protocol_name: str = "MQTT",
    protocol_level: int = 5,
) -> bytes:
    flags = 0
    if clean_start:
        flags |= 0x02
    if password is not None:
        flags |= 0x40
    if username is not None:
        flags |= 0x80

    vh = mqtt_str(protocol_name) + bytes([protocol_level]) + bytes([flags])
    vh += struct.pack("!H", keepalive)
    vh += varint(len(properties)) + properties

    payload = mqtt_str(client_id)
    if username is not None:
        payload += mqtt_str(username)
    if password is not None:
        payload += mqtt_str(password)

    body = vh + payload
    return bytes([0x10]) + varint(len(body)) + body


def publish(
    topic: str,
    payload: bytes = b"",
    qos: int = 0,
    retain: bool = False,
    dup: bool = False,
    pkid: int = 0,
    properties: bytes = b"",
) -> bytes:
    flags = (qos & 0x03) << 1
    if dup:
        flags |= 0x08
    if retain:
        flags |= 0x01

    vh = mqtt_str(topic)
    if qos > 0:
        vh += struct.pack("!H", pkid or 1)
    vh += varint(len(properties)) + properties

    data = payload if isinstance(payload, (bytes, bytearray)) else payload.encode()
    body = vh + data
    return bytes([0x30 | flags]) + varint(len(body)) + body


def subscribe(filters, pkid: int = 1, properties: bytes = b"") -> bytes:
    """filters: list of (topic, options_byte). options low 2 bits = requested QoS."""
    vh = struct.pack("!H", pkid) + varint(len(properties)) + properties
    payload = b""
    for topic, opts in filters:
        payload += mqtt_str(topic) + bytes([opts & 0xFF])
    body = vh + payload
    return bytes([0x82]) + varint(len(body)) + body


PINGREQ = bytes([0xC0, 0x00])
DISCONNECT = bytes([0xE0, 0x00])

# Packet type nibbles (for decoding replies).
CONNACK, PUBLISH, PUBACK, PUBREC, SUBACK, PINGRESP, DISCONNECT_T = 2, 3, 4, 5, 9, 13, 14


def read_packet(sock: socket.socket, timeout: float = 2.0):
    """Read one packet. Returns (ptype, flags, body) or None on EOF/short close."""
    sock.settimeout(timeout)
    head = _recv_exact(sock, 1)
    if head is None:
        return None
    b0 = head[0]

    mult, length = 1, 0
    for _ in range(4):  # a VBI is at most 4 bytes
        eb = _recv_exact(sock, 1)
        if eb is None:
            return None
        byte = eb[0]
        length += (byte & 0x7F) * mult
        if not byte & 0x80:
            break
        mult *= 128
    else:
        raise ValueError("malformed remaining length (>4 bytes)")

    body = _recv_exact(sock, length) if length else b""
    if body is None:
        return None
    return (b0 >> 4, b0 & 0x0F, body)


def _recv_exact(sock: socket.socket, n: int):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def expect_connack_ok(sock: socket.socket, timeout: float = 3.0) -> bool:
    """Read a CONNACK and return True iff the reason code is 0x00 (Success)."""
    pkt = read_packet(sock, timeout)
    if pkt is None or pkt[0] != CONNACK:
        return False
    body = pkt[2]
    # CONNACK body: [ack flags][reason code][properties...]; success == 0x00.
    return len(body) >= 2 and body[1] == 0x00
