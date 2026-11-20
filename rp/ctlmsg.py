"""Mirror of the guest control wire format (rp_msg_ctl_t/rp_msg_ctl_reply_t).

The frame is [u32 LE length][packed struct]; both structs are packed and
little-endian, matching guest/include/rp_protocol.h. All supported
targets are little-endian.
"""

from __future__ import annotations

import socket
import struct

PROTOCOL_VERSION = 1

CTL_BARRIER = 1
CTL_RESTART = 2
CTL_CONTINUE = 3
CTL_STOP = 4
CTL_STATUS = 5
CTL_EVENTS = 6

CTL_OK = 0
CTL_ERR_NONCE = -1
CTL_ERR_STALE = -2
CTL_ERR_NO_BARRIER = -3
CTL_ERR_BAD_MSG = -4
CTL_ERR_INTERNAL = -5

_CTL = struct.Struct("<IIIIq32s64s64s")      # 184 bytes
_CTL_REPLY = struct.Struct("<IiI160s")       # 172 bytes


def _s(text: str, size: int) -> bytes:
    b = text.encode("utf-8")
    return b + b"\x00" * (size - len(b))


def pack_ctl(*, kind: int, nonce: str, generation: int = 0,
             control_generation: int = 0, receipt_seq: int = 0,
             op_id: str = "", attempt_id: str = "") -> bytes:
    return _CTL.pack(PROTOCOL_VERSION, kind, generation,
                     control_generation, receipt_seq,
                     _s(nonce, 32), _s(op_id, 64), _s(attempt_id, 64))


def unpack_ctl_reply(data: bytes) -> dict:
    proto, status, gen, detail = _CTL_REPLY.unpack(data)
    return {"protocolVersion": proto, "status": status,
            "generation": gen,
            "detail": detail.split(b"\x00", 1)[0].decode("utf-8",
                                                         "replace")}


def ctl_request(host: str, port: int, msg: bytes,
                timeout: float = 10.0) -> dict:
    """Send one framed ctl message; return the unpacked reply."""
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(struct.pack("<I", len(msg)) + msg)
        hdr = _recv_exact(sock, 4)
        length = struct.unpack("<I", hdr)[0]
        if length > 4096:
            raise ValueError(f"implausible ctl frame length {length}")
        return unpack_ctl_reply(_recv_exact(sock, length))


def ctl_request_events(host: str, port: int, nonce: str,
                       timeout: float = 10.0) -> dict:
    """RP_CTL_EVENTS: normal reply frame, then a JSON evidence blob.

    Read-only gather for the operator bridge — returns
    {"reply": <ctl reply>, "evidence": {uname, events, clientReport}}.
    """
    import json as _json
    msg = pack_ctl(kind=CTL_EVENTS, nonce=nonce)
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(struct.pack("<I", len(msg)) + msg)
        hdr = _recv_exact(sock, 4)
        length = struct.unpack("<I", hdr)[0]
        if length > 4096:
            raise ValueError(f"implausible ctl frame length {length}")
        reply = unpack_ctl_reply(_recv_exact(sock, length))
        hdr = _recv_exact(sock, 4)
        blen = struct.unpack("<I", hdr)[0]
        if blen > 1024 * 1024:
            raise ValueError(f"implausible evidence blob length {blen}")
        evidence = _json.loads(_recv_exact(sock, blen).decode("utf-8"))
        return {"reply": reply, "evidence": evidence}


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("ctl connection closed mid-frame")
        buf += chunk
    return buf
