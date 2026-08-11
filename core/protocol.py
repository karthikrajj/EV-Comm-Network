"""
EV-Comm Packet Protocol
=======================
Defines the JSON packet format with seq_no, type, payload, checksum, TTL, and timestamp.
Provides helpers for creating, serializing, validating, and computing checksums on packets.
"""

import json
import hashlib
import time
from datetime import datetime, timezone
from typing import Optional


# ── Message types ──────────────────────────────────────────────────────────────
MSG_EMERGENCY_REQUEST = "EMERGENCY_REQUEST"
MSG_ACK               = "ACK"
MSG_NACK              = "NACK"
MSG_HEARTBEAT         = "HEARTBEAT"
MSG_ROUTE_UPDATE      = "ROUTE_UPDATE"
MSG_SIGNAL_CHANGE     = "SIGNAL_CHANGE"
MSG_HOSPITAL_NOTIFY   = "HOSPITAL_NOTIFY"
MSG_HOSPITAL_ACK      = "HOSPITAL_ACK"
MSG_AUTH              = "AUTH"
MSG_AUTH_RESPONSE     = "AUTH_RESPONSE"
MSG_REGISTER          = "REGISTER"
MSG_STATUS_UPDATE     = "STATUS_UPDATE"
MSG_CONGESTION_ALERT  = "CONGESTION_ALERT"
MSG_NODE_DOWN         = "NODE_DOWN"
MSG_NODE_UP           = "NODE_UP"
MSG_RELAY             = "RELAY"

# Priority levels
PRIORITY_CRITICAL = "CRITICAL"
PRIORITY_URGENT   = "URGENT"
PRIORITY_STABLE   = "STABLE"

# Sequence number counter (global per process)
_seq_counter = 0


def _next_seq() -> int:
    """Thread-safe-ish sequence number generator with wraparound."""
    global _seq_counter
    _seq_counter = (_seq_counter + 1) % 65536
    return _seq_counter


def compute_checksum(data: dict) -> str:
    """Compute a 6-char hex checksum over the packet payload (excluding checksum field itself)."""
    filtered = {k: v for k, v in data.items() if k != "checksum"}
    raw = json.dumps(filtered, sort_keys=True).encode("utf-8")
    return hashlib.md5(raw).hexdigest()[:6]


def create_packet(
    msg_type: str,
    sender: str,
    receiver: str,
    payload: Optional[dict] = None,
    ttl: int = 4,
    seq_no: Optional[int] = None,
) -> dict:
    """Build a well-formed EV-Comm packet."""
    packet = {
        "seq_no": seq_no if seq_no is not None else _next_seq(),
        "type": msg_type,
        "sender": sender,
        "receiver": receiver,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload or {},
        "ttl": ttl,
    }
    packet["checksum"] = compute_checksum(packet)
    return packet


def validate_packet(packet: dict) -> bool:
    """Verify a packet's checksum and required fields."""
    required = {"seq_no", "type", "sender", "receiver", "timestamp", "payload", "checksum", "ttl"}
    if not required.issubset(packet.keys()):
        return False
    expected = compute_checksum(packet)
    return packet["checksum"] == expected


def serialize(packet: dict) -> bytes:
    """Serialize packet to bytes for transmission. Uses a length-prefix framing protocol."""
    data = json.dumps(packet).encode("utf-8")
    length = len(data)
    # 4-byte big-endian length prefix
    return length.to_bytes(4, "big") + data


def deserialize_from_buffer(buffer: bytes):
    """
    Attempt to extract one complete packet from the buffer.
    Returns (packet_dict, remaining_buffer) or (None, buffer) if incomplete.
    """
    if len(buffer) < 4:
        return None, buffer
    length = int.from_bytes(buffer[:4], "big")
    if len(buffer) < 4 + length:
        return None, buffer
    data = buffer[4:4 + length]
    remaining = buffer[4 + length:]
    try:
        packet = json.loads(data.decode("utf-8"))
        return packet, remaining
    except json.JSONDecodeError:
        return None, remaining


def make_ack(original: dict, sender: str) -> dict:
    """Create an ACK for a given packet."""
    return create_packet(
        MSG_ACK, sender, original["sender"],
        payload={"ack_seq": original["seq_no"]},
    )


def make_nack(original: dict, sender: str, reason: str = "") -> dict:
    """Create a NACK for a given packet."""
    return create_packet(
        MSG_NACK, sender, original["sender"],
        payload={"nack_seq": original["seq_no"], "reason": reason},
    )
