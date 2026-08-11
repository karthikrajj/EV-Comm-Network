"""
EV-Comm Junction Client
========================
TCP + UDP client representing a traffic junction node.
  - TCP: receives SIGNAL_CHANGE commands from the server
  - UDP: sends periodic HEARTBEAT packets to the server
Demonstrates: TCP vs UDP split, heartbeat-based failure detection.
"""

import socket
import json
import time
import sys
import os
import threading
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.protocol import (
    create_packet, serialize, deserialize_from_buffer, make_ack, validate_packet,
    MSG_AUTH, MSG_AUTH_RESPONSE, MSG_HEARTBEAT, MSG_SIGNAL_CHANGE, MSG_ACK,
    MSG_RELAY,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [JUNCTION] %(message)s",
)
logger = logging.getLogger("Junction")

SERVER_HOST = "127.0.0.1"
TCP_PORT = 9000
UDP_PORT = 9001
HEARTBEAT_INTERVAL = 3.0


class JunctionClient:
    """Traffic junction node — TCP for commands, UDP for heartbeats."""

    def __init__(self, jnc_id: str, token: str):
        self.jnc_id = jnc_id
        self.token = token
        self.tcp_sock: socket.socket = None
        self.udp_sock: socket.socket = None
        self.running = False
        self.signal_state = "RED"
        self.buffer = b""

    def connect(self):
        """Connect TCP and prepare UDP."""
        # TCP connection
        self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_sock.connect((SERVER_HOST, TCP_PORT))

        # UDP socket (no connection needed — send to server)
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.running = True
        logger.info(f"🚦 Junction {self.jnc_id} connected")

        # Authenticate via TCP
        auth_pkt = create_packet(
            MSG_AUTH, self.jnc_id, "SERVER",
            payload={"token": self.token, "client_type": "junction"},
        )
        self.tcp_sock.sendall(serialize(auth_pkt))

        # Wait for auth response
        response = self._recv_packet()
        if response and response["type"] == MSG_AUTH_RESPONSE:
            if response["payload"]["status"] == "OK":
                logger.info(f"✅ Authenticated: {response['payload']['message']}")
            else:
                logger.error(f"❌ Auth failed: {response['payload']['message']}")
                self.disconnect()
                return False

        # Start threads
        threading.Thread(target=self._tcp_listener, daemon=True).start()
        threading.Thread(target=self._heartbeat_sender, daemon=True).start()
        return True

    def _recv_packet(self) -> dict:
        """Receive a single packet from TCP (blocking)."""
        while True:
            packet, self.buffer = deserialize_from_buffer(self.buffer)
            if packet:
                return packet
            data = self.tcp_sock.recv(4096)
            if not data:
                return None
            self.buffer += data

    def _tcp_listener(self):
        """Listen for TCP commands from the server."""
        while self.running:
            try:
                data = self.tcp_sock.recv(4096)
                if not data:
                    break
                self.buffer += data

                while True:
                    packet, self.buffer = deserialize_from_buffer(self.buffer)
                    if packet is None:
                        break
                    self._handle_packet(packet)
            except (ConnectionResetError, OSError):
                break
        logger.info(f"🔌 Junction {self.jnc_id} TCP disconnected")

    def _handle_packet(self, packet: dict):
        """Handle incoming TCP packets."""
        if packet["type"] == MSG_SIGNAL_CHANGE:
            signal = packet["payload"]["signal"]
            ambulance = packet["payload"].get("for_ambulance", "")
            old_signal = self.signal_state
            self.signal_state = signal
            tag = "GREEN" if signal == "GREEN" else "RED"
            logger.info(
                f"[SIGNAL] {old_signal} -> {signal} "
                f"(for ambulance {ambulance})"
            )
            # Send ACK
            ack = make_ack(packet, self.jnc_id)
            self.tcp_sock.sendall(serialize(ack))

        elif packet["type"] == MSG_RELAY:
            # Multi-hop relay: store-and-forward (Feature #9)
            payload = packet["payload"]
            ttl = packet.get("ttl", 0) - 1
            hop_count = payload.get("hop_count", 0) + 1
            final_dest = payload.get("final_dest", "")
            inner_type = payload.get("inner_type", "")

            logger.info(
                f"[RELAY] Received relay for {final_dest} "
                f"(hop {hop_count}, TTL {ttl}) - store & forward"
            )

            # Apply the inner signal change if this IS the final destination
            if final_dest == self.jnc_id:
                signal = payload.get("signal", "GREEN")
                old_signal = self.signal_state
                self.signal_state = signal
                logger.info(
                    f"[RELAY->SIGNAL] {old_signal} -> {signal} "
                    f"(relayed, hop {hop_count})"
                )
                # ACK back
                ack = make_ack(packet, self.jnc_id)
                self.tcp_sock.sendall(serialize(ack))
            elif ttl > 0:
                # Forward the relay back to server (server will deliver to final dest)
                fwd_pkt = create_packet(
                    MSG_RELAY, self.jnc_id, "SERVER", ttl=ttl,
                    payload={
                        **payload,
                        "hop_count": hop_count,
                    },
                )
                self.tcp_sock.sendall(serialize(fwd_pkt))
                logger.info(
                    f"[RELAY] Forwarded to SERVER for {final_dest} "
                    f"(hop {hop_count}, TTL {ttl})"
                )
            else:
                logger.warning(f"[RELAY] TTL expired for relay to {final_dest} - dropped")

    def _heartbeat_sender(self):
        """Send periodic UDP heartbeats to the server."""
        seq = 0
        while self.running:
            seq += 1
            hb = {
                "seq_no": seq,
                "type": MSG_HEARTBEAT,
                "sender": self.jnc_id,
                "receiver": "SERVER",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "payload": {"signal_state": self.signal_state},
            }
            try:
                self.udp_sock.sendto(
                    json.dumps(hb).encode("utf-8"),
                    (SERVER_HOST, UDP_PORT),
                )
            except OSError:
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    def disconnect(self):
        """Disconnect the junction."""
        self.running = False
        if self.tcp_sock:
            self.tcp_sock.close()
        if self.udp_sock:
            self.udp_sock.close()


# ── CLI ──────────────────────────────────────────────────────────────────────
def main():
    jnc_id = sys.argv[1] if len(sys.argv) > 1 else "J1"

    auth_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "auth_tokens.json")
    with open(auth_path) as f:
        tokens = json.load(f)["tokens"]
    token = tokens.get(jnc_id, "")

    client = JunctionClient(jnc_id, token)
    if not client.connect():
        sys.exit(1)

    print(f"\n{'='*50}")
    print(f"  Junction {jnc_id} -- Running")
    print(f"  Signal: {client.signal_state}")
    print(f"  Heartbeat every {HEARTBEAT_INTERVAL}s via UDP")
    print(f"{'='*50}")
    print("Press Ctrl+C to simulate junction failure\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nJunction {jnc_id} shutting down (simulated failure)")
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
