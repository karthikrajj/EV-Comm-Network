"""
EV-Comm Ambulance Client
=========================
TCP client that connects to the central server, authenticates,
sends EMERGENCY_REQUEST packets, and receives route updates.
Demonstrates: TCP reliable delivery, ACK/NACK, retransmission.
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
    MSG_AUTH, MSG_AUTH_RESPONSE, MSG_EMERGENCY_REQUEST, MSG_ACK, MSG_NACK,
    MSG_ROUTE_UPDATE,
    PRIORITY_CRITICAL, PRIORITY_URGENT, PRIORITY_STABLE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [AMBULANCE] %(message)s",
)
logger = logging.getLogger("Ambulance")

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 9000


class AmbulanceClient:
    """Ambulance TCP client node."""

    def __init__(self, amb_id: str, token: str):
        self.amb_id = amb_id
        self.token = token
        self.sock: socket.socket = None
        self.running = False
        self.current_route = None
        self.buffer = b""

    def connect(self):
        """Connect to the server and authenticate."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((SERVER_HOST, SERVER_PORT))
        self.running = True
        logger.info(f"🚑 {self.amb_id} connected to server")

        # Authenticate
        auth_pkt = create_packet(
            MSG_AUTH, self.amb_id, "SERVER",
            payload={"token": self.token, "client_type": "ambulance"},
        )
        self.sock.sendall(serialize(auth_pkt))

        # Wait for auth response
        response = self._recv_packet()
        if response and response["type"] == MSG_AUTH_RESPONSE:
            if response["payload"]["status"] == "OK":
                logger.info(f"✅ Authenticated: {response['payload']['message']}")
            else:
                logger.error(f"❌ Auth failed: {response['payload']['message']}")
                self.disconnect()
                return False

        # Start listener thread
        threading.Thread(target=self._listen, daemon=True).start()
        return True

    def _recv_packet(self) -> dict:
        """Receive a single packet (blocking)."""
        while True:
            packet, self.buffer = deserialize_from_buffer(self.buffer)
            if packet:
                return packet
            data = self.sock.recv(4096)
            if not data:
                return None
            self.buffer += data

    def _listen(self):
        """Listen for incoming packets from the server."""
        while self.running:
            try:
                data = self.sock.recv(4096)
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
        logger.info(f"🔌 {self.amb_id} disconnected")

    def _handle_packet(self, packet: dict):
        """Handle incoming packets."""
        if packet["type"] == MSG_ROUTE_UPDATE:
            route = packet["payload"]["route"]
            cost = packet["payload"]["cost"]
            reason = packet["payload"].get("reason", "")
            prefix = "🔄 REROUTE" if reason else "📍 Route assigned"
            logger.info(f"{prefix}: {' → '.join(route)} (cost: {cost})")
            if reason:
                logger.info(f"   Reason: {reason}")
            self.current_route = route

            # Send ACK
            ack = make_ack(packet, self.amb_id)
            self.sock.sendall(serialize(ack))

        elif packet["type"] == MSG_ACK:
            ack_seq = packet["payload"].get("ack_seq")
            logger.info(f"✓ Server ACK for packet #{ack_seq}")

        elif packet["type"] == MSG_NACK:
            reason = packet["payload"].get("reason", "")
            logger.warning(f"✗ Server NACK: {reason}")

    def send_emergency(self, priority: str = PRIORITY_CRITICAL,
                       location: str = "J1", destination: str = "HOSPITAL_1"):
        """Send an emergency request to the server."""
        pkt = create_packet(
            MSG_EMERGENCY_REQUEST, self.amb_id, "SERVER",
            payload={
                "priority": priority,
                "location": location,
                "destination": destination,
            },
        )
        self.sock.sendall(serialize(pkt))
        logger.info(f"🚨 Emergency sent: {priority} at {location} → {destination}")

    def disconnect(self):
        """Disconnect from the server."""
        self.running = False
        if self.sock:
            self.sock.close()


# ── Interactive CLI ──────────────────────────────────────────────────────────
def main():
    amb_id = sys.argv[1] if len(sys.argv) > 1 else "A01"

    # Load token
    auth_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "auth_tokens.json")
    with open(auth_path) as f:
        tokens = json.load(f)["tokens"]
    token = tokens.get(amb_id, "")

    client = AmbulanceClient(amb_id, token)
    if not client.connect():
        sys.exit(1)

    print(f"\n{'='*50}")
    print(f"  🚑 Ambulance {amb_id} — Interactive Console")
    print(f"{'='*50}")
    print("Commands:")
    print("  e [priority] [location] [dest] — Send emergency")
    print("    priority: CRITICAL, URGENT, STABLE")
    print("    location: J1, J2, J3, J4, J5")
    print("    dest:     HOSPITAL_1")
    print("  r — Show current route")
    print("  q — Quit")
    print()

    try:
        while True:
            cmd = input(f"[{amb_id}] > ").strip().split()
            if not cmd:
                continue
            if cmd[0] == "q":
                break
            elif cmd[0] == "e":
                priority = cmd[1] if len(cmd) > 1 else PRIORITY_CRITICAL
                location = cmd[2] if len(cmd) > 2 else "J1"
                dest = cmd[3] if len(cmd) > 3 else "HOSPITAL_1"
                client.send_emergency(priority, location, dest)
            elif cmd[0] == "r":
                if client.current_route:
                    print(f"  Route: {' → '.join(client.current_route)}")
                else:
                    print("  No route assigned yet.")
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
