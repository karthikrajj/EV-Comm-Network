"""
EV-Comm Hospital Client
========================
TCP client representing a hospital node.
Receives HOSPITAL_NOTIFY alerts and sends HOSPITAL_ACK back through the chain.
Completes the request → route → notify → acknowledge cycle.
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
    MSG_AUTH, MSG_AUTH_RESPONSE, MSG_HOSPITAL_NOTIFY, MSG_HOSPITAL_ACK, MSG_ACK,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [HOSPITAL] %(message)s",
)
logger = logging.getLogger("Hospital")

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 9000


class HospitalClient:
    """Hospital TCP client node."""

    def __init__(self, hosp_id: str, token: str):
        self.hosp_id = hosp_id
        self.token = token
        self.sock: socket.socket = None
        self.running = False
        self.incoming_ambulances = []
        self.buffer = b""

    def connect(self):
        """Connect to the server and authenticate."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((SERVER_HOST, SERVER_PORT))
        self.running = True
        logger.info(f"🏥 Hospital {self.hosp_id} connected")

        # Authenticate
        auth_pkt = create_packet(
            MSG_AUTH, self.hosp_id, "SERVER",
            payload={"token": self.token, "client_type": "hospital"},
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

        # Start listener
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

    def _handle_packet(self, packet: dict):
        """Handle incoming packets."""
        if packet["type"] == MSG_HOSPITAL_NOTIFY:
            payload = packet["payload"]
            amb_id = payload["ambulance_id"]
            priority = payload["priority"]
            route = payload["route"]
            req_id = payload["req_id"]

            logger.info(f"🚨 INCOMING AMBULANCE: {amb_id}")
            logger.info(f"   Priority: {priority}")
            logger.info(f"   Route: {' → '.join(route)}")
            logger.info(f"   Request ID: {req_id}")

            self.incoming_ambulances.append({
                "ambulance_id": amb_id, "priority": priority,
                "route": route, "req_id": req_id, "time": time.time(),
            })

            # Send ACK for the notification
            ack = make_ack(packet, self.hosp_id)
            self.sock.sendall(serialize(ack))

            # Auto-send HOSPITAL_ACK (in a real system this would be manual)
            time.sleep(1)  # Simulate preparation time
            self._send_hospital_ack(amb_id, req_id)

    def _send_hospital_ack(self, ambulance_id: str, req_id: int):
        """Acknowledge ambulance arrival preparation."""
        pkt = create_packet(
            MSG_HOSPITAL_ACK, self.hosp_id, "SERVER",
            payload={
                "ambulance_id": ambulance_id,
                "req_id": req_id,
                "status": "READY",
                "message": f"Hospital {self.hosp_id} ready for {ambulance_id}",
            },
        )
        self.sock.sendall(serialize(pkt))
        logger.info(f"✅ Hospital ACK sent for {ambulance_id} (req #{req_id})")

    def disconnect(self):
        """Disconnect from the server."""
        self.running = False
        if self.sock:
            self.sock.close()


# ── CLI ──────────────────────────────────────────────────────────────────────
def main():
    hosp_id = sys.argv[1] if len(sys.argv) > 1 else "HOSPITAL_1"

    auth_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "auth_tokens.json")
    with open(auth_path) as f:
        tokens = json.load(f)["tokens"]
    token = tokens.get(hosp_id, "")

    client = HospitalClient(hosp_id, token)
    if not client.connect():
        sys.exit(1)

    print(f"\n{'='*50}")
    print(f"  🏥 Hospital {hosp_id} — Listening")
    print(f"{'='*50}")
    print("Waiting for ambulance notifications...\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
