"""
EV-Comm Central Network Server
===============================
The core TCP + UDP server that manages the entire EV-Comm network:
  - Accepts TCP connections from ambulances, junctions, and hospitals
  - Receives UDP heartbeats from junctions
  - Authenticates clients via tokens
  - Routes emergency requests using Dijkstra
  - Handles simulated packet loss, retransmission, congestion
  - Detects junction failures via heartbeat timeout
  - Pushes live events to the Flask-SocketIO dashboard
"""

import socket
import threading
import json
import time
import random
import heapq
import logging
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.protocol import (
    create_packet, validate_packet, serialize, deserialize_from_buffer,
    make_ack, make_nack,
    MSG_EMERGENCY_REQUEST, MSG_ACK, MSG_NACK, MSG_HEARTBEAT,
    MSG_ROUTE_UPDATE, MSG_SIGNAL_CHANGE, MSG_HOSPITAL_NOTIFY,
    MSG_HOSPITAL_ACK, MSG_AUTH, MSG_AUTH_RESPONSE, MSG_REGISTER,
    MSG_STATUS_UPDATE, MSG_CONGESTION_ALERT, MSG_NODE_DOWN, MSG_NODE_UP,
    MSG_RELAY,
    PRIORITY_CRITICAL, PRIORITY_URGENT, PRIORITY_STABLE,
)
from core.database import (
    init_db, upsert_ambulance, upsert_junction, update_junction_signal,
    create_request, update_request, log_packet, mark_junction_down,
    refresh_analytics, get_junctions, get_ambulances, get_requests,
    get_packet_log,
)
from core.routing import JunctionGraph

# ── Logging setup ────────────────────────────────────────────────────────────
import io
os.makedirs(os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs"), exist_ok=True)
_console_handler = logging.StreamHandler(
    stream=io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
)
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
_file_handler = logging.FileHandler(
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs", "server.log"),
    encoding="utf-8",
)
_file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.basicConfig(
    level=logging.INFO,
    handlers=[_file_handler, _console_handler],
)
logger = logging.getLogger("EV-Comm-Server")

# ── Configuration ────────────────────────────────────────────────────────────
TCP_HOST = "127.0.0.1"
TCP_PORT = 9000
UDP_PORT = 9001
HEARTBEAT_TIMEOUT = 8.0       # seconds before a junction is considered dead
HEARTBEAT_CHECK_INTERVAL = 3.0
PACKET_DROP_RATE = 0.1        # 10% simulated packet loss
CONGESTION_THRESHOLD = 2      # Number of concurrent ambulances to trigger congestion
RETRANSMIT_TIMEOUT = 2.0      # Seconds before retransmitting
RELAY_PROBABILITY = 0.4       # 40% chance of multi-hop relay for signal changes

# ── Shared state (a real notification bus would be better, but fine for a demo) ─
_socketio_instance = None   # Will be set by the dashboard when it starts

def set_socketio(sio):
    global _socketio_instance
    _socketio_instance = sio

def emit_event(event: str, data: dict):
    """Emit a SocketIO event if the dashboard bridge is running."""
    if _socketio_instance:
        _socketio_instance.emit(event, data, namespace="/")


class NetworkServer:
    """Central server managing TCP connections and UDP heartbeats."""

    def __init__(self):
        # Initialize database
        init_db()

        # Load topology graph
        self.graph = JunctionGraph()
        logger.info(f"Loaded topology: {list(self.graph.adjacency.keys())}")

        # Load auth tokens
        auth_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "config", "auth_tokens.json"
        )
        with open(auth_path) as f:
            self.auth_tokens = json.load(f)["tokens"]

        # Connected client tracking
        self.clients: dict[str, dict] = {}      # client_id → {socket, type, authenticated}
        self.client_lock = threading.Lock()

        # Heartbeat tracking
        self.last_heartbeat: dict[str, float] = {}  # junction_id → timestamp

        # Priority queue for concurrent ambulance requests (heap)
        self.request_queue: list[tuple] = []     # heap of (priority_value, timestamp, request_data)
        self.queue_lock = threading.Lock()
        self._queue_counter = 0  # tiebreaker for heap

        # Active routes tracking
        self.active_routes: dict[str, dict] = {}  # ambulance_id → route info

        # Pending ACKs (for retransmission)
        self.pending_acks: dict[int, dict] = {}   # seq_no → {packet, target, send_time, retries}

        # Ambulance position tracking (for live 2D map)
        self.ambulance_positions: dict[str, dict] = {}  # amb_id → {current_node_idx, path, started_at}

        # Multi-hop relay counter
        self.relay_count = 0
        
        self.dashboard_congestion = 0
        self.degraded_nodes = set()
        self.processed_requests = set()

        self.running = False

    # ── Priority mapping ─────────────────────────────────────────────────────

    @staticmethod
    def _priority_value(priority: str) -> int:
        """Lower value = higher priority."""
        return {PRIORITY_CRITICAL: 0, PRIORITY_URGENT: 1, PRIORITY_STABLE: 2}.get(priority, 3)

    # ── TCP Connection Handling ──────────────────────────────────────────────

    def _handle_tcp_client(self, conn: socket.socket, addr: tuple):
        """Handle an incoming TCP client connection."""
        buffer = b""
        client_id = None
        logger.info(f"New TCP connection from {addr}")

        try:
            while self.running:
                data = conn.recv(4096)
                if not data:
                    break
                buffer += data

                while True:
                    packet, buffer = deserialize_from_buffer(buffer)
                    if packet is None:
                        break

                    # Simulate packet loss & graceful degradation
                    sender = packet["sender"]
                    drop_prob = PACKET_DROP_RATE
                    if sender in self.degraded_nodes:
                        drop_prob = 0.5
                    
                    if packet["type"] not in (MSG_AUTH,) and random.random() < drop_prob:
                        logger.warning(f"📦 SIMULATED DROP: packet #{packet['seq_no']} from {packet['sender']}")
                        log_packet(
                            packet["seq_no"], packet["type"],
                            packet["sender"], packet["receiver"],
                            dropped=True,
                        )
                        emit_event("packet_dropped", {
                            "seq_no": packet["seq_no"],
                            "type": packet["type"],
                            "sender": packet["sender"],
                        })
                        continue

                    # Validate checksum
                    if not validate_packet(packet):
                        logger.warning(f"Invalid checksum on packet #{packet['seq_no']}")
                        nack = make_nack(packet, "SERVER", "CHECKSUM_FAIL")
                        conn.sendall(serialize(nack))
                        continue

                    # Record receive time for latency
                    recv_time = time.time()

                    # ─── AUTH ──────────────────────────────────────────
                    if packet["type"] == MSG_AUTH:
                        client_id = packet["sender"]
                        token = packet["payload"].get("token", "")
                        client_type = packet["payload"].get("client_type", "unknown")

                        if self.auth_tokens.get(client_id) == token:
                            with self.client_lock:
                                self.clients[client_id] = {
                                    "socket": conn,
                                    "type": client_type,
                                    "authenticated": True,
                                    "addr": addr,
                                }
                            logger.info(f"✅ Authenticated: {client_id} ({client_type})")
                            response = create_packet(
                                MSG_AUTH_RESPONSE, "SERVER", client_id,
                                payload={"status": "OK", "message": f"Welcome {client_id}"},
                            )
                            conn.sendall(serialize(response))

                            # Register node in DB
                            if client_type == "ambulance":
                                upsert_ambulance(client_id, "CONNECTED")
                            elif client_type == "junction":
                                upsert_junction(client_id, "UP")
                                self.last_heartbeat[client_id] = time.time()

                            emit_event("client_connected", {
                                "id": client_id, "type": client_type
                            })
                        else:
                            logger.warning(f"❌ Auth failed for {client_id}")
                            response = create_packet(
                                MSG_AUTH_RESPONSE, "SERVER", client_id,
                                payload={"status": "DENIED", "message": "Invalid token"},
                            )
                            conn.sendall(serialize(response))
                            break

                    # ─── EMERGENCY REQUEST ────────────────────────────
                    elif packet["type"] == MSG_EMERGENCY_REQUEST:
                        req_key = (packet["sender"], packet["seq_no"])
                        if req_key in self.processed_requests:
                            # Idempotency: Duplicate request, just re-ACK
                            logger.info(f"Duplicate request {req_key}, re-ACKing.")
                            ack = make_ack(packet, "SERVER")
                            conn.sendall(serialize(ack))
                        else:
                            self.processed_requests.add(req_key)
                            self._enqueue_emergency(packet, conn, recv_time)

                    # ─── RELAY (multi-hop forwarded packet) ───────────
                    elif packet["type"] == MSG_RELAY:
                        self._handle_relay_arrival(packet)

                    # ─── HOSPITAL ACK ─────────────────────────────────
                    elif packet["type"] == MSG_HOSPITAL_ACK:
                        self._handle_hospital_ack(packet)

                    # ─── ACK (retransmission resolution) ─────────────
                    elif packet["type"] == MSG_ACK:
                        ack_seq = packet["payload"].get("ack_seq")
                        if ack_seq in self.pending_acks:
                            send_time = self.pending_acks[ack_seq]["send_time"]
                            latency = (recv_time - send_time) * 1000
                            del self.pending_acks[ack_seq]
                            log_packet(
                                packet["seq_no"], MSG_ACK,
                                packet["sender"], "SERVER",
                                latency_ms=latency,
                            )
                            logger.info(f"✓ ACK received for #{ack_seq} (RTT: {latency:.1f}ms)")
                            emit_event("packet_ack", {
                                "seq_no": ack_seq, "rtt_ms": round(latency, 1)
                            })

                    # Log all received packets
                    log_packet(
                        packet["seq_no"], packet["type"],
                        packet["sender"], packet["receiver"],
                    )
                    emit_event("packet_received", packet)

        except (ConnectionResetError, ConnectionAbortedError, OSError) as e:
            logger.info(f"Connection lost: {client_id or addr} — {e}")
        finally:
            conn.close()
            if client_id:
                with self.client_lock:
                    self.clients.pop(client_id, None)
                logger.info(f"🔌 Disconnected: {client_id}")
                emit_event("client_disconnected", {"id": client_id})

    # ── Priority Queue (Feature #12) ──────────────────────────────────────────

    def inject_dashboard_request(self, origin: str, priority: str):
        """Inject a virtual emergency request from the dashboard UI."""
        import uuid
        amb_id = f"A_{str(uuid.uuid4())[:4].upper()}"
        from core.protocol import create_packet, MSG_EMERGENCY_REQUEST
        packet = create_packet(
            MSG_EMERGENCY_REQUEST, amb_id, "SERVER",
            payload={"location": origin, "priority": priority}
        )
        pval = self._priority_value(priority)
        with self.queue_lock:
            self._queue_counter += 1
            heapq.heappush(
                self.request_queue,
                (pval, self._queue_counter, time.time(), packet, None)
            )
            
        emit_event("queue_status", {
            "queue_length": len(self.request_queue),
            "priority": priority,
        })
        logger.info(f"[DASHBOARD] Injected emergency request for {amb_id} at {origin}")

    def _enqueue_emergency(self, packet: dict, conn: socket.socket, recv_time: float):
        """Add emergency to priority queue. CRITICAL > URGENT > STABLE, then FCFS."""
        amb_id = packet["sender"]
        priority = packet["payload"].get("priority", PRIORITY_STABLE)
        pval = self._priority_value(priority)

        # ACK immediately so ambulance knows we received it
        ack = make_ack(packet, "SERVER")
        conn.sendall(serialize(ack))
        log_packet(ack["seq_no"], MSG_ACK, "SERVER", amb_id)

        with self.queue_lock:
            self._queue_counter += 1
            heapq.heappush(
                self.request_queue,
                (pval, self._queue_counter, recv_time, packet, conn),
            )
        logger.info(f"[QUEUE] {amb_id} ({priority}) enqueued — queue depth: {len(self.request_queue)}")
        emit_event("queue_update", {
            "ambulance": amb_id, "priority": priority,
            "queue_depth": len(self.request_queue),
        })

    def _queue_processor(self):
        """Continuously process the priority queue — highest priority first, FCFS for ties."""
        while self.running:
            item = None
            with self.queue_lock:
                if self.request_queue:
                    item = heapq.heappop(self.request_queue)
            if item:
                pval, counter, recv_time, packet, conn = item
                self._handle_emergency(packet, conn, recv_time)
            else:
                time.sleep(0.2)

    # ── Emergency request handling ───────────────────────────────────────────

    def _handle_emergency(self, packet: dict, conn: socket.socket, recv_time: float):
        """Process an EMERGENCY_REQUEST: route, signal, notify."""
        amb_id = packet["sender"]
        payload = packet["payload"]
        priority = payload.get("priority", PRIORITY_STABLE)
        origin = payload.get("location", "J1")
        destination = payload.get("destination", "HOSPITAL_1")

        logger.info(f"[EMERGENCY] {amb_id}: {priority} at {origin} -> {destination}")

        # Create request in DB
        req_id = create_request(amb_id, priority, origin, destination)
        upsert_ambulance(amb_id, "ACTIVE", origin)

        # Check congestion (multiple active ambulances)
        active_count = len(self.active_routes)
        if active_count >= CONGESTION_THRESHOLD:
            logger.warning(f"[CONGESTION] {active_count + 1} active ambulances")
            for active_amb, route_info in self.active_routes.items():
                route_path = route_info.get("path", [])
                for i in range(len(route_path) - 1):
                    self.graph.set_congestion(route_path[i], route_path[i + 1], 2.5)
            emit_event("congestion_alert", {"active_count": active_count + 1})

        # Run Dijkstra
        result = self.graph.dijkstra(origin, destination)
        if result is None:
            logger.error(f"No route found from {origin} to {destination}!")
            if conn:
                nack = make_nack(packet, "SERVER", "NO_ROUTE")
                conn.sendall(serialize(nack))
            update_request(req_id, status="FAILED")
            return

        path, cost = result
        route_str = " -> ".join(path)
        logger.info(f"[ROUTE] {route_str} (cost: {cost})")

        # Store active route
        self.active_routes[amb_id] = {
            "path": path, "cost": cost, "req_id": req_id
        }

        # Start ambulance position tracking (for live 2D map — Feature #17)
        self.ambulance_positions[amb_id] = {
            "path": path,
            "current_idx": 0,
            "started_at": time.time(),
            "step_duration": max(cost / len(path), 1.5),  # seconds per hop
        }

        # Update DB
        update_request(req_id, route=route_str, status="ROUTED")

        # Send ROUTE_UPDATE to ambulance
        route_packet = create_packet(
            MSG_ROUTE_UPDATE, "SERVER", amb_id,
            payload={"route": path, "cost": cost, "req_id": req_id},
        )
        if conn:
            conn.sendall(serialize(route_packet))
            send_time = time.time()
            self.pending_acks[route_packet["seq_no"]] = {
                "packet": route_packet, "target_conn": conn,
                "send_time": send_time, "retries": 0,
            }
        log_packet(route_packet["seq_no"], MSG_ROUTE_UPDATE, "SERVER", amb_id)

        # Send SIGNAL_CHANGE to junctions along the route (with multi-hop relay)
        for junction_id in path:
            if junction_id.startswith("J"):
                self._send_signal_change(junction_id, "GREEN", amb_id, path)

        # Notify hospital (TCP)
        self._notify_hospital(destination, amb_id, priority, path, req_id)

        emit_event("emergency_routed", {
            "ambulance": amb_id, "route": path, "cost": cost,
            "priority": priority, "req_id": req_id,
        })

    def _send_signal_change(self, junction_id: str, signal: str, ambulance_id: str, route_path: list = None):
        """Send a SIGNAL_CHANGE — directly or via multi-hop relay (Feature #9)."""
        # Decide whether to relay through a neighbor
        relay_via = None
        if route_path and random.random() < RELAY_PROBABILITY:
            # Find a neighbor of junction_id that is also on the route and connected
            for neighbor_id in route_path:
                if (neighbor_id != junction_id and neighbor_id.startswith("J")
                        and neighbor_id not in self.graph.down_nodes):
                    with self.client_lock:
                        nc = self.clients.get(neighbor_id)
                    if nc and nc["authenticated"]:
                        relay_via = neighbor_id
                        break

        if relay_via:
            # Multi-hop: Server → relay_junction → target_junction
            self.relay_count += 1
            relay_pkt = create_packet(
                MSG_RELAY, "SERVER", relay_via, ttl=3,
                payload={
                    "inner_type": MSG_SIGNAL_CHANGE,
                    "final_dest": junction_id,
                    "signal": signal,
                    "for_ambulance": ambulance_id,
                    "hop_count": 1,
                },
            )
            with self.client_lock:
                rc = self.clients.get(relay_via)
            if rc:
                try:
                    rc["socket"].sendall(serialize(relay_pkt))
                    log_packet(relay_pkt["seq_no"], MSG_RELAY, "SERVER", relay_via)
                    logger.info(
                        f"[RELAY] Signal for {junction_id} relayed via {relay_via} "
                        f"(hop 1, TTL 3)"
                    )
                    emit_event("multi_hop_relay", {
                        "from": "SERVER", "via": relay_via,
                        "to": junction_id, "ttl": 3, "hop": 1,
                    })
                    update_junction_signal(junction_id, signal)
                    return
                except OSError:
                    pass  # Fall through to direct send

        # Direct send (no relay)
        with self.client_lock:
            client = self.clients.get(junction_id)
        if client and client["authenticated"]:
            pkt = create_packet(
                MSG_SIGNAL_CHANGE, "SERVER", junction_id,
                payload={"signal": signal, "for_ambulance": ambulance_id},
            )
            try:
                client["socket"].sendall(serialize(pkt))
                send_time = time.time()
                self.pending_acks[pkt["seq_no"]] = {
                    "packet": pkt, "target_conn": client["socket"],
                    "send_time": send_time, "retries": 0,
                }
                log_packet(pkt["seq_no"], MSG_SIGNAL_CHANGE, "SERVER", junction_id)
                update_junction_signal(junction_id, signal)
                logger.info(f"[SIGNAL] {signal} at {junction_id} for {ambulance_id} (direct)")
                emit_event("signal_change", {
                    "junction": junction_id, "signal": signal,
                    "ambulance": ambulance_id,
                })
            except OSError:
                logger.warning(f"Failed to reach junction {junction_id}")

    def _handle_relay_arrival(self, packet: dict):
        """Handle a RELAY packet that arrived back at server (forwarded by junction)."""
        payload = packet["payload"]
        final_dest = payload.get("final_dest", "")
        logger.info(
            f"[RELAY] Relay for {final_dest} arrived via {packet['sender']} "
            f"(hop {payload.get('hop_count', '?')})"
        )
        # The junction already applied the signal change, so just log it
        log_packet(packet["seq_no"], MSG_RELAY, packet["sender"], "SERVER")

    def _notify_hospital(self, hospital_id: str, ambulance_id: str,
                         priority: str, route: list, req_id: int):
        """Notify a hospital about an incoming ambulance."""
        with self.client_lock:
            client = self.clients.get(hospital_id)
        if client and client["authenticated"]:
            pkt = create_packet(
                MSG_HOSPITAL_NOTIFY, "SERVER", hospital_id,
                payload={
                    "ambulance_id": ambulance_id,
                    "priority": priority,
                    "route": route,
                    "req_id": req_id,
                },
            )
            try:
                client["socket"].sendall(serialize(pkt))
                send_time = time.time()
                self.pending_acks[pkt["seq_no"]] = {
                    "packet": pkt, "target_conn": client["socket"],
                    "send_time": send_time, "retries": 0,
                }
                log_packet(pkt["seq_no"], MSG_HOSPITAL_NOTIFY, "SERVER", hospital_id)
                logger.info(f"🏥 Hospital {hospital_id} notified about {ambulance_id}")
                emit_event("hospital_notified", {
                    "hospital": hospital_id, "ambulance": ambulance_id,
                })
            except OSError:
                logger.warning(f"Failed to reach hospital {hospital_id}")
        else:
            logger.warning(f"Hospital {hospital_id} not connected — cannot notify")

    def _handle_hospital_ack(self, packet: dict):
        """Handle hospital acknowledgment — completes the chain."""
        payload = packet["payload"]
        ambulance_id = payload.get("ambulance_id", "")
        req_id = payload.get("req_id", 0)

        logger.info(f"🏥✅ Hospital ACK for {ambulance_id} (req #{req_id})")
        update_request(req_id, status="COMPLETED")

        # Reset signals on the route
        route_info = self.active_routes.pop(ambulance_id, None)
        if route_info:
            for jnc in route_info["path"]:
                if jnc.startswith("J"):
                    self._send_signal_change(jnc, "RED", ambulance_id)
            # Clear any congestion set for this route
            path = route_info["path"]
            for i in range(len(path) - 1):
                self.graph.clear_congestion(path[i], path[i + 1])

        upsert_ambulance(ambulance_id, "IDLE")
        emit_event("request_completed", {
            "ambulance": ambulance_id, "req_id": req_id
        })

    # ── UDP Heartbeat Listener ───────────────────────────────────────────────

    def _udp_listener(self):
        """Listen for UDP heartbeats from junction nodes."""
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.bind((TCP_HOST, UDP_PORT))
        udp_sock.settimeout(1.0)
        logger.info(f"UDP heartbeat listener on {TCP_HOST}:{UDP_PORT}")

        while self.running:
            try:
                data, addr = udp_sock.recvfrom(1024)
                packet = json.loads(data.decode("utf-8"))
                if packet.get("type") == MSG_HEARTBEAT:
                    jnc_id = packet["sender"]
                    self.last_heartbeat[jnc_id] = time.time()
                    upsert_junction(jnc_id, "UP")
                    log_packet(
                        packet.get("seq_no", 0), MSG_HEARTBEAT,
                        jnc_id, "SERVER",
                    )
                    emit_event("heartbeat", {"junction": jnc_id})
            except socket.timeout:
                continue
            except Exception as e:
                logger.error(f"UDP error: {e}")

        udp_sock.close()

    # ── Heartbeat Timeout Checker ────────────────────────────────────────────

    def _heartbeat_checker(self):
        """Periodically check for missed heartbeats → mark junctions DOWN."""
        while self.running:
            time.sleep(HEARTBEAT_CHECK_INTERVAL)
            now = time.time()
            for jnc_id, last_time in list(self.last_heartbeat.items()):
                if now - last_time > HEARTBEAT_TIMEOUT:
                    if jnc_id not in self.graph.down_nodes:
                        logger.warning(f"💀 Junction {jnc_id} HEARTBEAT TIMEOUT — marking DOWN")
                        self.graph.mark_down(jnc_id)
                        mark_junction_down(jnc_id)
                        emit_event("node_down", {"junction": jnc_id})

                        # Reroute any active routes going through this junction
                        self._reroute_affected(jnc_id)
                elif jnc_id in self.graph.down_nodes:
                    # Junction came back!
                    logger.info(f"✅ Junction {jnc_id} is back UP")
                    self.graph.mark_up(jnc_id)
                    upsert_junction(jnc_id, "UP")
                    emit_event("node_up", {"junction": jnc_id})

    def _reroute_affected(self, failed_junction: str):
        """Reroute all active routes that pass through a failed junction."""
        for amb_id, route_info in list(self.active_routes.items()):
            if failed_junction in route_info["path"]:
                logger.info(f"🔄 Rerouting {amb_id} (was passing through {failed_junction})")
                origin = route_info["path"][0]
                destination = route_info["path"][-1]
                result = self.graph.dijkstra(origin, destination)
                if result:
                    new_path, new_cost = result
                    self.active_routes[amb_id] = {
                        "path": new_path, "cost": new_cost,
                        "req_id": route_info["req_id"],
                    }
                    update_request(route_info["req_id"], route=" → ".join(new_path))

                    # Send updated route to ambulance
                    with self.client_lock:
                        client = self.clients.get(amb_id)
                    if client:
                        pkt = create_packet(
                            MSG_ROUTE_UPDATE, "SERVER", amb_id,
                            payload={
                                "route": new_path, "cost": new_cost,
                                "req_id": route_info["req_id"],
                                "reason": f"Junction {failed_junction} DOWN",
                            },
                        )
                        try:
                            client["socket"].sendall(serialize(pkt))
                            log_packet(pkt["seq_no"], MSG_ROUTE_UPDATE, "SERVER", amb_id)
                        except OSError:
                            pass

                    # Update signals
                    for jnc in new_path:
                        if jnc.startswith("J") and jnc != failed_junction:
                            self._send_signal_change(jnc, "GREEN", amb_id)

                    logger.info(f"📍 New route for {amb_id}: {' → '.join(new_path)} (cost: {new_cost})")
                    emit_event("route_recalculated", {
                        "ambulance": amb_id, "new_route": new_path,
                        "new_cost": new_cost, "reason": f"{failed_junction} DOWN",
                    })
                else:
                    logger.error(f"No alternative route for {amb_id}!")

    # ── Retransmission Checker ───────────────────────────────────────────────

    def _retransmission_checker(self):
        """Check for pending ACKs and retransmit if timed out."""
        while self.running:
            time.sleep(1.0)
            now = time.time()
            for seq_no, info in list(self.pending_acks.items()):
                if now - info["send_time"] > RETRANSMIT_TIMEOUT:
                    if info["retries"] < 3:
                        info["retries"] += 1
                        info["send_time"] = now
                        logger.warning(
                            f"🔁 Retransmitting packet #{seq_no} "
                            f"(attempt {info['retries']}/3)"
                        )
                        try:
                            info["target_conn"].sendall(serialize(info["packet"]))
                            log_packet(
                                seq_no, info["packet"]["type"],
                                "SERVER", info["packet"]["receiver"],
                                retransmitted=True,
                            )
                            emit_event("packet_retransmit", {
                                "seq_no": seq_no, "attempt": info["retries"],
                            })
                        except OSError:
                            del self.pending_acks[seq_no]
                    else:
                        logger.error(f"❌ Gave up on packet #{seq_no} after 3 retries")
                        del self.pending_acks[seq_no]

    # ── API methods (called by the Flask dashboard) ──────────────────────────

    def _compute_ambulance_map_positions(self) -> dict:
        """Compute current ambulance positions for the live 2D map (Feature #17)."""
        positions = {}
        now = time.time()
        for amb_id, pos_info in list(self.ambulance_positions.items()):
            elapsed = now - pos_info["started_at"]
            step_dur = pos_info["step_duration"]
            path = pos_info["path"]
            idx = min(int(elapsed / step_dur), len(path) - 1)
            progress = min((elapsed % step_dur) / step_dur, 1.0) if idx < len(path) - 1 else 1.0
            positions[amb_id] = {
                "path": path,
                "current_idx": idx,
                "progress": round(progress, 2),
                "current_node": path[idx],
                "next_node": path[idx + 1] if idx < len(path) - 1 else path[idx],
                "arrived": idx >= len(path) - 1 and progress >= 1.0,
            }
        return positions

    def get_state(self) -> dict:
        """Return full server state for dashboard."""
        return {
            "clients": {
                cid: {"type": info["type"], "authenticated": info["authenticated"]}
                for cid, info in self.clients.items()
            },
            "topology": self.graph.get_topology_data(),
            "active_routes": {
                amb_id: {"path": info["path"], "cost": info["cost"]}
                for amb_id, info in self.active_routes.items()
            },
            "down_nodes": list(self.graph.down_nodes),
            "ambulances": get_ambulances(),
            "junctions": get_junctions(),
            "requests": get_requests(),
            "packet_log": get_packet_log(50),
            "analytics": refresh_analytics(),
            "ambulance_map": self._compute_ambulance_map_positions(),
            "relay_count": self.relay_count,
            "queue_depth": len(self.request_queue),
            "dashboard_congestion": self.dashboard_congestion,
            "degraded_nodes": list(self.degraded_nodes),
        }

    # ── Dashboard Controls ───────────────────────────────────────────────────

    def toggle_node_from_dashboard(self, node_id: str):
        if node_id in self.graph.down_nodes:
            self.graph.mark_up(node_id)
            from core.database import upsert_junction
            upsert_junction(node_id, "UP")
            emit_event("node_up", {"junction": node_id})
            logger.info(f"[DASHBOARD] Toggled {node_id} UP")
        else:
            self.graph.mark_down(node_id)
            from core.database import mark_junction_down
            mark_junction_down(node_id)
            emit_event("node_down", {"junction": node_id})
            logger.info(f"[DASHBOARD] Toggled {node_id} DOWN")
            self._reroute_affected(node_id)

    def set_congestion_from_dashboard(self, congestion_level: int):
        try:
            val = int(congestion_level)
            self.dashboard_congestion = val
            if val > 70:
                self.graph.set_congestion("J2", "J3", 3.0)
            else:
                self.graph.clear_congestion("J2", "J3")
            logger.info(f"[DASHBOARD] Congestion set to {val}%")
        except ValueError:
            pass

    def reset_network_from_dashboard(self):
        for node in list(self.graph.down_nodes):
            self.graph.mark_up(node)
            from core.database import upsert_junction
            upsert_junction(node, "UP")
        self.graph.clear_congestion("J2", "J3")
        self.dashboard_congestion = 0
        self.degraded_nodes.clear()
        self.active_routes.clear()
        self.ambulance_positions.clear()
        self.request_queue.clear()
        self.processed_requests.clear()
        logger.info("[DASHBOARD] Network reset")
        
    def chaos_inject(self, action: str, target: str):
        if action == "kill_node" and target:
            if target not in self.graph.down_nodes:
                self.toggle_node_from_dashboard(target)
        elif action == "degrade_node" and target:
            if target not in self.degraded_nodes:
                self.degraded_nodes.add(target)
                emit_event("node_degraded", {"junction": target})
                logger.warning(f"CHAOS: {target} degraded to 50% packet loss.")
        elif action == "flood_requests":
            for _ in range(5):
                origin = random.choice(["J1", "J2", "J3", "J4"])
                self.inject_dashboard_request(origin, "STABLE")

    # ── Start / Stop ─────────────────────────────────────────────────────────

    def start(self):
        """Start all server threads."""
        self.running = True

        # TCP listener
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_sock.bind((TCP_HOST, TCP_PORT))
        tcp_sock.listen(10)
        tcp_sock.settimeout(1.0)
        logger.info(f"TCP server listening on {TCP_HOST}:{TCP_PORT}")

        def accept_loop():
            while self.running:
                try:
                    conn, addr = tcp_sock.accept()
                    t = threading.Thread(
                        target=self._handle_tcp_client, args=(conn, addr), daemon=True
                    )
                    t.start()
                except socket.timeout:
                    continue
            tcp_sock.close()

        threading.Thread(target=accept_loop, daemon=True).start()
        threading.Thread(target=self._udp_listener, daemon=True).start()
        threading.Thread(target=self._heartbeat_checker, daemon=True).start()
        threading.Thread(target=self._retransmission_checker, daemon=True).start()
        threading.Thread(target=self._queue_processor, daemon=True).start()

        logger.info("=" * 47)
        logger.info("   EV-Comm Network Server -- RUNNING")
        logger.info("=" * 47)

    def stop(self):
        """Stop the server."""
        self.running = False
        logger.info("Server shutting down...")


# ── Standalone runner ────────────────────────────────────────────────────────
if __name__ == "__main__":
    server = NetworkServer()
    server.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop()
