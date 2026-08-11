"""
EV-Comm Dashboard Server
=========================
Flask-SocketIO web application providing a real-time browser dashboard.
Bridges the raw TCP/UDP socket network to WebSocket-connected browsers.
"""

import os
import sys
import json
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from flask import Flask, render_template, jsonify, request, session, redirect, url_for
from flask_socketio import SocketIO
from flask_cors import CORS

from server.network_server import NetworkServer, set_socketio
from core.database import init_db, refresh_analytics, get_packet_log, get_requests

# ── Flask App ────────────────────────────────────────────────────────────────
app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
    static_folder=os.path.join(os.path.dirname(__file__), "static"),
)
app.config["SECRET_KEY"] = "evcomm-secret-key"
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# Create the network server
network_server = NetworkServer()

# Bridge SocketIO to the network server
set_socketio(socketio)


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    return render_template("dashboard.html")

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        if request.form.get("password") == "admin123":
            session["logged_in"] = True
            return redirect(url_for("index"))
        else:
            error = "Invalid access code."
    return render_template("login.html", error=error)

@app.route("/logout")
def logout():
    session.pop("logged_in", None)
    return redirect(url_for("login"))

@app.route("/health")
def health():
    return jsonify({"status": "ok", "time": time.time()})

@app.route("/api/state")
def api_state():
    return jsonify(network_server.get_state())


@app.route("/api/analytics")
def api_analytics():
    return jsonify(refresh_analytics())


@app.route("/api/packets")
def api_packets():
    return jsonify(get_packet_log(200))


@app.route("/api/requests")
def api_requests():
    return jsonify(get_requests())


# ── SocketIO Events ─────────────────────────────────────────────────────────

@socketio.on("connect")
def handle_connect():
    print("[Dashboard] Client connected")
    # Send initial state
    socketio.emit("initial_state", network_server.get_state())


@socketio.on("request_state")
def handle_request_state():
    socketio.emit("state_update", network_server.get_state())

@socketio.on("client_dispatch")
def handle_client_dispatch(data):
    origin = data.get("origin", "J1")
    priority = data.get("priority", "STABLE")
    network_server.inject_dashboard_request(origin, priority)
    # Immediately broadcast state update
    socketio.emit("state_update", network_server.get_state())

@socketio.on("toggle_node")
def handle_toggle_node(data):
    node_id = data.get("id")
    if node_id:
        network_server.toggle_node_from_dashboard(node_id)
        socketio.emit("state_update", network_server.get_state())

@socketio.on("set_congestion")
def handle_set_congestion(data):
    val = data.get("value", 0)
    network_server.set_congestion_from_dashboard(val)
    socketio.emit("state_update", network_server.get_state())

@socketio.on("reset_network")
def handle_reset_network():
    network_server.reset_network_from_dashboard()
    socketio.emit("state_update", network_server.get_state())

@socketio.on("chaos_inject")
def handle_chaos_inject(data):
    action = data.get("action")
    target = data.get("target")
    network_server.chaos_inject(action, target)
    socketio.emit("state_update", network_server.get_state())

# ── Periodic state push ─────────────────────────────────────────────────────

def periodic_state_push():
    """Push state updates to all dashboard clients every 2 seconds."""
    while True:
        time.sleep(2)
        try:
            socketio.emit("state_update", network_server.get_state())
        except Exception:
            pass


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    init_db()

    # Start the network server (TCP/UDP)
    network_server.start()

    # Start periodic push
    threading.Thread(target=periodic_state_push, daemon=True).start()

    print("")
    print("=" * 55)
    print("  EV-Comm Dashboard  -- http://127.0.0.1:5000")
    print("  TCP Server         -- 127.0.0.1:9000")
    print("  UDP Heartbeats     -- 127.0.0.1:9001")
    print("=" * 55)
    print("")

    socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)


if __name__ == "__main__":
    main()
