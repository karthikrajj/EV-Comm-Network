# EV-Comm: Emergency Vehicle Communication and Traffic Priority Network

> Computer Networks Mini Project — Amal Jyothi College of Engineering

A real-time simulation of emergency vehicle communication across a city's traffic network, demonstrating core Computer Networks concepts through a live, demoable prototype.

## 🏗️ Architecture

```
Ambulance Client  ↔  Network Server  ↔  Traffic Junction Nodes  ↔  Hospital Client
    (TCP)                (TCP+UDP)              (TCP+UDP)               (TCP)
                              ↕
                     Flask-SocketIO Dashboard
                        (WebSocket)
```

## 🎯 CN Concepts Demonstrated

| Concept | Implementation |
|---|---|
| Client-Server Architecture | Ambulance/junction/hospital clients ↔ central server |
| TCP vs UDP | Emergency requests (TCP) vs heartbeats (UDP) |
| Packet Structure & Headers | JSON format: seq_no, TTL, checksum, timestamp |
| Reliable Delivery | ACK/NACK + retransmission on simulated packet loss |
| Routing Algorithms | Dijkstra's algorithm over config-driven junction graph |
| Congestion Control | Delay injection + reroute on multiple active ambulances |
| Multi-hop Topology | Junction graph with weighted edges |
| Fault Tolerance | Heartbeat-based failure detection + live rerouting |
| Network Security | Per-client auth tokens |
| Network Monitoring | Live packet log, latency, topology view, analytics |

## 📁 Project Structure

```
cn/
├── config/
│   ├── topology.json          # Junction graph (config-driven)
│   └── auth_tokens.json       # Per-client auth tokens
├── core/
│   ├── protocol.py            # Packet format, checksums, serialization
│   ├── database.py            # SQLite logging layer
│   └── routing.py             # Dijkstra routing engine
├── server/
│   └── network_server.py      # Central TCP+UDP server
├── clients/
│   ├── ambulance_client.py    # Ambulance TCP client (interactive)
│   ├── junction_client.py     # Junction TCP+UDP client
│   └── hospital_client.py     # Hospital TCP client
├── dashboard/
│   ├── app.py                 # Flask-SocketIO web dashboard
│   ├── templates/
│   │   └── dashboard.html     # Dashboard UI
│   └── static/
│       ├── dashboard.css      # Premium dark theme
│       └── dashboard.js       # Real-time WebSocket frontend
├── run_demo.py                # One-click demo launcher
├── requirements.txt           # Python dependencies
└── EV-Comm-Project-Plan.md    # Full project plan
```

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the Demo
```bash
python run_demo.py
```

This starts everything:
- Dashboard + Network Server → http://127.0.0.1:5000
- Login Access Code: **admin123**
- 5 Junction Nodes (J1-J5) with UDP heartbeats
- Hospital (HOSPITAL_1)
- Ambulance A01 (interactive CLI)

### 3. Send an Emergency
In the ambulance terminal:
```
[A01] > e CRITICAL J1 HOSPITAL_1
```

### 4. Simulate Junction Failure
Open a separate terminal and start a junction:
```bash
python clients/junction_client.py J2
```
Then press **Ctrl+C** to kill it — watch the dashboard reroute in real time!

### 5. Multiple Ambulances
Open another terminal:
```bash
python clients/ambulance_client.py A02
```
Send a simultaneous emergency to trigger congestion detection.

## 🖥️ Manual Start (Separate Terminals)

```bash
# Terminal 1: Dashboard + Server
python dashboard/app.py

# Terminal 2-6: Junctions
python clients/junction_client.py J1
python clients/junction_client.py J2
python clients/junction_client.py J3
python clients/junction_client.py J4
python clients/junction_client.py J5

# Terminal 7: Hospital
python clients/hospital_client.py HOSPITAL_1

# Terminal 8: Ambulance
python clients/ambulance_client.py A01
```

## 🐳 Docker Deployment

You can run the entire simulation network in an isolated Docker container.

```bash
docker-compose up --build
```
This builds the image and maps ports `5000` (Dashboard), `9000` (TCP Server), and `9001` (UDP Heartbeats). Access the dashboard at `http://localhost:5000` with the access code **admin123**.

## 📊 Demo Flow (5-6 minutes)

1. Start server → shows listening port, loads junction graph
2. Junctions + hospital connect → heartbeats begin over UDP
3. Ambulance A01 connects → sends EMERGENCY_REQUEST over TCP
4. Server ACKs, runs Dijkstra, returns route
5. Packet log shows live JSON packets with occasional simulated drops + retransmits
6. Traffic junctions flip to GREEN along the route
7. **Kill Junction J2** → heartbeat timeout → server marks DOWN → route recalculates live
8. Second ambulance A02 sends request → congestion detection triggers
9. Hospital receives notification, sends ACK back through the chain
10. Analytics dashboard shows session totals

## 🔧 Configuration

### Topology (`config/topology.json`)
Modify the junction graph without touching code — add/remove nodes and edges.

### Auth Tokens (`config/auth_tokens.json`)
Each client must authenticate with its token to connect.

---

*Prepared for: Karthik — CN Mini Project, Amal Jyothi College of Engineering*
