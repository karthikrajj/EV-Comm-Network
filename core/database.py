"""
EV-Comm Database Layer
======================
SQLite logging of every request, route, message, and analytics.
Thread-safe via check_same_thread=False and a module-level lock.
"""

import sqlite3
import threading
import os
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "evcomm.db")
_lock = threading.Lock()


def _get_conn() -> sqlite3.Connection:
    """Get a thread-safe connection."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Create all tables if they don't exist."""
    conn = _get_conn()
    with _lock:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS ambulances (
                id TEXT PRIMARY KEY,
                status TEXT DEFAULT 'IDLE',
                current_location TEXT,
                last_seen TEXT
            );

            CREATE TABLE IF NOT EXISTS junctions (
                id TEXT PRIMARY KEY,
                status TEXT DEFAULT 'UP',
                signal_state TEXT DEFAULT 'RED',
                last_heartbeat TEXT
            );

            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ambulance_id TEXT,
                priority TEXT,
                origin TEXT,
                destination TEXT,
                route TEXT,
                status TEXT DEFAULT 'PENDING',
                created_at TEXT,
                resolved_at TEXT
            );

            CREATE TABLE IF NOT EXISTS packet_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seq_no INTEGER,
                type TEXT,
                sender TEXT,
                receiver TEXT,
                timestamp TEXT,
                latency_ms REAL,
                dropped INTEGER DEFAULT 0,
                retransmitted INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS analytics (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_requests INTEGER DEFAULT 0,
                avg_response_time REAL DEFAULT 0.0,
                busiest_junction TEXT DEFAULT '',
                packet_loss_rate REAL DEFAULT 0.0
            );

            INSERT OR IGNORE INTO analytics (id) VALUES (1);
        """)
        conn.commit()
    conn.close()


# ── Ambulance operations ─────────────────────────────────────────────────────

def upsert_ambulance(amb_id: str, status: str = "IDLE", location: str = ""):
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    with _lock:
        conn.execute(
            "INSERT INTO ambulances (id, status, current_location, last_seen) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET status=?, current_location=?, last_seen=?",
            (amb_id, status, location, now, status, location, now),
        )
        conn.commit()
    conn.close()


def get_ambulances() -> list:
    conn = _get_conn()
    rows = conn.execute("SELECT * FROM ambulances").fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Junction operations ──────────────────────────────────────────────────────

def upsert_junction(jnc_id: str, status: str = "UP"):
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    with _lock:
        conn.execute(
            "INSERT INTO junctions (id, status, last_heartbeat) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET status=?, last_heartbeat=?",
            (jnc_id, status, now, status, now),
        )
        conn.commit()
    conn.close()


def update_junction_signal(jnc_id: str, signal_state: str):
    conn = _get_conn()
    with _lock:
        conn.execute(
            "UPDATE junctions SET signal_state=? WHERE id=?",
            (signal_state, jnc_id),
        )
        conn.commit()
    conn.close()


def get_junctions() -> list:
    conn = _get_conn()
    rows = conn.execute("SELECT * FROM junctions").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_junction_down(jnc_id: str):
    upsert_junction(jnc_id, "DOWN")


# ── Request operations ───────────────────────────────────────────────────────

def create_request(ambulance_id: str, priority: str, origin: str, destination: str) -> int:
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    with _lock:
        cursor = conn.execute(
            "INSERT INTO requests (ambulance_id, priority, origin, destination, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (ambulance_id, priority, origin, destination, now),
        )
        conn.commit()
        req_id = cursor.lastrowid
    conn.close()
    return req_id


def update_request(req_id: int, route: str = None, status: str = None):
    conn = _get_conn()
    with _lock:
        if route:
            conn.execute("UPDATE requests SET route=? WHERE id=?", (route, req_id))
        if status:
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE requests SET status=?, resolved_at=? WHERE id=?",
                (status, now, req_id),
            )
        conn.commit()
    conn.close()


def get_requests() -> list:
    conn = _get_conn()
    rows = conn.execute("SELECT * FROM requests ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Packet log ───────────────────────────────────────────────────────────────

def log_packet(seq_no: int, ptype: str, sender: str, receiver: str,
               latency_ms: float = 0.0, dropped: bool = False, retransmitted: bool = False):
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    with _lock:
        conn.execute(
            "INSERT INTO packet_log (seq_no, type, sender, receiver, timestamp, latency_ms, dropped, retransmitted) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (seq_no, ptype, sender, receiver, now, latency_ms, int(dropped), int(retransmitted)),
        )
        conn.commit()
    conn.close()


def get_packet_log(limit: int = 100) -> list:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM packet_log ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Analytics ────────────────────────────────────────────────────────────────

def refresh_analytics() -> dict:
    """Recompute analytics from raw data."""
    conn = _get_conn()
    with _lock:
        total = conn.execute("SELECT COUNT(*) FROM requests").fetchone()[0]

        avg_row = conn.execute("""
            SELECT AVG(
                CAST((julianday(resolved_at) - julianday(created_at)) * 86400 AS REAL)
            ) as avg_time
            FROM requests WHERE resolved_at IS NOT NULL
        """).fetchone()
        avg_time = avg_row[0] if avg_row[0] else 0.0

        busiest = conn.execute("""
            SELECT receiver, COUNT(*) as cnt FROM packet_log
            WHERE receiver LIKE 'J%' GROUP BY receiver ORDER BY cnt DESC LIMIT 1
        """).fetchone()
        busiest_jnc = busiest[0] if busiest else ""

        loss_row = conn.execute("""
            SELECT
                CAST(SUM(dropped) AS REAL) / MAX(COUNT(*), 1) * 100
            FROM packet_log
        """).fetchone()
        loss_rate = loss_row[0] if loss_row[0] else 0.0

        conn.execute(
            "UPDATE analytics SET total_requests=?, avg_response_time=?, "
            "busiest_junction=?, packet_loss_rate=? WHERE id=1",
            (total, round(avg_time, 2), busiest_jnc, round(loss_rate, 2)),
        )
        conn.commit()

    result = dict(conn.execute("SELECT * FROM analytics WHERE id=1").fetchone())
    conn.close()
    return result
