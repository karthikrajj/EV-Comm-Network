"""
EV-Comm Network Launcher
========================
One-click script that starts all components for the network:
1. Dashboard + Network Server (Flask-SocketIO)
2. Junction nodes (J1-J5)
3. Hospital
4. Ambulance (interactive)

Usage:
    python run_network.py
"""

import subprocess
import sys
import os
import time
import signal

PROJECT_ROOT = os.path.dirname(__file__)
PYTHON = sys.executable


def main():
    processes = []

    print()
    print("=" * 60)
    print("   🚑 EV-Comm Network Launcher")
    print("   Emergency Vehicle Communication Network")
    print("=" * 60)
    print()

    # Step 1: Start Dashboard + Server
    print("[1/4] Starting Dashboard + Network Server...")
    p_dashboard = subprocess.Popen(
        [PYTHON, os.path.join(PROJECT_ROOT, "dashboard", "app.py")],
        cwd=PROJECT_ROOT,
    )
    processes.append(p_dashboard)
    time.sleep(3)  # Wait for server to be ready

    # Step 2: Start Junction nodes
    print("[2/4] Starting Junction Nodes (J1-J5)...")
    for jnc in ["J1", "J2", "J3", "J4", "J5"]:
        p = subprocess.Popen(
            [PYTHON, os.path.join(PROJECT_ROOT, "clients", "junction_client.py"), jnc],
            cwd=PROJECT_ROOT,
        )
        processes.append(p)
        time.sleep(0.5)

    # Step 3: Start Hospital
    print("[3/4] Starting Hospital...")
    p_hospital = subprocess.Popen(
        [PYTHON, os.path.join(PROJECT_ROOT, "clients", "hospital_client.py"), "HOSPITAL_1"],
        cwd=PROJECT_ROOT,
    )
    processes.append(p_hospital)
    time.sleep(1)

    print()
    print("=" * 60)
    print("   ✅ All systems online!")
    print()
    print("   🖥️  Dashboard:  http://127.0.0.1:5000")
    print("   📡 TCP Server:  127.0.0.1:9000")
    print("   📡 UDP Beats:   127.0.0.1:9001")
    print()
    print("   Now start an ambulance client in a new terminal:")
    print(f"     {PYTHON} clients/ambulance_client.py A01")
    print()
    print("   Ambulance commands:")
    print("     e CRITICAL J1 HOSPITAL_1   — Send emergency")
    print("     r                          — Show route")
    print("     q                          — Quit")
    print()
    print("   To inject junction failure:")
    print("     Kill any junction terminal with Ctrl+C")
    print("     Watch the dashboard reroute in real time!")
    print("=" * 60)
    print()

    # Step 4: Start interactive ambulance
    print("[4/4] Starting Ambulance A01 (interactive)...")
    print()
    try:
        p_ambulance = subprocess.Popen(
            [PYTHON, os.path.join(PROJECT_ROOT, "clients", "ambulance_client.py"), "A01"],
            cwd=PROJECT_ROOT,
        )
        processes.append(p_ambulance)
        p_ambulance.wait()  # Wait for the ambulance to exit (interactive)
    except KeyboardInterrupt:
        pass
    finally:
        print("\n🛑 Shutting down all processes...")
        for p in processes:
            try:
                p.terminate()
            except Exception:
                pass
        time.sleep(1)
        for p in processes:
            try:
                p.kill()
            except Exception:
                pass
        print("Done.")


if __name__ == "__main__":
    main()
