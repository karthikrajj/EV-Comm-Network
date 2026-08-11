"""
EV-Comm Routing Engine
======================
Dijkstra's algorithm over a config-driven junction graph.
Supports dynamic edge weight updates (congestion) and node removal (failure).
"""

import json
import heapq
import os
from typing import Optional


class JunctionGraph:
    """
    Weighted undirected graph representing the traffic junction network.
    Loaded from config/topology.json — config-driven, not hardcoded.
    """

    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "config", "topology.json"
            )
        self.config_path = config_path
        self.adjacency: dict[str, list[tuple[str, float, float]]] = {}
        self.down_nodes: set[str] = set()
        self.node_coords = {}
        self.original_weights = {}
        self.load()

    def load(self):
        """Load topology from JSON config file."""
        with open(self.config_path, "r") as f:
            data = json.load(f)

        self.adjacency = {node: [] for node in data.get("nodes", [])}
        if isinstance(data.get("nodes"), dict):
            self.node_coords = data["nodes"]
            self.adjacency = {node: [] for node in self.node_coords}

        for edge in data["edges"]:
            src, dst, w = edge["from"], edge["to"], edge["weight"]
            road_type = edge.get("road_type", 1.0)
            # Store (destination, base_weight, road_type)
            self.adjacency.setdefault(src, []).append((dst, w, road_type))
            self.adjacency.setdefault(dst, []).append((src, w, road_type))
            self.original_weights[(src, dst)] = {"weight": w, "road_type": road_type}
            self.original_weights[(dst, src)] = {"weight": w, "road_type": road_type}

    def mark_down(self, node: str):
        """Mark a junction as down (removes it from routing consideration)."""
        self.down_nodes.add(node)

    def mark_up(self, node: str):
        """Mark a junction as back up."""
        self.down_nodes.discard(node)

    def set_congestion(self, src: str, dst: str, multiplier: float = 3.0):
        """Inflate an edge's weight to simulate congestion."""
        orig_data = self.original_weights.get((src, dst))
        if orig_data is None:
            return
        new_weight = orig_data["weight"] * multiplier
        for i, (neighbor, w, rt) in enumerate(self.adjacency.get(src, [])):
            if neighbor == dst:
                self.adjacency[src][i] = (dst, new_weight, rt)
        for i, (neighbor, w, rt) in enumerate(self.adjacency.get(dst, [])):
            if neighbor == src:
                self.adjacency[dst][i] = (src, new_weight, rt)

    def clear_congestion(self, src: str, dst: str):
        """Restore an edge's original weight."""
        orig_data = self.original_weights.get((src, dst))
        if orig_data is None:
            return
        orig_w = orig_data["weight"]
        for i, (neighbor, w, rt) in enumerate(self.adjacency.get(src, [])):
            if neighbor == dst:
                self.adjacency[src][i] = (dst, orig_w, rt)
        for i, (neighbor, w, rt) in enumerate(self.adjacency.get(dst, [])):
            if neighbor == src:
                self.adjacency[dst][i] = (src, orig_w, rt)

    def heuristic(self, node_a: str, node_b: str) -> float:
        """Euclidean distance heuristic for A*."""
        import math
        if node_a not in self.node_coords or node_b not in self.node_coords:
            return 0
        c1, c2 = self.node_coords[node_a], self.node_coords[node_b]
        return math.sqrt((c1["x"] - c2["x"])**2 + (c1["y"] - c2["y"])**2) / 100.0

    def dijkstra(self, start: str, end: str) -> Optional[tuple[list[str], float]]:
        """
        Compute the shortest path using A* search.
        Incorporates multi-criteria weights (base_weight * congestion * road_type).
        """
        if start not in self.adjacency or end not in self.adjacency:
            return None

        # Distance map and predecessor tracking
        dist = {node: float("inf") for node in self.adjacency}
        dist[start] = 0
        prev = {node: None for node in self.adjacency}
        visited = set()

        # Min-heap: (f_score, d, node)
        heap = [(self.heuristic(start, end), 0, start)]

        while heap:
            f, d, u = heapq.heappop(heap)

            if u in visited:
                continue
            visited.add(u)

            if u == end:
                break

            if u in self.down_nodes and u != start:
                continue

            for neighbor, weight, road_type in self.adjacency.get(u, []):
                if neighbor in self.down_nodes:
                    continue
                if neighbor in visited:
                    continue
                # Multi-criteria routing (weight considers congestion & road type)
                cost = weight * road_type
                new_dist = d + cost
                
                if new_dist < dist[neighbor]:
                    dist[neighbor] = new_dist
                    prev[neighbor] = u
                    f_score = new_dist + self.heuristic(neighbor, end)
                    heapq.heappush(heap, (f_score, new_dist, neighbor))

        # Reconstruct path
        if dist[end] == float("inf"):
            return None

        path = []
        current = end
        while current is not None:
            path.append(current)
            current = prev[current]
        path.reverse()

        return path, dist[end]

    def get_topology_data(self) -> dict:
        """Return topology info for the dashboard visualization."""
        nodes = []
        for node_id in self.adjacency:
            nodes.append({
                "id": node_id,
                "status": "DOWN" if node_id in self.down_nodes else "UP",
                "type": "hospital" if "HOSPITAL" in node_id else "junction",
            })

        edges = []
        seen = set()
        for src in self.adjacency:
            for dst, weight, rt in self.adjacency[src]:
                edge_key = tuple(sorted([src, dst]))
                if edge_key not in seen:
                    seen.add(edge_key)
                    orig = self.original_weights.get((src, dst), {"weight": weight})["weight"]
                    edges.append({
                        "from": src,
                        "to": dst,
                        "weight": weight,
                        "road_type": rt,
                        "congested": weight > orig,
                    })

        return {"nodes": nodes, "edges": edges}
