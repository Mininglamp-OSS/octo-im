#!/usr/bin/env python3
"""Probe application and WebSocket heartbeats across a real user-leader crash.

Uses only the Python standard library. All client sockets stay on surviving
nodes; no automatic reconnect hides missed heartbeats. New application probes
continue while earlier requests are outstanding, so an obsolete queued request
can be distinguished from current routing failure. Only owned processes stop.
"""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import threading
import time
import urllib.request
import zlib


class Run:
    def __init__(self, args):
        self.args = args
        self.root = args.output.resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.started = time.monotonic()
        self.lock = threading.RLock()
        self.rows = []
        self.nodes = []
        self.clients = []
        self.reservations = []
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        for index in range(3):
            host = f"127.87.0.{11 + index}"
            folder = self.root / f"node{index + 1}"
            folder.mkdir()
            ports = {}
            for key in ("api", "cluster", "tcp", "ws", "intranet", "manager", "demo"):
                reserved = socket.socket()
                reserved.bind((host, 0))
                ports[key] = reserved.getsockname()[1]
                self.reservations.append((index, reserved))
            self.nodes.append(dict(id=1001 + index, index=index, host=host,
                                   folder=folder, ports=ports, process=None))
        for node in self.nodes:
            host, p = node["host"], node["ports"]
            node["url"] = f"http://{host}:{p['api']}"
            config = dict(
                mode="release", rootDir=str(node["folder"] / "data"),
                addr=f"tcp://{host}:{p['tcp']}", httpAddr=f"{host}:{p['api']}",
                wsAddr=f"ws://{host}:{p['ws']}",
                intranet=dict(tcpAddr=f"{host}:{p['intranet']}"),
                tokenAuthOn=False, whitelistOffOfPerson=True, pprofOn=False,
                auth=dict(kind="none"), logger=dict(level=1, dir=str(node["folder"] / "logs")),
                manager=dict(on=False, addr=f"{host}:{p['manager']}"),
                demo=dict(on=False, addr=f"{host}:{p['demo']}"),
                jwt=dict(secret="heartbeat-failover-isolated-test-only"),
                conversation=dict(on=True), db=dict(shardNum=2, slotShardNum=2),
                plugin=dict(socketPath=f"/tmp/heartbeat-failover-{os.getpid()}-{node['id']}.sock"),
                cluster=dict(nodeId=node["id"], addr=f"tcp://{host}:{p['cluster']}",
                             serverAddr=f"{host}:{p['cluster']}", apiUrl=node["url"],
                             slotCount=64, slotReplicaCount=3, channelReplicaCount=3,
                             initNodes=[f"{n['id']}@{n['host']}:{n['ports']['cluster']}"
                                        for n in self.nodes]))
            (node["folder"] / "config.json").write_text(json.dumps(config, indent=2))
        manifest = dict(binary=str(args.binary.resolve()),
                        binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),
                        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                        clients_per_group=args.clients_per_group, crash_after_s=args.crash_after,
                        downtime_s=args.downtime, after_restart_s=args.after_restart,
                        json_ping_timeout_s=10, control_pong_timeout_s=3,
                        gomaxprocs_per_node=2, load="heartbeats only", automatic_reconnect=False)
        (self.root / "manifest.json").write_text(json.dumps(manifest, indent=2))

    def record(self, kind, **data):
        with self.lock:
            row = dict(t=round(time.monotonic() - self.started, 6), kind=kind, **data)
            self.rows.append(row)
            with (self.root / "events.jsonl").open("a") as stream:
                stream.write(json.dumps(row) + "\n")
        return row

    def start(self, node):
        for index, reserved in self.reservations:
            if index == node["index"]:
                reserved.close()
        env = {key: value for key, value in os.environ.items() if not key.startswith("WK_")}
        env["GOMAXPROCS"] = "2"
        with (node["folder"] / "stdout.log").open("ab") as log:
            node["process"] = subprocess.Popen(
                [str(self.args.binary.resolve()), "--config", str(node["folder"] / "config.json")],
                cwd=node["folder"], env=env, stdout=log, stderr=subprocess.STDOUT)
        self.record("node_start", node=node["id"], pid=node["process"].pid)

    def call(self, node, path, payload=None):
        request = urllib.request.Request(node["url"] + path,
                    data=None if payload is None else json.dumps(payload).encode(),
                    headers={"Content-Type": "application/json"})
        with self.http.open(request, timeout=1) as response:
            return json.load(response)

    def slots(self, node):
        rows = self.call(node, "/cluster/allslot")["data"]
        return {int(row["id"]): int(row["leader_id"]) for row in rows}

    def ready(self):
        deadline = time.monotonic() + 45
        last_error = None
        while time.monotonic() < deadline:
            try:
                views = [self.slots(node) for node in self.nodes]
                assert all(len(view) == 64 and set(view.values()) == {1001, 1002, 1003}
                           for view in views), views
                assert views[0] == views[1] == views[2], views
                time.sleep(2)
                return views[0]
            except (OSError, ValueError, AssertionError, KeyError) as error:
                last_error = repr(error)
                for node in self.nodes:
                    if node["process"].poll() is not None:
                        raise RuntimeError(f"node {node['id']} exited; inspect stdout.log")
                time.sleep(0.2)
        raise AssertionError(f"cluster readiness failed: {last_error}")

    def stop(self):
        for client in self.clients:
            client.close()
        for node in self.nodes:
            proc = node["process"]
            if proc is not None and proc.poll() is None:
                proc.terminate()
        for node in self.nodes:
            proc = node["process"]
            if proc is not None:
                try:
                    proc.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
                self.record("node_stop", node=node["id"], exit_code=proc.returncode)
        for _, reserved in self.reservations:
            reserved.close()


class Client:
    def __init__(self, run, node, uid, slot, leader):
        self.run, self.node, self.uid = run, node, uid
        self.slot, self.original_leader = slot, leader
        self.lock = threading.RLock()
        self.pending = {}
        self.connected = threading.Event()
        self.connect_response = None
        self.closing = False
        self.sock = socket.create_connection((node["host"], node["ports"]["ws"]), timeout=5)
        run.clients.append(self)
        self.reader = self.sock.makefile("rb")
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall((f"GET / HTTP/1.1\r\nHost: {node['host']}:{node['ports']['ws']}\r\n"
                           "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                           f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
        status = self.reader.readline()
        assert b" 101 " in status, status
        headers = {}
        while True:
            line = self.reader.readline()
            if line == b"\r\n":
                break
            if not line:
                raise EOFError("websocket handshake ended early")
            name, value = line.decode().split(":", 1)
            headers[name.lower()] = value.strip()
        expected = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
        assert headers.get("sec-websocket-accept") == expected, headers
        self.sock.settimeout(None)
        self.thread = threading.Thread(target=self.receive, daemon=True)
        self.thread.start()
        self.send_frame(1, json.dumps(dict(jsonrpc="2.0", id="connect", method="connect",
                         params=dict(version=5, deviceId="heartbeat-" + uid, deviceFlag=1,
                                     uid=uid, token="test"))).encode())
        assert self.connected.wait(8), f"CONNECT timeout for {uid}"
        assert self.connect_response.get("result", {}).get("reasonCode") == 1, self.connect_response
        run.record("client_ready", uid=uid, owner=node["id"], slot=slot, original_leader=leader)

    def send_frame(self, opcode, payload):
        mask = os.urandom(4)
        length = len(payload)
        if length < 126:
            header = bytes([0x80 | opcode, 0x80 | length])
        elif length < 65536:
            header = bytes([0x80 | opcode, 0x80 | 126]) + struct.pack("!H", length)
        else:
            header = bytes([0x80 | opcode, 0x80 | 127]) + struct.pack("!Q", length)
        encoded = bytes(value ^ mask[i % 4] for i, value in enumerate(payload))
        with self.lock:
            self.sock.sendall(header + mask + encoded)

    def probe(self, sequence):
        for kind, timeout in (("application", 10), ("control", 3)):
            ident = f"{kind}-{sequence}"
            with self.lock:
                entry = dict(sent=time.monotonic(), timeout=timeout, timed_out=False, kind=kind)
                self.pending[ident] = entry
                self.run.record("probe_send", uid=self.uid, owner=self.node["id"],
                                original_leader=self.original_leader, request_id=ident, probe=kind)
                if kind == "application":
                    data = json.dumps(dict(jsonrpc="2.0", id=ident, method="ping", params={})).encode()
                    self.send_frame(1, data)
                else:
                    self.send_frame(9, ident.encode())

    def finish_probe(self, ident, response=None):
        with self.lock:
            pending = self.pending.pop(ident, None)
            if pending is None:
                self.run.record("unexpected_response", uid=self.uid, request_id=ident, response=response)
                return
            latency = time.monotonic() - pending["sent"]
            if latency >= pending["timeout"] and not pending["timed_out"]:
                pending["timed_out"] = True
                self.run.record("probe_timeout", uid=self.uid, owner=self.node["id"],
                                original_leader=self.original_leader, request_id=ident,
                                probe=pending["kind"], timeout_s=pending["timeout"])
            self.run.record("probe_response", uid=self.uid, owner=self.node["id"],
                            original_leader=self.original_leader, request_id=ident,
                            probe=pending["kind"], late=pending["timed_out"], response=response,
                            latency_ms=1000 * latency)
            if response is not None and (response.get("error") is not None or "method" in response):
                self.run.record("probe_error_response", uid=self.uid, request_id=ident, response=response)

    def expire(self):
        now = time.monotonic()
        with self.lock:
            for ident, entry in self.pending.items():
                if not entry["timed_out"] and now - entry["sent"] >= entry["timeout"]:
                    entry["timed_out"] = True
                    self.run.record("probe_timeout", uid=self.uid, owner=self.node["id"],
                                    original_leader=self.original_leader, request_id=ident,
                                    probe=entry["kind"], timeout_s=entry["timeout"])

    def read_exact(self, size):
        data = self.reader.read(size)
        if len(data) != size:
            raise EOFError("physical WebSocket closed")
        return data

    def receive(self):
        try:
            while not self.closing:
                first, second = self.read_exact(2)
                opcode, size = first & 15, second & 127
                if size == 126:
                    size = struct.unpack("!H", self.read_exact(2))[0]
                elif size == 127:
                    size = struct.unpack("!Q", self.read_exact(8))[0]
                if second & 128 or size > 1048576:
                    raise AssertionError("unexpected server frame")
                data = self.read_exact(size)
                if opcode == 10:
                    self.finish_probe(data.decode())
                elif opcode == 9:
                    self.send_frame(10, data)
                elif opcode in (1, 2):
                    reply = json.loads(data)
                    if reply.get("id") == "connect":
                        self.connect_response = reply
                        self.connected.set()
                    else:
                        self.finish_probe(reply.get("id"), reply)
                elif opcode == 8:
                    raise EOFError(f"server close frame {data.hex()}")
                else:
                    raise AssertionError(f"unexpected server opcode {opcode}")
        except Exception as error:
            if not self.closing:
                self.run.record("socket_error", uid=self.uid, owner=self.node["id"], error=repr(error))

    def close(self):
        self.closing = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
        if hasattr(self, "thread"):
            self.thread.join(timeout=2)
        self.reader.close()


def exercise(run):
    for node in run.nodes:
        run.start(node)
    initial = run.ready()
    (run.root / "initial-slots.json").write_text(json.dumps(initial, indent=2))
    for owner in run.nodes[1:]:
        for leader in (1001, 1002, 1003):
            matched = 0
            for candidate in range(10000):
                uid = f"heartbeat-{owner['id']}-{leader}-{candidate}"
                slot = zlib.crc32(uid.encode()) % 64
                if initial[slot] == leader:
                    Client(run, owner, uid, slot, leader)
                    matched += 1
                if matched == run.args.clients_per_group:
                    break
            assert matched == run.args.clients_per_group
    began = time.monotonic()
    victim = run.nodes[0]
    killed = restarted = False
    first_new_leader = {}
    last_views = {}
    sequence = 0
    next_probe = next_slots = began
    probe_end = began + run.args.crash_after + run.args.downtime + run.args.after_restart
    while time.monotonic() < probe_end + 10.5:
        now = time.monotonic()
        elapsed = now - began
        if not killed and elapsed >= run.args.crash_after:
            victim["process"].kill()
            victim["process"].wait(timeout=5)
            run.record("leader_killed", node=1001)
            killed = True
            print("Killed user-slot leader 1001; all client sockets remain on 1002/1003.", flush=True)
        if killed and not restarted and elapsed >= run.args.crash_after + run.args.downtime:
            run.start(victim)
            restarted = True
            print("Restarted node 1001.", flush=True)
        if now >= next_probe and now < probe_end:
            for client in run.clients:
                client.probe(sequence)
            sequence += 1
            next_probe += 1
        if now >= next_slots:
            for owner in run.nodes[1:]:
                try:
                    view = run.slots(owner)
                    if view != last_views.get(owner["id"]):
                        run.record("slot_view", observer=owner["id"], leaders=view)
                        last_views[owner["id"]] = view
                    if killed and owner["id"] not in first_new_leader and all(
                            view[slot] not in (0, 1001) for slot in initial if initial[slot] == 1001):
                        first_new_leader[owner["id"]] = run.record("leaders_replaced", observer=owner["id"])["t"]
                except (OSError, ValueError, KeyError) as error:
                    run.record("slot_query_error", observer=owner["id"], error=repr(error))
            next_slots = time.monotonic() + 0.2
        for client in run.clients:
            client.expire()
        time.sleep(0.025)

    rows = run.rows
    kill_time = next(row["t"] for row in rows if row["kind"] == "leader_killed")
    summary = dict(clients=len(run.clients),
                   original_leader_1001_clients=sum(c.original_leader == 1001 for c in run.clients),
                   leader_replacement_s={str(k): v - kill_time for k, v in first_new_leader.items()},
                   groups=[])
    for owner in (1002, 1003):
        for leader in (1001, 1002, 1003):
            group = [r for r in rows if r.get("owner") == owner and r.get("original_leader") == leader]
            group_summary = dict(owner=owner, original_leader=leader)
            for probe in ("application", "control"):
                group_summary[probe + "_sent"] = sum(r["kind"] == "probe_send" and r.get("probe") == probe for r in group)
                group_summary[probe + "_timeouts"] = sum(r["kind"] == "probe_timeout" and r.get("probe") == probe for r in group)
                latencies = [r["latency_ms"] for r in group if r["kind"] == "probe_response" and r["probe"] == probe]
                group_summary[probe + "_max_response_ms"] = max(latencies, default=None)
            summary["groups"].append(group_summary)
    summary["socket_errors"] = [r for r in rows if r["kind"] == "socket_error"]
    summary["application_timeouts"] = sum(g["application_timeouts"] for g in summary["groups"])
    summary["control_timeouts"] = sum(g["control_timeouts"] for g in summary["groups"])
    summary["healthy_leader_application_timeouts"] = sum(g["application_timeouts"] for g in summary["groups"] if g["original_leader"] != 1001)
    summary["timed_out_clients"] = sorted({r["uid"] for r in rows if r["kind"] == "probe_timeout" and r["probe"] == "application"})
    summary["error_responses"] = [r for r in rows if r["kind"] == "probe_error_response"]
    expected_uids = {client.uid for client in run.clients}
    summary["final_online"] = {}
    for node in run.nodes:
        try:
            response = run.call(node, "/user/onlinestatus", sorted(expected_uids))
            online = {entry["uid"] for entry in response if entry.get("online") == 1}
            summary["final_online"][str(node["id"])] = dict(count=len(online), matches=online == expected_uids)
            run.record("final_online", observer=node["id"], response=response)
        except (OSError, KeyError, TypeError, ValueError) as error:
            summary["final_online"][str(node["id"])] = dict(matches=False, error=repr(error))
    (run.root / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2), flush=True)
    assert len(first_new_leader) == 2, "new leaders were not observed on both survivors"
    assert not summary["socket_errors"], "a surviving physical socket closed"
    assert not summary["error_responses"], "application PING returned an error instead of PONG"
    assert all(row["matches"] for row in summary["final_online"].values()), "physical sessions were not preserved in online queries"
    assert summary["control_timeouts"] == 0, "local WebSocket control PONG stalled"
    assert summary["healthy_leader_application_timeouts"] == 0, "healthy-leader control group stalled"
    if run.args.expect_timeouts:
        assert summary["application_timeouts"] > 0, "suspected application heartbeat failure did not reproduce"
    else:
        assert summary["application_timeouts"] == 0, "application heartbeat lost across user-leader failover"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--clients-per-group", type=int, default=2)
    parser.add_argument("--crash-after", type=float, default=6)
    parser.add_argument("--downtime", type=float, default=20)
    parser.add_argument("--after-restart", type=float, default=14)
    parser.add_argument("--expect-timeouts", action="store_true")
    args = parser.parse_args()
    if args.clients_per_group < 1 or args.crash_after < 2 or args.downtime < 12 or args.after_restart < 1:
        parser.error("need clients, warmup, and a downtime longer than the application timeout")
    run = Run(args)
    try:
        exercise(run)
    except Exception as error:
        run.record("failure", error=repr(error))
        raise
    finally:
        run.stop()


if __name__ == "__main__":
    main()
