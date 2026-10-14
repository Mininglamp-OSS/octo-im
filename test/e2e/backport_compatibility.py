#!/usr/bin/env python3
"""Real-process compatibility checks for subscriber cleanup and socket recovery.

Build main.go, then supply --binary and a new --output directory.
cleanup checks all replicas before and after restart, with a small churn workload.
sessions uses four nodes so the physical socket owner is outside the user slot's
three replicas; it keeps that socket open across loss of the user-slot leader.
Only this invocation's processes and data directories are cleaned up.
"""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
import zlib


def eventually(check, seconds=60):
    deadline, last = time.monotonic() + seconds, None
    while time.monotonic() < deadline:
        try:
            return check()
        except (AssertionError, OSError, ValueError) as error:
            last = error
            time.sleep(0.2)
    raise AssertionError(f"did not converge in {seconds}s: {last}")


class Cluster:
    def __init__(self, binary, root, count, slots):
        self.binary, self.root, self.slots = binary, root, slots
        self.nodes, self.reservations, self.clients = [], {}, []
        self.lock = threading.Lock()
        self.replicas = min(count, 3)
        for index in range(count):
            folder = root / f"node{index + 1}"
            folder.mkdir()
            ports = {}
            for key in ("api", "cluster", "tcp", "ws", "intranet", "manager", "demo"):
                sock = socket.socket()
                sock.bind(("127.0.0.1", 0))
                ports[key] = sock.getsockname()[1]
                self.reservations[ports[key]] = sock
            self.nodes.append(dict(id=1001 + index, folder=folder, ports=ports, process=None))
        for node in self.nodes:
            p, folder = node["ports"], node["folder"]
            node["url"] = f"http://127.0.0.1:{p['api']}"
            node["socket_path"] = f"/tmp/backport-compat-{os.getpid()}-{node['id']}.sock"
            config = dict(
                mode="release", rootDir=str(folder / "data"),
                addr=f"tcp://127.0.0.1:{p['tcp']}", httpAddr=f"127.0.0.1:{p['api']}",
                wsAddr=f"ws://127.0.0.1:{p['ws']}", intranet=dict(tcpAddr=f"127.0.0.1:{p['intranet']}"),
                tokenAuthOn=False, whitelistOffOfPerson=True, pprofOn=False,
                logger=dict(level=1, dir=str(folder / "logs")), auth=dict(kind="none"),
                manager=dict(on=False, addr=f"127.0.0.1:{p['manager']}"),
                demo=dict(on=False, addr=f"127.0.0.1:{p['demo']}"),
                jwt=dict(secret="backport-compatibility-isolated-test-only"),
                conversation=dict(on=True), db=dict(shardNum=2, slotShardNum=2),
                plugin=dict(socketPath=node["socket_path"]),
                cluster=dict(nodeId=node["id"], addr=f"tcp://127.0.0.1:{p['cluster']}",
                             apiUrl=node["url"], slotCount=slots,
                             slotReplicaCount=self.replicas, channelReplicaCount=self.replicas,
                             initNodes=[f"{n['id']}@127.0.0.1:{n['ports']['cluster']}"
                                        for n in self.nodes] if count > 1 else []))
            (folder / "config.json").write_text(json.dumps(config, indent=2))

    def record(self, kind, **data):
        with self.lock, (self.root / "events.jsonl").open("a") as stream:
            stream.write(json.dumps(dict(time=time.time(), kind=kind, **data)) + "\n")

    def start(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith("WK_")}
        env["GOMAXPROCS"] = "4"
        for node in self.nodes:
            for number in node["ports"].values():
                reservation = self.reservations.pop(number, None)
                if reservation is not None:
                    reservation.close()
            with (node["folder"] / "stdout.log").open("ab") as log:
                node["process"] = subprocess.Popen(
                    [str(self.binary), "--config", str(node["folder"] / "config.json")],
                    cwd=node["folder"], env=env, stdout=log, stderr=subprocess.STDOUT)
            self.record("start", node=node["id"], pid=node["process"].pid)

    def stop(self):
        for node in self.nodes:
            proc = node["process"]
            if proc is not None and proc.poll() is None:
                proc.terminate()
        for node in self.nodes:
            proc = node["process"]
            if proc is not None:
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                self.record("stop", node=node["id"], exit_code=proc.returncode)
                node["process"] = None

    def call(self, node, path, payload=None):
        request = urllib.request.Request(
            node["url"] + path, data=None if payload is None else json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=30) as response:
            body = json.load(response)
        self.record("http", node=node["id"], path=path, body=body)
        return body

    def ready(self):
        def check():
            result = None
            for node in self.nodes:
                if node["process"].poll() is not None:
                    raise RuntimeError(f"node {node['id']} exited; inspect stdout.log")
                result = self.call(node, "/cluster/allslot")["data"]
                assert len(result) == self.slots
                assert all(s["leader_id"] and s["status"] == 0 and
                           len(s["replicas"]) == self.replicas for s in result), result
            return result
        result = eventually(check)
        # Metadata can advertise leaders before each peer installs its role.
        time.sleep(3)
        return result

    def rows(self, users):
        def read(item):
            node, uid = item
            body = self.call(node, f"/cluster/conversations?node_id={node['id']}&uid={uid}&limit=10000")
            assert "data" in body, body
            return dict(node=node["id"], uid=uid, rows=body["data"] or [])
        with ThreadPoolExecutor(max_workers=8) as pool:
            return list(pool.map(read, [(n, u) for n in self.nodes for u in users]))

    def wait_rows(self, users, count):
        def check():
            rows = self.rows(users)
            assert all(len(row["rows"]) == count for row in rows), rows
            return rows
        return eventually(check, seconds=30)


def cleanup(cluster):
    slots = {s["id"]: s for s in cluster.ready()}
    users = [f"cleanup-user-{i}" for i in range(24)]
    channels = ["cleanup-remove", "cleanup-blacklist-add", "cleanup-blacklist-set"]
    pairs = [(channel, uid) for channel in channels for uid in users
             if slots[zlib.crc32(channel.encode()) % cluster.slots]["leader_id"] !=
             slots[zlib.crc32(uid.encode()) % cluster.slots]["leader_id"]]
    if len(cluster.nodes) > 1:
        assert pairs, "must exercise cleanup across different channel and user slot leaders"
    ingress = cluster.nodes[0]
    for channel in channels:
        cluster.call(ingress, "/channel", dict(channel_id=channel, channel_type=2, subscribers=users))
    before = cluster.wait_rows(users, 3)
    (cluster.root / "before-cleanup.json").write_text(json.dumps(before, indent=2))
    for channel, endpoint, field in zip(channels,
            ["/channel/subscriber_remove", "/channel/blacklist_add", "/channel/blacklist_set"],
            ["subscribers", "uids", "uids"]):
        cluster.call(ingress, endpoint, dict(channel_id=channel, channel_type=2, **{field: users}))
    cluster.wait_rows(users, 0)

    load_users = [f"churn-user-{i}" for i in range(50)]
    def churn(index):
        payload = dict(channel_id=f"churn-channel-{index}", channel_type=2, subscribers=load_users)
        for endpoint in ("/channel", "/channel/subscriber_remove", "/channel/subscriber_add",
                         "/channel/subscriber_remove"):
            cluster.call(ingress, endpoint, payload)
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(churn, range(16)))
    seconds = time.monotonic() - started
    after = cluster.wait_rows(users + load_users, 0)
    (cluster.root / "after-cleanup.json").write_text(json.dumps(after, indent=2))
    cluster.stop()
    cluster.start()
    cluster.ready()
    after_restart = cluster.wait_rows(users + load_users, 0)
    (cluster.root / "after-restart.json").write_text(json.dumps(after_restart, indent=2))
    return dict(replicas=len(cluster.nodes), initial_conversations_per_replica=72,
                cross_leader_pairs=len(pairs), churn_requests=64, churn_seconds=seconds,
                all_replicas_clean=True, restart_clean=True)


class Client:
    def __init__(self, cluster, node, uid):
        self.cluster, self.buffer = cluster, ""
        self.sock = socket.create_connection(("127.0.0.1", node["ports"]["tcp"]), timeout=5)
        cluster.clients.append(self.sock)
        self.write("connect", "connect-" + uid,
                   dict(version=5, deviceId="compat-" + uid, deviceFlag=1, uid=uid, token="test"))
        reply = self.wait(lambda e: e.get("id") == "connect-" + uid)
        assert reply.get("result", {}).get("reasonCode") == 1, reply

    def write(self, method, ident, params):
        data = dict(jsonrpc="2.0", method=method, params=params)
        if method != "recvack":
            data["id"] = ident
        self.sock.sendall(json.dumps(data).encode())

    def wait(self, predicate, seconds=12):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self.buffer = self.buffer.lstrip()
            try:
                event, count = json.JSONDecoder().raw_decode(self.buffer)
            except json.JSONDecodeError:
                self.sock.settimeout(max(0.01, deadline - time.monotonic()))
                data = self.sock.recv(65536)
                if not data:
                    raise EOFError("physical socket closed")
                self.buffer += data.decode()
                continue
            self.buffer = self.buffer[count:]
            self.cluster.record("client", event=event)
            if event.get("method") == "recv":
                p = event["params"]
                self.write("recvack", "", dict(messageId=p["messageId"], messageSeq=p["messageSeq"]))
            if predicate(event):
                return event
        raise TimeoutError("matching client event not received")


def sessions(cluster):
    cluster.ready()
    uid = "compat-receiver"
    slot_id = zlib.crc32(uid.encode()) % cluster.slots
    info = cluster.call(cluster.nodes[0], "/cluster/info")
    slot = next(s for s in info["slots"] if s.get("id", 0) == slot_id)
    owner = next(n for n in cluster.nodes if n["id"] not in slot["replicas"])
    victim = next(n for n in cluster.nodes if n["id"] == slot["leader"])
    receiver, sender = Client(cluster, owner, uid), Client(cluster, owner, "compat-sender")
    payload = base64.b64encode(b'{"type":1,"content":"backport session recovery"}').decode()

    def send(label, key):
        for attempt in range(6):
            ident = f"{label}-{attempt}"
            sender.write("send", ident, dict(channelId=uid, channelType=1, clientMsgNo=key, payload=payload))
            reply = sender.wait(lambda e: e.get("id") == ident)
            result = reply.get("result", {})
            if result.get("reasonCode") == 1:
                return result
            assert result.get("reasonCode") in (10, 18, 22), reply
            time.sleep(0.5)
        raise AssertionError(f"send did not recover: {reply}")

    before = send("before", "before-failover")
    receiver.wait(lambda e: e.get("method") == "recv" and
                  e.get("params", {}).get("messageId") == before["messageId"])
    victim["process"].kill()
    victim["process"].wait(timeout=5)
    cluster.record("leader_killed", node=victim["id"], socket_owner=owner["id"], slot=slot_id)

    def new_leader():
        info = cluster.call(owner, "/cluster/info")
        current = next(s for s in info["slots"] if s.get("id", 0) == slot_id)
        assert current.get("leader", 0) not in (0, victim["id"], owner["id"]), current
        assert not current.get("migrateFrom") and not current.get("migrateTo"), current
        return current["leader"]
    leader = eventually(new_leader)
    after = send("after", "after-failover")
    receiver.wait(lambda e: e.get("method") == "recv" and
                  e.get("params", {}).get("messageId") == after["messageId"])
    retry = send("retry", "after-failover")
    assert (retry["messageId"], retry["messageSeq"]) == (after["messageId"], after["messageSeq"])
    receiver.write("ping", "still-open", {})
    assert "error" not in receiver.wait(lambda e: e.get("id") == "still-open")
    return dict(previous_leader=victim["id"], new_leader=leader, physical_owner=owner["id"],
                same_socket_received_after_failover=True, canonical_retry=True, ping_after_failover=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--scenario", required=True, choices=["cleanup", "sessions"])
    parser.add_argument("--nodes", type=int, choices=[1, 3], default=3, help="node count for cleanup")
    parser.add_argument("--keep-data", action="store_true")
    args = parser.parse_args()
    root, binary = args.output.resolve(), args.binary.resolve()
    root.mkdir(parents=True, exist_ok=False)
    result = dict(scenario=args.scenario, binary=str(binary),
                  binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(), result="fail")
    cluster = Cluster(binary, root, 4 if args.scenario == "sessions" else args.nodes,
                      16 if args.scenario == "sessions" else 64)
    try:
        cluster.start()
        result.update((sessions if args.scenario == "sessions" else cleanup)(cluster))
        result["result"] = "pass"
    except Exception as error:
        result["error"] = repr(error)
        raise
    finally:
        for sock in cluster.clients + list(cluster.reservations.values()):
            sock.close()
        cluster.stop()
        for node in cluster.nodes:
            Path(node["socket_path"]).unlink(missing_ok=True)
            if not args.keep_data and (node["folder"] / "data").exists():
                shutil.rmtree(node["folder"] / "data")
        (root / "summary.json").write_text(json.dumps(result, indent=2))
        print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
