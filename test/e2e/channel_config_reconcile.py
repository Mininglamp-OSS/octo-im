#!/usr/bin/env python3
"""Three real processes, persisted data, HTTP assertions; no manual channel wake.

Build main.go, then run with --binary /path/to/server --output /new/directory.
--scenario upgrade --legacy-binary /path/to/tag-server tests old data upgrades.
--scenario repair --legacy-binary /path/to/f00056b0 tests restart from the bug.
--expect-stale with --scenario expand verifies the unfixed negative control.
Only this run's processes and data directories are removed on exit.
"""

import argparse
import base64
import json
import os
from pathlib import Path
import shutil
import socket
import statistics
import subprocess
import time
import urllib.error
import urllib.request
import zlib


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--legacy-binary", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--scenario", choices=["expand", "fresh", "upgrade", "repair"], default="expand")
    parser.add_argument("--expect-stale", action="store_true")
    parser.add_argument("--keep-data", action="store_true")
    parser.add_argument("--failover", action="store_true", help="stop a channel leader and require complete reads without writes")
    parser.add_argument("--no-post-expansion-send", action="store_true", help="require maintenance without any send after joining")
    parser.add_argument("--recovery-seconds", type=float, default=45, help="bounded maintenance convergence window")
    args = parser.parse_args()
    if args.scenario in ("upgrade", "repair") and not args.legacy_binary:
        parser.error("upgrade/repair requires --legacy-binary")
    if args.no_post_expansion_send and (args.expect_stale or args.scenario == "repair"):
        parser.error("negative controls require the send that creates their stale configuration")
    if args.expect_stale and args.scenario != "expand":
        parser.error("--expect-stale requires --scenario expand")
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    events, nodes, expected = [], [], {}
    reservations = {}
    channels = [f"config-reconcile-{i}" for i in range(12)]

    def record(kind, **data):
        entry = {"time": time.time(), "kind": kind, **data}
        events.append(entry)
        with (root / "events.jsonl").open("a") as stream:
            stream.write(json.dumps(entry) + "\n")

    def port():
        # Keep future joiners' ports bound until launch. Closing immediately
        # lets an outgoing connection (or another fixture) reuse them while
        # the first node is creating its initial channels.
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        number = sock.getsockname()[1]
        reservations[number] = sock
        return number

    for index in range(3):
        folder = root / f"node{index}"
        folder.mkdir()
        nodes.append({"id": 1001 + index, "dir": folder, "process": None,
                      "ports": {key: port() for key in ["http", "cluster", "tcp", "ws", "intranet", "manager", "demo"]}})

    def call(node, path, payload=None):
        request = urllib.request.Request(
            f"http://127.0.0.1:{node['ports']['http']}{path}",
            data=None if payload is None else json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"})
        start = time.monotonic()
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                status, raw = response.status, response.read()
        except urllib.error.HTTPError as error:
            status, raw = error.code, error.read()
        result = {"status": status, "body": json.loads(raw), "seconds": time.monotonic() - start}
        record("http", node=node["id"], path=path, response=result)
        return result

    def start(node, binary, joining=False):
        ports = node["ports"]
        cfg = {
            "mode": "release", "rootDir": str(node["dir"] / "data"),
            "addr": f"tcp://127.0.0.1:{ports['tcp']}", "httpAddr": f"127.0.0.1:{ports['http']}",
            "wsAddr": f"ws://127.0.0.1:{ports['ws']}",
            "intranet": {"tcpAddr": f"127.0.0.1:{ports['intranet']}"}, "tokenAuthOn": False,
            "logger": {"level": 1, "dir": str(node["dir"] / "logs")},
            "manager": {"on": False, "addr": f"127.0.0.1:{ports['manager']}"},
            "demo": {"on": False, "addr": f"127.0.0.1:{ports['demo']}"},
            "conversation": {"on": True}, "db": {"shardNum": 2, "slotShardNum": 2},
            "jwt": {"secret": "config-reconcile-isolated-test-only"},
            "plugin": {"socketPath": f"/tmp/config-reconcile-{os.getpid()}-{node['id']}.sock"},
            "cluster": {"nodeId": node["id"], "addr": f"tcp://127.0.0.1:{ports['cluster']}",
                        "serverAddr": f"127.0.0.1:{ports['cluster']}", "apiUrl": f"http://127.0.0.1:{ports['http']}",
                        "slotCount": 64, "slotReplicaCount": 3, "channelReplicaCount": 3,
                        "channelReactorSubCount": 8, "reqTimeout": "2s"},
        }
        if args.scenario == "fresh":
            cfg["cluster"]["initNodes"] = [f"{n['id']}@127.0.0.1:{n['ports']['cluster']}" for n in nodes]
        elif joining:
            cfg["cluster"]["seed"] = f"1001@127.0.0.1:{nodes[0]['ports']['cluster']}"
        config_path = node["dir"] / "config.json"
        config_path.write_text(json.dumps(cfg, indent=2))
        env = {key: value for key, value in os.environ.items() if not key.startswith("WK_")}
        env["GOMAXPROCS"] = "4"
        for number in ports.values():
            reservation = reservations.pop(number, None)
            if reservation is not None:
                reservation.close()
        with (node["dir"] / "stdout.log").open("ab") as stream:
            node["process"] = subprocess.Popen([str(binary.resolve()), "--config", str(config_path), "--mode", "release"],
                                               cwd=node["dir"], stdout=stream, stderr=subprocess.STDOUT, env=env)
        record("start", node=node["id"], binary=str(binary), pid=node["process"].pid)

    def stop(group):
        for node in group:
            proc = node["process"]
            if proc is not None and proc.poll() is None:
                proc.terminate()
        for node in group:
            proc = node["process"]
            if proc is not None:
                try:
                    proc.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                node["process"] = None

    def eventually(check, seconds=30):
        deadline, last = time.monotonic() + seconds, None
        while time.monotonic() < deadline:
            try:
                check()
                return
            except (AssertionError, OSError, ValueError) as error:
                last = error
                time.sleep(0.1)
        raise AssertionError(f"did not converge in {seconds}s: {last}")

    def ready(group):
        def check():
            for node in group:
                if node["process"].poll() is not None:
                    raise RuntimeError(f"node {node['id']} exited; inspect its stdout.log")
                assert call(node, "/health")["status"] == 200
            response = call(nodes[0], "/cluster/allslot")
            assert response["status"] == 200, response
            slots = response["body"]["data"]
            assert len(slots) == 64
            if len(group) == 3:
                assert {slot["leader_id"] for slot in slots} == {1001, 1002, 1003}
        eventually(check)
        time.sleep(3)

    def send_round(payload):
        encoded = base64.b64encode(payload.encode()).decode()
        for channel in channels:
            response = call(nodes[0], "/message/send", {"from_uid": "sender", "channel_id": channel,
                            "channel_type": 2, "payload": encoded, "header": {"red_dot": 1}})
            assert response["status"] == 200, response
            expected.setdefault(channel, []).append((str(response["body"]["data"]["message_id"]), encoded))

    def read(node):
        response = call(node, "/conversation/sync", {"uid": "admin", "version": 0, "last_msg_seqs": "", "msg_count": 10})
        assert response["status"] == 200, response
        conversations = response["body"]
        assert len(conversations) == 12
        assert {conv["channel_id"] for conv in conversations} == set(channels)
        for conv in conversations:
            want = expected[conv["channel_id"]]
            assert conv["last_msg_seq"] == len(want), conv
            messages = sorted(conv["recents"], key=lambda message: message["message_seq"])
            assert [message["message_seq"] for message in messages] == list(range(1, len(want)+1)), conv
            assert [(str(message["message_id"]), message["payload"]) for message in messages] == want, conv
        return response["seconds"]

    def configs(require_converged):
        slots = call(nodes[0], "/cluster/allslot")["body"]["data"]
        owners = {slot["id"]: slot["leader_id"] for slot in slots}
        separated, stale, migrating = 0, 0, 0
        for channel in channels:
            owner_id = owners[zlib.crc32(channel.encode()) % 64]
            owner = next(node for node in nodes if node["id"] == owner_id)
            cfg = call(owner, f"/cluster/channels/{channel}/2/config")["body"]
            leader = next(node for node in nodes if node["id"] == cfg["learder_id"])
            runtime = call(leader, f"/cluster/channels/{channel}/2/localReplica")["body"]
            separated += owner_id != cfg["learder_id"]
            migration = bool(cfg.get("learners") or cfg.get("migrate_from") or cfg.get("migrate_to"))
            migrating += migration
            mismatch = runtime.get("running") and runtime["conf_version"] != cfg["conf_version"]
            stale += bool(mismatch)
            if require_converged:
                assert not migration and not mismatch, (channel, cfg, runtime)
                assert len(set(cfg["replicas"])) == 3, (channel, cfg)
                assert cfg["replica_max_count"] == 3, cfg
        if args.scenario != "fresh":
            assert separated > 0, "expansion test must separate metadata and message leaders"
        return {"separated": separated, "stale": stale, "migrating": migrating}

    def failover():
        # Choose a node that actually leads channels; random placement must not
        # let a fault test pass without exercising a channel leadership change.
        slots = call(nodes[0], "/cluster/allslot")["body"]["data"]
        owners = {slot["id"]: slot["leader_id"] for slot in slots}
        before = {}
        for channel in channels:
            owner = next(n for n in nodes if n["id"] == owners[zlib.crc32(channel.encode()) % 64])
            cfg = call(owner, f"/cluster/channels/{channel}/2/config")["body"]
            assert len(set(cfg["replicas"])) == 3 and not cfg.get("learners"), cfg
            before[channel] = cfg
        victim = max(nodes, key=lambda n: sum(c["learder_id"] == n["id"] for c in before.values()))
        affected = [channel for channel, cfg in before.items() if cfg["learder_id"] == victim["id"]]
        assert affected
        survivors = [n for n in nodes if n is not victim]
        record("leader_stopped", node=victim["id"], affected=affected, configs=before)
        stop([victim])
        def slots_recovered():
            for node in survivors:
                response = call(node, "/cluster/allslot")
                assert response["status"] == 200, response
                slots = response["body"]["data"]
                assert len(slots) == 64
                assert all(s["leader_id"] in {n["id"] for n in survivors} for s in slots), slots
        eventually(slots_recovered, seconds=args.recovery_seconds)
        record("slots_recovered")
        started = time.monotonic()
        def recovered():
            for node in survivors:
                read(node)
        # There is deliberately no control send, /start or manual wake here.
        eventually(recovered, seconds=args.recovery_seconds)
        recovery = time.monotonic() - started
        for _ in range(10):
            recovered()
        slots = call(survivors[0], "/cluster/allslot")["body"]["data"]
        owners = {slot["id"]: slot["leader_id"] for slot in slots}
        after = {}
        for channel in affected:
            owner = next(n for n in survivors if n["id"] == owners[zlib.crc32(channel.encode()) % 64])
            cfg = call(owner, f"/cluster/channels/{channel}/2/config")["body"]
            assert cfg["learder_id"] != victim["id"] and cfg["term"] > before[channel]["term"], cfg
            assert cfg["conf_version"] > before[channel]["conf_version"], cfg
            assert set(cfg["replicas"]) == set(before[channel]["replicas"]), cfg
            after[channel] = cfg
        result = {"victim": victim["id"], "affected": affected, "recovered_without_write": True,
                  "recovery_after_slots_seconds": recovery, "complete_reads_after_recovery": 22,
                  "before_configs": before, "after_configs": after}
        (root / "failover-summary.json").write_text(json.dumps(result, indent=2))
        record("failover_verified", **result)
        return result

    try:
        initial = nodes if args.scenario == "fresh" else nodes[:1]
        initial_binary = args.legacy_binary if args.scenario in ("upgrade", "repair") else args.binary
        for node in initial:
            start(node, initial_binary)
        ready(initial)
        for channel in channels:
            assert call(nodes[0], "/channel", {"channel_id": channel, "channel_type": 2,
                        "subscribers": ["admin", "sender"]})["status"] == 200
        send_round("before-expansion")
        eventually(lambda: read(nodes[0]))
        if args.scenario != "fresh":
            if args.scenario == "upgrade":
                stop(initial)
                start(nodes[0], args.binary)
                ready(initial)
                eventually(lambda: read(nodes[0]))
            join_binary = initial_binary if args.scenario == "repair" else args.binary
            for node in nodes[1:]:
                start(node, join_binary, joining=True)
            ready(nodes)
            if not args.no_post_expansion_send:
                send_round("after-expansion")
        if args.expect_stale or args.scenario == "repair":
            time.sleep(3)
            broken = configs(False)
            assert broken["stale"] > 0 and broken["migrating"] > 0, broken
            for node in nodes:
                response = call(node, "/conversation/sync", {"uid": "admin", "version": 0, "last_msg_seqs": "", "msg_count": 10})
                assert response["status"] == 503, response
            record("negative_control", **broken)
            if args.expect_stale:
                print(json.dumps({"result": "expected regression reproduced", **broken}), flush=True)
                return
            stop(nodes)
            for index, node in enumerate(nodes):
                start(node, args.binary, joining=index > 0)
            ready(nodes)
        recovery_start = time.monotonic()
        def complete():
            configs(True)
            for node in nodes:
                read(node)
        eventually(complete, seconds=args.recovery_seconds)
        recovery_seconds = time.monotonic() - recovery_start
        latencies = [read(node) for _ in range(30) for node in nodes]
        result = {"scenario": args.scenario, "result": "pass", "channels": 12,
                  "messages_per_channel": len(expected[channels[0]]), "successful_reads": len(latencies),
                  "recovery_check_seconds": recovery_seconds, "read_p50_ms": statistics.median(latencies)*1000,
                  "read_p95_ms": sorted(latencies)[int(len(latencies)*0.95)]*1000,
                  "read_max_ms": max(latencies)*1000, **configs(True)}
        if args.failover:
            result["failover"] = failover()
        (root / "summary.json").write_text(json.dumps(result, indent=2))
        record("verified", **result)
        print(json.dumps(result), flush=True)
    finally:
        stop(nodes)
        for reservation in reservations.values():
            reservation.close()
        if not args.keep_data:
            for node in nodes:
                data = node["dir"] / "data"
                if data.exists():
                    shutil.rmtree(data)


if __name__ == "__main__":
    main()
