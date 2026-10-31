#!/usr/bin/env python3
"""Compare exported online users with live sockets and online API after failover.

Uses the isolated three-process/raw-WebSocket harness in heartbeat_failover.py.
No existing service is touched. Only clients disconnected by the killed node
are reconnected; heartbeat timeouts do not cause a reconnect in this fixture.
"""

import argparse
import base64
from collections import Counter
import hashlib
import hmac
import json
from pathlib import Path
import threading
import time
import urllib.error
import urllib.request
import zlib

from heartbeat_failover import Client, Run


class AlternateDeviceClient(Client):
    def send_frame(self, opcode, payload):
        if opcode == 1:
            request = json.loads(payload)
            if request.get("method") == "connect":
                request["params"].update(deviceFlag=2, deviceId="online-count-second-" + self.uid)
                payload = json.dumps(request).encode()
        return super().send_frame(opcode, payload)


class CountRun(Run):
    def __init__(self, args):
        super().__init__(args)
        (self.root / "online_count_failover.used.py").write_bytes(Path(__file__).read_bytes())
        (self.root / "heartbeat_failover.py").write_bytes(Path(__file__).with_name("heartbeat_failover.py").read_bytes())
        self.samples = []
        self.stopped = threading.Event()
        self.prober = None
        (self.root / "metrics").mkdir()
        for node in self.nodes:
            path = node["folder"] / "config.json"
            config = json.loads(path.read_text())
            config["manager"]["on"] = True
            path.write_text(json.dumps(config, indent=2))
        manifest_path = self.root / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest.update(scenario=args.scenario,
                        workload="authenticated connections and 1 Hz heartbeats",
                        reconnect="only sockets owned by killed node, after election",
                        online_count_script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                        settle_s=args.settle, expected=args.expect,
                        extra_device=args.extra_device,
                        sample_period_s=2, user_handler_timeout_s=300,
                        handler_cleanup_period_s=120)
        manifest_path.write_text(json.dumps(manifest, indent=2))

    def call(self, node, path, payload=None):
        headers = {"Content-Type": "application/json"}
        if path.endswith("/migrate"):
            # This secret belongs only to the local cluster created by Run.
            encode = lambda value: base64.urlsafe_b64encode(value).rstrip(b"=")
            claims = dict(username="____manager", exp=int(time.time()) + 300)
            body = encode(b'{"alg":"HS256","typ":"JWT"}') + b"." + encode(json.dumps(claims).encode())
            signature = hmac.new(b"heartbeat-failover-isolated-test-only", body, hashlib.sha256).digest()
            headers["Authorization"] = "Bearer " + (body + b"." + encode(signature)).decode()
        request = urllib.request.Request(
            node["url"] + path,
            data=None if payload is None else json.dumps(payload).encode(),
            headers=headers)
        try:
            with self.http.open(request, timeout=2) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            self.record("http_error", node=node["id"], path=path,
                        status=error.code, body=error.read().decode(errors="replace"))
            raise

    def active(self):
        with self.lock:
            return [c for c in self.clients if not c.closing and c.thread.is_alive()
                    and c.connect_response is not None
                    and c.connect_response.get("result", {}).get("reasonCode") == 1]

    def start_probes(self):
        def loop():
            sequence = 0
            while not self.stopped.is_set():
                for client in self.active():
                    try:
                        client.probe(sequence)
                        client.expire()
                    except OSError as error:
                        self.record("probe_send_error", uid=client.uid, error=repr(error))
                sequence += 1
                self.stopped.wait(1)
        self.prober = threading.Thread(target=loop, daemon=True)
        self.prober.start()

    def stop(self):
        self.stopped.set()
        if self.prober:
            self.prober.join(timeout=3)
        super().stop()

    def snapshot(self, stage, all_online=False):
        clients = self.active()
        live_uids = sorted({c.uid for c in clients})
        sample = dict(stage=stage, live_uid_count=len(live_uids),
                      live_uids=live_uids, live_socket_count=len(clients),
                      physical_owners=dict(Counter(c.node["id"] for c in clients)),
                      nodes={}, online={})
        for node in self.nodes:
            result = {}
            sample["nodes"][str(node["id"])] = result
            proc = node["process"]
            result["pid"] = proc.pid if proc else None
            if proc is None or proc.poll() is not None:
                result["unavailable"] = "process exited"
                continue
            try:
                url = f"http://{node['host']}:{node['ports']['manager']}/metrics"
                with self.http.open(url, timeout=2) as response:
                    raw = response.read().decode()
                artifact = f"metrics/{len(self.samples):04d}-{node['id']}.prom"
                (self.root / artifact).write_text(raw)
                result["raw"] = artifact
                for line in raw.splitlines():
                    if line.startswith("#") or not line.strip():
                        continue
                    name = line.split("{", 1)[0].split()[0]
                    if name in ("app_online_user_count_total", "app_conn_count_total"):
                        result[name] = int(float(line.split()[-1]))
                assert "app_online_user_count_total" in result, "online metric missing"
                result["varz"] = self.call(node, "/varz")
            except Exception as error:
                result["error"] = repr(error)
        values = [node.get("app_online_user_count_total") for node in sample["nodes"].values()]
        sample["metric_sum"] = sum(values) if all(v is not None for v in values) else None
        sample["available_metric_sum"] = sum(v for v in values if v is not None)
        for node in self.nodes if all_online else self.nodes[1:2]:
            try:
                response = self.call(node, "/user/onlinestatus", sorted(self.expected_uids))
                # The API serializes a nil result as JSON null when no UID is online.
                online = sorted({row["uid"] for row in response or [] if row.get("online") == 1})
                sample["online"][str(node["id"])] = dict(count=len(online), uids=online,
                                                           matches_live=online == live_uids,
                                                           response_was_null=response is None)
            except Exception as error:
                sample["online"][str(node["id"])] = dict(error=repr(error))
        try:
            slots = self.slots(self.nodes[1])
            sample["slots"] = slots
            sample["expected_users_by_leader"] = dict(Counter(slots[zlib.crc32(uid.encode()) % 64]
                                                              for uid in live_uids))
        except Exception as error:
            sample["slots_error"] = repr(error)
        sample.update(t=round(time.monotonic() - self.started, 6))
        self.samples.append(sample)
        with (self.root / "samples.jsonl").open("a") as stream:
            stream.write(json.dumps(sample) + "\n")
        print(json.dumps({key: sample[key] for key in ("t", "stage", "live_uid_count", "metric_sum")}), flush=True)
        return sample

    def observe(self, stage, seconds):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            self.snapshot(stage)
            self.stopped.wait(min(2, max(0, until - time.monotonic())))


def exercise(run):
    for node in run.nodes:
        run.start(node)
    initial = run.ready()
    (run.root / "initial-slots.json").write_text(json.dumps(initial, indent=2))
    owners = run.nodes if run.args.scenario == "crash" else run.nodes[1:]
    run.expected_uids = set()
    for owner in owners:
        for leader in (1001, 1002, 1003):
            matched = 0
            for index in range(100000):
                uid = f"online-count-{owner['id']}-{leader}-{index}"
                slot = zlib.crc32(uid.encode()) % 64
                if initial[slot] != leader:
                    continue
                Client(run, owner, uid, slot, leader)
                run.expected_uids.add(uid)
                matched += 1
                if matched == run.args.clients_per_group:
                    break
            assert matched == run.args.clients_per_group
    first_device = None
    if run.args.extra_device:
        first_device = next(c for c in run.active()
                            if c.node["id"] == 1002 and c.original_leader == 1001)
        AlternateDeviceClient(run, run.nodes[2], first_device.uid,
                              first_device.slot, first_device.original_leader)
        run.record("extra_device_connected", uid=first_device.uid, owner=1003)
    run.start_probes()
    run.observe("warmup", run.args.crash_after)
    before = run.snapshot("before", all_online=True)
    assert before["metric_sum"] == len(run.expected_uids), before
    assert all(row.get("matches_live") for row in before["online"].values()), before

    if run.args.scenario == "crash":
        victim = run.nodes[0]
        lost = [c for c in run.active() if c.node == victim]
        victim["process"].kill()
        victim["process"].wait(timeout=5)
        killed_at = time.monotonic()
        run.record("leader_killed", node=1001, lost_uids=sorted(c.uid for c in lost))
        # Keep the original downtime, even when reconnects take additional time.
        run.observe("election", 8)
        slots = run.slots(run.nodes[1])
        assert all(slots[s] not in (0, 1001) for s in initial if initial[s] == 1001), slots
        for i, client in enumerate(lost):
            client.close()
            owner = run.nodes[1 + i % 2]
            Client(run, owner, client.uid, client.slot, client.original_leader)
        run.record("victim_clients_reconnected", count=len(lost))
        run.observe("victim_down", max(0, run.args.downtime - (time.monotonic() - killed_at)))
        run.start(victim)
        run.record("victim_restart", node=1001)
    else:
        moved = sorted({c.slot for c in run.active() if c.original_leader == 1001})
        observed = []
        for slot in moved:
            reply = run.call(run.nodes[0], f"/cluster/slots/{slot}/migrate",
                             dict(migrate_from=1001, migrate_to=1002))
            run.record("slot_migrate_requested", slot=slot, source=1001, target=1002, reply=reply)
            # Automatic slot balancing may return the slot within a fraction
            # of a second. Observe the actual handoff at 50 ms resolution;
            # metrics continue using the ordinary 2 s collection period.
            until = time.monotonic() + 30
            while time.monotonic() < until:
                slots = run.slots(run.nodes[1])
                if slots[slot] == 1002:
                    observed.append(slot)
                    run.record("slot_target_observed", slot=slot, target=1002)
                    break
                run.stopped.wait(0.05)
            assert slots[slot] == 1002, (slot, slots)
        assert observed == moved, (observed, moved)
        run.record("slot_migrate_completed", slots=moved, users=sum(c.original_leader == 1001 for c in run.active()))

    run.observe("settling", run.args.settle)
    after = run.snapshot("after", all_online=True)
    assert after["live_uid_count"] == len(run.expected_uids), after
    assert all(row.get("matches_live") for row in after["online"].values()), after
    assert after["metric_sum"] is not None, after
    summary = dict(before=before, after=after,
                   excess=after["metric_sum"] - after["live_uid_count"],
                   socket_errors=[r for r in run.rows if r["kind"] == "socket_error"],
                   application_timeouts=sum(r["kind"] == "probe_timeout" and r["probe"] == "application"
                                            for r in run.rows))
    if run.args.disconnect:
        if first_device is not None:
            first_device.close()
            run.observe("one_device_closed", 4)
            remaining = run.snapshot("one_device_closed_final", all_online=True)
            summary["one_device_closed"] = remaining
            assert remaining["live_uid_count"] == len(run.expected_uids), remaining
            assert all(row.get("matches_live") for row in remaining["online"].values()), remaining
            if run.args.expect == "accurate":
                assert remaining["metric_sum"] == len(run.expected_uids), remaining
        for client in run.active():
            client.close()
        run.observe("disconnected", 8)
        summary["disconnected"] = run.snapshot("disconnected_final", all_online=True)
    (run.root / "summary.json").write_text(json.dumps(summary, indent=2))
    if run.args.expect == "overcount":
        assert summary["excess"] > 0, "online overcount did not reproduce"
    elif run.args.expect == "accurate":
        assert summary["excess"] == 0, "online metric differs from live UID count"
        expected = after["expected_users_by_leader"]
        assert all(row["app_online_user_count_total"] == expected.get(int(node), 0)
                   for node, row in after["nodes"].items()), after
        if "disconnected" in summary:
            assert summary["disconnected"]["metric_sum"] == 0, summary["disconnected"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scenario", choices=("crash", "transfer"), default="crash")
    parser.add_argument("--clients-per-group", type=int, default=14)
    parser.add_argument("--crash-after", type=float, default=5)
    parser.add_argument("--downtime", type=float, default=20)
    parser.add_argument("--settle", type=float, default=30)
    parser.add_argument("--expect", choices=("observe", "overcount", "accurate"), default="observe")
    parser.add_argument("--disconnect", action="store_true")
    parser.add_argument("--extra-device", action="store_true")
    args = parser.parse_args()
    args.after_restart = args.settle
    if args.clients_per_group < 1 or args.crash_after < 2 or args.downtime < 12 or args.settle < 5:
        parser.error("need positive client count, warmup, election downtime, and settling time")
    run = CountRun(args)
    try:
        exercise(run)
    except Exception as error:
        run.record("failure", error=repr(error))
        raise
    finally:
        run.stop()


if __name__ == "__main__":
    main()
