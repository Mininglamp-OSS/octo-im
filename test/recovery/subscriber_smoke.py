#!/usr/bin/env python3
"""Finite, cgroup-contained three-node subscriber backlog recovery smoke test.

No Docker/browser dependencies. Run under the limits documented in
docs/recovery/subscriber-backlog.md. The script only stops its own child PIDs.
"""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request


def cgroup_path():
    relative = Path('/proc/self/cgroup').read_text().split('0::', 1)[1].strip()
    return Path('/sys/fs/cgroup') / relative.lstrip('/')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--port', type=int, default=24100)
    args = parser.parse_args()
    cg = cgroup_path()
    limit = (cg / 'memory.max').read_text().strip()
    if limit == 'max' or int(limit) > 4 * 1024**3:
        raise SystemExit('Run in a cgroup with MemoryMax <= 4 GiB')
    if (cg / 'memory.swap.max').read_text().strip() != '0':
        raise SystemExit('Run with MemorySwapMax=0')
    out = Path(args.output).resolve()
    out.mkdir(parents=True, exist_ok=True)
    filesystem = subprocess.check_output(['stat', '-f', '-c', '%T', str(out)], text=True).strip()
    if filesystem in ('tmpfs', 'ramfs'):
        raise SystemExit('Use a disk-backed output directory; Raft WAL preallocation fills tmpfs')
    binary = str(Path(args.binary).resolve())
    report = {'concurrency': 2, 'nodes': 3, 'workers_per_node': 2,
              'groups_per_burst': 12, 'members_per_group': 8, 'rounds': []}
    children = []
    logs = []
    samples = []
    # Fail before starting if any selected local port is already in use.
    for node in range(1, 4):
        for offset in (0, 10, 20, 30):
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', args.port + offset + node))

    def request(path, payload=None, node=1, timeout=7):
        req = urllib.request.Request(
            f'http://127.0.0.1:{args.port + node}{path}',
            data=None if payload is None else json.dumps(payload).encode(),
            headers={'Content-Type': 'application/json'})
        start = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                status, body = response.status, response.read()
        except urllib.error.HTTPError as err:
            status, body = err.code, err.read()
        elapsed = time.monotonic() - start
        result = json.loads(body)
        samples.append({'path': path, 'status': status, 'elapsed_s': elapsed})
        return status, result, elapsed

    def start_nodes():
        for node in range(1, 4):
            root = out / f'node{node}'
            root.mkdir(exist_ok=True)
            cfg = root / 'config.yaml'
            cfg.write_text(f'''
mode: release
addr: tcp://127.0.0.1:{args.port+10+node}
httpAddr: 127.0.0.1:{args.port+node}
wsAddr: ws://127.0.0.1:{args.port+20+node}
rootDir: {root}
pprofOn: false
tokenAuthOn: false
auth:
  kind: none
manager:
  on: false
demo:
  on: false
db:
  shardNum: 2
  slotShardNum: 2
  memTableSize: 1048576
conversation:
  on: true
subscriberRecovery:
  enabled: true
  workers: 2
  maxPending: 1024
  interval: 100ms
  timeout: 5s
cluster:
  nodeId: {node}
  addr: tcp://127.0.0.1:{args.port+30+node}
  apiUrl: http://127.0.0.1:{args.port+node}
  slotCount: 8
  slotReplicaCount: 3
  channelReplicaCount: 3
  initNodes:
    - 1@127.0.0.1:{args.port+31}
    - 2@127.0.0.1:{args.port+32}
    - 3@127.0.0.1:{args.port+33}
plugin:
  socketPath: ./plugin.sock
''')
            log = (root / f'process-{len(logs)}.log').open('wb')
            logs.append(log)
            env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='384MiB')
            children.append(subprocess.Popen([binary, '--config', str(cfg)],
                                             cwd=root, stdout=log,
                                             stderr=subprocess.STDOUT, env=env))
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if any(p.poll() is not None for p in children):
                raise RuntimeError('node exited; inspect node process logs')
            try:
                if all(request('/channel/subscriber_recovery', node=n)[0] == 200
                       for n in range(1, 4)):
                    # API listeners start before slot election has completed.
                    statuses = [request('/channel/subscriber_operation', {
                        'channel_id': f'readiness-{i}', 'channel_type': 2,
                        'operation_id': 'missing'})[0] for i in range(16)]
                    if all(status == 404 for status in statuses):
                        return
            except (OSError, ValueError):
                pass
            time.sleep(.3)
        raise RuntimeError('cluster did not become ready')

    def stop_nodes(crash=False):
        for process in children:
            if process.poll() is None:
                process.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
        for process in children:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        children.clear()

    members = [f'recovery-user-{i}' for i in range(8)]
    expected = {uid: set() for uid in members}

    def submit_many(items):
        def submit(item):
            path, body = item
            status, result, elapsed = request(path, body)
            if status != 200 or result.get('data', {}).get('state') not in ('pending', 'complete'):
                raise AssertionError((path, status, result))
            return {'body': body, 'receipt': result['data'], 'elapsed_s': elapsed}
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            return list(pool.map(submit, items))

    def drain(receipts, start):
        first_success = None
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            pending = 0
            for node in range(1, 4):
                status, result, _ = request('/channel/subscriber_recovery', node=node)
                assert status == 200, result
                pending += result['data']['pending_units']
            status, _, elapsed = request('/conversation/channels', {'uid': members[0]})
            if status == 200 and elapsed < 1 and first_success is None:
                first_success = time.monotonic() - start
            if pending == 0:
                for item in receipts:
                    r = item['receipt']
                    status, result, _ = request('/channel/subscriber_operation', {
                        'channel_id': r['channel_id'], 'channel_type': 2,
                        'operation_id': r['operation_id']})
                    assert status == 200 and result['data']['state'] == 'complete', result
                return {'first_successful_probe_s': first_success,
                        'drain_observed_s': time.monotonic() - start}
            time.sleep(.1)
        raise AssertionError('backlog did not drain within 120 seconds')

    def consistency(start):
        for uid in members:
            status, result, _ = request('/conversation/channels', {'uid': uid})
            assert status == 200, result
            channels = {r['channel_id'] for r in result if r['channel_type'] == 2}
            assert channels == expected[uid], (uid, channels, expected[uid])
        return time.monotonic() - start

    try:
        start_nodes()
        create = [('/channel', {'channel_id': f'recovery-group-{i}', 'channel_type': 2,
                               'operation_id': f'create-{i}', 'subscribers': members})
                  for i in range(12)]
        rows = submit_many(create)
        for uid in members:
            expected[uid].update(f'recovery-group-{i}' for i in range(12))
        start = time.monotonic()
        recovery = drain(rows, start)
        recovery['consistent_observed_s'] = consistency(start)
        latencies = sorted(r['elapsed_s'] for r in rows)
        report['rounds'].append({'name': 'create burst', 'request_p95_s': latencies[int(.95*(len(latencies)-1))], **recovery})

        retried = submit_many(create)
        assert [r['receipt']['version'] for r in retried] == [r['receipt']['version'] for r in rows]
        status, _, _ = request('/channel/subscriber_remove', {
            'channel_id': 'recovery-group-0', 'channel_type': 2,
            'operation_id': 'create-0', 'subscribers': members})
        assert status == 409, status
        blocked = submit_many([('/channel/blacklist_add', {
            'channel_id': 'recovery-group-0', 'channel_type': 2,
            'operation_id': 'block-0', 'uids': members[:2]})])
        for uid in members[:2]:
            expected[uid].remove('recovery-group-0')
        start = time.monotonic()
        drain(blocked, start)
        consistency(start)
        unblocked = submit_many([('/channel/blacklist_remove', {
            'channel_id': 'recovery-group-0', 'channel_type': 2,
            'operation_id': 'unblock-0', 'uids': members[:2]})])
        for uid in members[:2]:
            expected[uid].add('recovery-group-0')
        start = time.monotonic()
        drain(unblocked, start)
        consistency(start)
        report['rounds'].append({'name': 'stable retries, input conflict and blacklist unblock', 'passed': True})

        leaves = [('/channel/subscriber_remove', {'channel_id': f'recovery-group-{i}',
                   'channel_type': 2, 'operation_id': f'leave-{i}', 'subscribers': members[:4]}) for i in range(12)]
        rejoins = [('/channel/subscriber_add', dict(body, operation_id=f'rejoin-{i}'))
                   for i, (_, body) in enumerate(leaves)]
        churn = submit_many(leaves) + submit_many(rejoins)
        start = time.monotonic()
        recovery = drain(churn, start)
        recovery['consistent_observed_s'] = consistency(start)
        report['rounds'].append({'name': 'leave/rejoin with old cleanup pending', **recovery})

        resets = [('/channel/subscriber_add', {'channel_id': f'recovery-group-{i}',
                   'channel_type': 2, 'operation_id': f'reset-{i}', 'reset': 1,
                   'subscribers': members[4:]}) for i in range(12)]
        rows = submit_many(resets)
        for uid in members[:4]:
            expected[uid].clear()
        before = sum(request('/channel/subscriber_recovery', node=n)[1]['data']['pending_units'] for n in range(1, 4))
        assert before > 0, 'restart must interrupt a nonempty backlog'
        stop_nodes(crash=True)
        start = time.monotonic()
        start_nodes()
        recovery = drain(rows, start)
        recovery['consistent_observed_s'] = consistency(start)
        report['rounds'].append({'name': 'crash/restart with accepted pending reset', 'pending_before_crash': before, **recovery})

        removes = [('/channel/subscriber_remove_all', {'channel_id': f'recovery-group-{i}',
                    'channel_type': 2, 'operation_id': f'remove-all-{i}'}) for i in range(12)]
        rows = submit_many(removes)
        for uid in members:
            expected[uid].clear()
        start = time.monotonic()
        recovery = drain(rows, start)
        recovery['consistent_observed_s'] = consistency(start)
        report['rounds'].append({'name': 'remove-all drain and final consistency', **recovery})
        report['passed'] = True
    except BaseException as exc:
        report['passed'] = False
        report['error'] = repr(exc)
        raise
    finally:
        stop_nodes()
        for log in logs:
            log.close()
        report['memory_peak_bytes'] = int((cg / 'memory.peak').read_text())
        report['memory_events'] = (cg / 'memory.events').read_text()
        (out / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
        (out / 'requests.json').write_text(json.dumps(samples, indent=2))
        print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)


if __name__ == '__main__':
    main()
