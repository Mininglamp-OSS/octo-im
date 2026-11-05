#!/usr/bin/env python3
"""Isolated real-process regression: lose 1002, join empty 1004, lose 1001.

Every successful SDK ACK is matched by ID, sequence, key and payload through
each surviving API. Only successful SDK SENDACKs enter the persistence ledger.

From the repository root, using a new output directory:
  go build -o /tmp/replica-server main.go
  go build -o /tmp/replica-sdk test/e2e/offline_replica_sdk.go
  python3 test/e2e/offline_replica_replacement.py --binary /tmp/replica-server \
    --sdk /tmp/replica-sdk --out /tmp/replica-check/case --fixed

Omit --fixed for the negative control; result.json records acceptance_pass.
--write-during-join exercises ongoing writes, --quiet-join leaves channels idle,
--return-retired restarts the failed node with its old database, and
--replace-joiner-data preserves then replaces the new node's data directory.
The fixture keeps its databases and logs for offline auditing.
Upgrade all configuration voters before exercising replacement joins.
"""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import queue
import signal
import socket
import subprocess
import threading
import time
import traceback
import urllib.error
import urllib.request
import zlib

HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))

def save(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--binary', required=True)
    p.add_argument('--sdk', required=True)
    p.add_argument('--out', required=True)
    p.add_argument('--observe', type=int, default=90)
    p.add_argument('--channels', type=int, default=12)
    p.add_argument('--port-base', type=int, default=32100)
    p.add_argument('--fixed', action='store_true')
    p.add_argument('--initial-messages', type=int, default=4)
    p.add_argument('--write-during-join', action='store_true')
    p.add_argument('--quiet-join', action='store_true')
    p.add_argument('--return-retired', action='store_true')
    p.add_argument('--replace-joiner-data', action='store_true')
    a = p.parse_args()
    out = Path(a.out).resolve()
    out.mkdir(parents=True, exist_ok=False)
    (out/'logs').mkdir()
    processes, streams, clients = {}, [], []
    initial = [1001, 1002, 1003]
    ports = {n: {k: a.port_base+(n-1001)*10+i for i,k in enumerate(['api','tcp','ws','peer','manager'])} for n in [*initial,1004]}
    for row in ports.values():
        for port in row.values():
            with socket.socket() as s: s.bind(('127.0.0.1',port))
    result = dict(args=vars(a), ports=ports, binary_sha256=hashlib.sha256(Path(a.binary).read_bytes()).hexdigest(), started=time.time(), complete=False)
    journal = (out/'events.jsonl').open('w', buffering=1)
    operations = (out/'operations.jsonl').open('w', buffering=1)
    io_lock = threading.Lock()
    def emit(event, **data):
        row = dict(time=time.time(),event=event,**data)
        journal.write(json.dumps(row)+'\n')
        print(json.dumps(row),flush=True)
    def request(node, path, body=None, timeout=4):
        start=time.monotonic()
        try:
            req=urllib.request.Request(f'http://127.0.0.1:{ports[node]["api"]}{path}',data=None if body is None else json.dumps(body).encode(),headers={'Content-Type':'application/json'})
            with HTTP.open(req,timeout=timeout) as r:
                text=r.read().decode()
                ans=dict(status=r.status,body=json.loads(text) if text else None)
        except urllib.error.HTTPError as e: ans=dict(status=e.code,error=e.read().decode(errors='replace'))
        except Exception as e: ans=dict(error=str(e))
        with io_lock: operations.write(json.dumps(dict(time=time.time(),node=node,path=path,request=body,response=ans,seconds=time.monotonic()-start))+'\n')
        return ans
    def start(node,retained=False):
        root=out/f'n{node}'
        root.mkdir(exist_ok=True)
        assert retained or not (root/'data').exists(), 'fresh start requires an empty data directory'
        q=ports[node]
        bootstrap='  initNodes:\n'+''.join(f'    - "{n}@127.0.0.1:{ports[n]["peer"]}"\n' for n in initial) if node in initial else f'  seed: "1001@127.0.0.1:{ports[1001]["peer"]}"\n'
        conf=f'''mode: release
rootDir: "{root}"
addr: "tcp://127.0.0.1:{q['tcp']}"
httpAddr: "127.0.0.1:{q['api']}"
wsAddr: "ws://127.0.0.1:{q['ws']}"
tokenAuthOn: false
pprofOn: false
logger:
  level: 2
  traceOn: false
  dir: "{root}/logs"
manager:
  on: false
  addr: "127.0.0.1:{q['manager']}"
demo:
  on: false
auth:
  kind: none
conversation:
  on: false
db:
  shardNum: 2
  slotShardNum: 2
cluster:
  nodeId: {node}
  addr: "tcp://127.0.0.1:{q['peer']}"
  serverAddr: "127.0.0.1:{q['peer']}"
  apiUrl: "http://127.0.0.1:{q['api']}"
  slotCount: 64
  slotReplicaCount: 3
  channelReplicaCount: 3
{bootstrap}plugin:
  socketPath: "/tmp/replace-{os.getpid()}-{node}.sock"
'''
        path=out/f'n{node}.yaml';path.write_text(conf)
        log=(out/'logs'/f'n{node}.log').open('a');streams.append(log)
        processes[node]=subprocess.Popen([a.binary,'--config',str(path)],stdout=log,stderr=subprocess.STDOUT,env=dict(os.environ,GOMAXPROCS='2'))
        emit('start',node=node,pid=processes[node].pid,empty_data=not retained)
    def stop(node,kill=False):
        proc=processes[node]
        if proc.poll() is None:
            proc.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            try: proc.wait(timeout=15)
            except subprocess.TimeoutExpired: proc.kill();proc.wait(timeout=5)
        emit('stop',node=node,code=proc.returncode,kill=kill)
    def config(node):
        try: return json.loads((out/f'n{node}'/'data/cluster/config/remote.json').read_text())
        except (OSError,ValueError): return {}
    def wait(pred,label,seconds=60):
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            dead={n:proc.returncode for n,proc in processes.items() if proc.poll() is not None and proc.returncode != -signal.SIGKILL}
            if dead:raise RuntimeError(f'Unexpected server exit: {dead}')
            if pred(): emit('ready',label=label);return
            time.sleep(.3)
        raise TimeoutError(label)
    def settled(nodes, expected=None):
        cs=[config(n) for n in nodes]
        for cfg in cs:
            slots=cfg.get('slots',[])
            if len(slots)!=64:return False
            for st in slots:
                if st.get('leader') not in nodes or st.get('learners') or st.get('migrateFrom') or st.get('migrateTo') or st.get('status',0):return False
                if expected and sorted(st['replicas'])!=sorted(expected):return False
        return all(c['slots']==cs[0]['slots'] for c in cs)
    def snapshot(label,nodes):
        configs={n:config(n) for n in nodes}
        cfg=configs[nodes[0]]
        owners={s.get('id',0):s['leader'] for s in cfg.get('slots',[])}
        channels={c:request(owners.get(zlib.crc32(c.encode())%64,nodes[0]),f'/cluster/channels/{c}/2/config') for c in channel_ids}
        summary=dict(slots=len(cfg.get('slots',[])),slots_with_dead=sum(1002 in s.get('replicas',[]) for s in cfg.get('slots',[])),slot_learners=sum(bool(s.get('learners')) for s in cfg.get('slots',[])),channel_configs=channels)
        save(out/f'{label}.json',dict(configs=configs,summary=summary))
        emit('snapshot',label=label,**{k:v for k,v in summary.items() if k!='channel_configs'})
        return summary
    def slots_applied(nodes):
        if not settled(nodes):return False
        cfg=config(nodes[0]);byid={s.get('id',0):s for s in cfg['slots']}
        def one(pair):
            n,i=pair;r=request(n,f'/cluster/slots/{i}/localReplica')
            v=r.get('body',{});st=byid[i]
            good=r.get('status')==200 and v.get('running')==1 and v.get('leader_id')==st['leader'] and v.get('term')==st['term'] and v.get('applied_index')==v.get('committed_index')==v.get('last_log_seq')
            return (i,v.get('last_log_seq')) if good else None
        with ThreadPoolExecutor(max_workers=8) as pool:rows=list(pool.map(one,[(n,i) for n in nodes for i in range(64)]))
        return None not in rows and all(len({seq for slot,seq in rows if slot==i})==1 for i in range(64))
    def sdk(node,uid):
        proc=subprocess.Popen([a.sdk,f'127.0.0.1:{ports[node]["tcp"]}',uid],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=(out/'logs'/f'sdk-{uid}-{len(clients)}.log').open('w'),text=True,bufsize=1)
        q=queue.Queue();log=(out/'logs'/f'sdk-{uid}-{len(clients)}.jsonl').open('w',buffering=1)
        def read():
            for line in proc.stdout:
                log.write(line)
                try:q.put(json.loads(line))
                except ValueError:pass
        threading.Thread(target=read,daemon=True).start()
        clients.append(proc)
        deadline=time.monotonic()+15
        while time.monotonic()<deadline:
            row=q.get(timeout=15)
            if row.get('event')=='connect':
                assert row.get('ok'),row
                return proc,q
        raise TimeoutError('SDK connect')
    expected={}
    def send(client,channel,phase,index):
        proc,q=client
        key=f'{phase}-{channel}-{index}'
        body=dict(channel=channel,key=key,payload=f'payload:{key}')
        proc.stdin.write(json.dumps(body)+'\n');proc.stdin.flush()
        deadline=time.monotonic()+16
        while time.monotonic()<deadline:
            row=q.get(timeout=16)
            if row.get('key')==key and row.get('event') in ['ack','send','timeout']:
                emit('send_result',result=row)
                if row.get('event')=='ack' and row.get('reason')==1:expected[key]=row
                return row
        raise TimeoutError(key)
    def histories(nodes,label):
        def one(pair):
            n,c=pair
            r=request(n,'/channel/messagesync',dict(channel_id=c,channel_type=2,login_uid='probe-sender',limit=100),timeout=6)
            msgs=r.get('body',{}).get('messages',[]) if isinstance(r.get('body'),dict) else []
            bykey={m.get('client_msg_no'):m for m in msgs}
            missing=[]
            for key,exp in expected.items():
                if exp['channel']!=c:continue
                m=bykey.get(key)
                if m is None or str(m.get('message_id'))!=str(exp['id']) or m.get('message_seq')!=exp['seq'] or m.get('payload')!=base64.b64encode(exp['payload'].encode()).decode():missing.append(key)
            return dict(node=n,channel=c,response=r,missing=missing,ok=r.get('status')==200 and not missing)
        with ThreadPoolExecutor(max_workers=8) as pool:rows=list(pool.map(one,[(n,c) for n in nodes for c in channel_ids]))
        save(out/f'history-{label}.json',rows)
        stats=dict(checks=len(rows),failed=sum(not r['ok'] for r in rows),missing=sum(len(r['missing']) for r in rows))
        emit('history',label=label,**stats)
        return stats
    selected={}
    for i in range(10000):
        c=f'replacement-probe-{i}'
        selected.setdefault(zlib.crc32(c.encode())%64,c)
        if len(selected)==a.channels:break
    channel_ids=list(selected.values())
    save(out/'manifest.json',result)
    try:
        for n in initial:start(n)
        wait(lambda:settled(initial,initial),'initial-slots',90)
        wait(lambda:slots_applied(initial),'initial-slot-application',60)
        for c in channel_ids:
            def create():
                r=request(1001,'/channel',dict(channel_id=c,channel_type=2,subscribers=['probe-sender','probe-receiver','probe-receiver-4']))
                return r.get('status')==200
            wait(create,f'channel-created-{c}',30)
        sender=sdk(1001,'probe-sender');receiver=sdk(1003,'probe-receiver')
        for c in channel_ids:
            for i in range(a.initial_messages):
                r=send(sender,c,'initial',i);assert r.get('reason')==1,r
        wait(lambda:histories(initial,'initial')['failed']==0,'initial-history',30)
        snapshot('initial',initial)
        stop(1002,True)
        wait(lambda:bool(config(1001)) and any(n['id']==1002 and not n.get('online') for n in config(1001).get('nodes',[])) and settled([1001,1003]),'first-failover',60)
        result['after_first_failure']=histories([1001,1003],'first-failure')
        start(1004)
        wait(lambda:any(n['id']==1004 and n.get('status')==3 for n in config(1001).get('nodes',[])),'new-node-joined',90)
        start_observe=time.monotonic()
        samples=[]
        while time.monotonic()-start_observe<a.observe:
            if a.write_during_join:
                for c in channel_ids:send(sender,c,'joining',len(samples))
            if not a.quiet_join:
                sample=histories([1001,1003,1004],f'join-{len(samples)}')
                samples.append(sample)
            time.sleep(2)
        result['join_reads']=samples
        result['after_join']=snapshot('after-join',[1001,1003,1004])
        if a.replace_joiner_data:
            stop(1004,True)
            (out/'n1004/data').rename(out/'n1004/data-before-empty-replacement')
            start(1004)
            wait(lambda:slots_applied([1001,1003,1004]),'replacement-slot-application',90)
            wait(lambda:histories([1001,1003,1004],'replacement-history')['failed']==0,'replacement-history',60)
            result['empty_replacement']=snapshot('empty-replacement',[1001,1003,1004])
        receiver4=sdk(1004,'probe-receiver-4')
        sender4=sdk(1004,'probe-sender-4')
        critical_success=0
        for c in channel_ids:
            r=request(1004,'/channel/subscriber_add',dict(channel_id=c,channel_type=2,subscribers=['probe-sender-4']))
            if r.get('status')!=200:emit('subscriber_failed',channel=c,response=r)
            critical_success+=send(sender4,c,'new-node',0).get('reason')==1
        result['before_second_failure']=histories([1001,1003,1004],'before-second')
        if a.return_retired:
            start(1002,retained=True)
            time.sleep(10)
            result['retired_return']=snapshot('retired-return',[1001,1003,1004])
            for n in config(1003).get('nodes',[]):
                if n['id']==1002:assert not n.get('allowVote'), 'returning retired node must not regain a vote implicitly'
        stop(1001,True)
        time.sleep(12)
        result['after_second_failure']=histories([1003,1004],'second-failure')
        result['final_config']=snapshot('after-second',[1003,1004])
        for c in channel_ids:critical_success+=send(sender4,c,'second-failure',0).get('reason')==1
        result['final_history']=histories([1003,1004],'final')
        wanted={k for k in expected if k.startswith(('new-node-','second-failure-'))}
        received={}
        deadline=time.monotonic()+10
        while wanted-set(received) and time.monotonic()<deadline:
            try:r=receiver4[1].get(timeout=.2)
            except queue.Empty:continue
            if r.get('event')=='recv':received[r['key']]=r
        mismatches=[k for k in wanted if k not in received or any(received[k][f]!=expected[k][f] for f in ['id','seq','payload'])]
        result['new_node_receive']=dict(expected=len(wanted),matched=len(wanted)-len(mismatches),mismatches=mismatches)
        result['critical_send_success']=critical_success
        emit('new_node_receive',**result['new_node_receive'])
        save(out/'acks.json',expected)
        result['successful_acks']=len(expected)
        result['complete']=True
        configs=[v.get('body',{}) for v in result['after_join']['channel_configs'].values()]
        result['channels_replaced']=sum(sorted(c.get('replicas',[]))==[1001,1003,1004] and not c.get('learners') and not c.get('migrate_from') and not c.get('migrate_to') for c in configs)
        result['acceptance_pass']=result['after_join']['slots_with_dead']==0 and result['channels_replaced']==len(channel_ids) and result['after_second_failure']['failed']==0 and result['final_history']['failed']==0 and critical_success==2*len(channel_ids) and not mismatches
        if a.fixed:assert result['acceptance_pass'],result
    except Exception as e:
        result['error']=str(e);result['traceback']=traceback.format_exc();emit('error',error=str(e));raise
    finally:
        for proc in clients:
            if proc.poll() is None:proc.terminate()
        for n in processes:stop(n)
        result['ended']=time.time();save(out/'result.json',result);save(out/'acks.json',expected)
        for stream in streams:stream.close()

if __name__=='__main__':main()
