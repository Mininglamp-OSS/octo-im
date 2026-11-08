#!/usr/bin/env python3
"""Audit stopped stores independently of HTTP; exit nonzero on any discrepancy.

Build the inspectors beside the case directory (from the repository root):
  go build -o /tmp/replica-check/dbinspect test/e2e/offline_replica_dbinspect.go
  go build -o /tmp/replica-check/slotinspect test/e2e/offline_replica_slotinspect.go
  python3 test/e2e/offline_replica_audit.py /tmp/replica-check/case

Run only after the process fixture exits. Message DB inspection uses disposable
copies; slot DBs are opened read-only. Original file hashes must remain unchanged.
The fixture's online acceptance and this physical audit must both pass.
"""
import base64
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import zlib

case=Path(sys.argv[1]).resolve()
bin=case.parent
acks=json.loads((case/'acks.json').read_text())
channels=sorted({v['channel'] for v in acks.values()})
physical={};slots={};checks={};failures=[]
def manifest(root):
    return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
for n in [1003,1004]:
    root=case/f'n{n}'/'data'
    before=manifest(root)
    dest=case/f'audit-copy-{n}'
    shutil.copytree(root/'db',dest)
    try:
        out=subprocess.check_output([str(bin/'dbinspect'),str(dest),'2'],input=json.dumps(channels),text=True,stderr=subprocess.STDOUT)
        (case/f'physical-{n}.log').write_text(out)
        data=json.loads(out.split('REPLACEMENT_DB_JSON=')[1].splitlines()[0]);physical[n]=data
        (case/f'physical-{n}.json').write_text(json.dumps(data,indent=2))
    finally:
        shutil.rmtree(dest)  # disposable inspection copy only
    out=subprocess.check_output([str(bin/'slotinspect'),str(root/'cluster/logdb'),'8'],text=True,stderr=subprocess.STDOUT)
    (case/f'slot-log-{n}.log').write_text(out)
    slots[n]=json.loads(out.split('REPLACEMENT_SLOT_JSON=')[1].splitlines()[0])
    (case/f'slot-log-{n}.json').write_text(json.dumps(slots[n],indent=2))
    assert manifest(root)==before,'inspection must not change original evidence'
    for c in channels:
        rows=data[c]['messages'] or []
        bykey={m['ClientMsgNo']:m for m in rows}
        for key,a in acks.items():
            if a['channel']!=c:continue
            m=bykey.get(key)
            if not m or (m['MessageID'],m['MessageSeq'],m['Payload']) != (a['id'],a['seq'],base64.b64encode(a['payload'].encode()).decode()):failures.append(dict(node=n,channel=c,key=key,error='ACK not exactly matched'))
        if data[c]['config_error']!='<nil>' or data[c]['messages_error']!='<nil>':failures.append(dict(node=n,channel=c,error='database read error'))
        cfg=data[c]['config']
        if sorted(cfg['replicas'])!=[1001,1003,1004] or cfg.get('learners') or cfg.get('migrate_from') or cfg.get('migrate_to') or cfg['replica_max_count']!=3:failures.append(dict(node=n,channel=c,error='unrepaired configuration',config=cfg))
        if len(rows)!=len(bykey):failures.append(dict(node=n,channel=c,error='duplicate durable client key'))
        applied=slots[n][str(zlib.crc32(c.encode())%64)]['applied']
        relevant=[l for l in slots[n][str(zlib.crc32(c.encode())%64)]['logs'] if l['index']<=applied and l.get('config',{}).get('channel_id')==c]
        if relevant:
            last=relevant[-1];expected=dict(last['config']);expected['conf_version']=last['index']
            fields=['replica_max_count','replicas','learners','learder_id','term','migrate_from','migrate_to','conf_version']
            diff={k:(expected.get(k),cfg.get(k)) for k in fields if expected.get(k)!=cfg.get(k)}
            if diff:failures.append(dict(node=n,channel=c,error='metadata differs from applied slot command',diff=diff))
        else:failures.append(dict(node=n,channel=c,error='no applied channel metadata'))
for i in range(64):
    a,b=slots[1003][str(i)],slots[1004][str(i)]
    okay=a['logs']==b['logs'] and all(x['applied']==(x['logs'][-1]['index'] if x['logs'] else 0) and [l['index'] for l in x['logs']]==list(range(1,len(x['logs'])+1)) for x in [a,b])
    checks[i]=okay
    if not okay:failures.append(dict(slot=i,error='slot logs diverged, noncontiguous or not applied to tail'))
for c in channels:
    fields=['MessageID','MessageSeq','ClientMsgNo','Payload']
    if [[m[k] for k in fields] for m in physical[1003][c]['messages']] != [[m[k] for k in fields] for m in physical[1004][c]['messages']]:failures.append(dict(channel=c,error='physical message replicas differ'))
result=dict(pass_=not failures,successful_acks=len(acks),channels=len(channels),slots_identical_and_applied=sum(checks.values()),physical_rows={n:sum(len(v['messages'] or []) for v in physical[n].values()) for n in physical},failures=failures)
(case/'physical-audit.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result));sys.exit(bool(failures))
