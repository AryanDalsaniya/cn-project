#!/usr/bin/env python3
"""One server + two remote client machines. Requires SSH keys and prepared checkouts."""
import argparse, concurrent.futures, csv, json, pathlib, queue, shlex, subprocess, threading, time
from test_project import ROOT,CASES,Server,build,digest

def main():
    ap=argparse.ArgumentParser()
    for arg in ['topics','server-ip','host-a','host-b','remote-root']:ap.add_argument('--'+arg,required=True)
    ap.add_argument('--topic',required=True);ap.add_argument('--repeats',type=int,default=3)
    ap.add_argument('--results',type=pathlib.Path,default=ROOT/'Results'/'LAN');a=ap.parse_args()
    if a.host_a==a.host_b:ap.error('use two distinct client computers')
    a.results.mkdir(parents=True,exist_ok=True);build()
    source=pathlib.Path(a.topics).resolve()/a.topic
    expected={f.name:digest(f) for f in source.iterdir() if f.is_file() and not f.is_symlink()}
    for host in [a.host_a,a.host_b]:
        for case in CASES:
            subprocess.run(['ssh','-o','BatchMode=yes',host,'make -C '+shlex.quote(a.remote_root+'/'+case)],check=True)
    configs=[('Part1',m,n,0,1) for m in ['single','multi'] for n in [2,4,8]]
    configs += [(c,'multi',2,p,4 if c==CASES[3] else 1) for c in CASES[1:] for p in [0,.0001,.0005,.001]]
    configs += [(CASES[3],'multi',2,.0005,k) for k in [1,2,8]]
    environment={'topology':'one local server, two SSH-controlled remote client computers','server_ip':a.server_ip,
                 'client_hosts':[a.host_a,a.host_b],'topic':a.topic,'dataset_root':str(source.parent),'repeats':a.repeats,
                 'timing':'controller GO release to receipt of both DONE messages; excludes file hash computation; includes SSH control latency',
                 'note':'separate from supplied loopback evidence; record machine hardware/network details separately'}
    (a.results/'environment.json').write_text(json.dumps(environment,indent=2)+'\n');rows=[]
    for case,mode,n,prob,k in configs:
        for rep in range(1,a.repeats+1):
            name=f'{case}_{mode}_n{n}_p{prob}_k{k}_r{rep}';logs=a.results/'raw'/name;procs=[];q=queue.Queue();responses={}
            def read_node(idx,proc):
                try:
                    line=proc.stdout.readline().strip()
                    if not line.startswith('DONE '):raise RuntimeError('missing DONE: '+line)
                    q.put((idx,time.monotonic(),None))
                    line=proc.stdout.readline().strip()
                    if not line.startswith('RESULT '):raise RuntimeError('missing RESULT')
                    responses[idx]=json.loads(line[7:])
                    if proc.wait(timeout=30)!=0:raise RuntimeError('remote helper failed')
                except Exception as e:q.put((idx,time.monotonic(),str(e)))
            try:
                with Server(case,pathlib.Path(a.topics),logs,prob,mode,97000+rep) as server:
                    for host,num in [(a.host_a,n//2),(a.host_b,n-n//2)]:
                        command=shlex.join(['python3',a.remote_root+'/Tools/lan_worker.py',case,a.server_ip,str(server.port),a.topic,str(num),str(k)])
                        proc=subprocess.Popen(['ssh','-o','BatchMode=yes',host,command],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                        procs.append(proc)
                        if proc.stdout.readline().strip()!='READY':raise RuntimeError('remote not ready: '+proc.stderr.read())
                    threads=[threading.Thread(target=read_node,args=(i,p)) for i,p in enumerate(procs)]
                    for t in threads:t.start()
                    start=time.monotonic()
                    for proc in procs:proc.stdin.write('GO\n');proc.stdin.flush();proc.stdin.close()
                    completed={}
                    while len(completed)<2:
                        i,stamp,error=q.get(timeout=660)
                        if error:raise RuntimeError(error)
                        completed[i]=stamp
                    elapsed=max(completed.values())-start
                    for t in threads:t.join(timeout=660)
                    if len(responses)!=2:raise RuntimeError('remote results incomplete')
                    metrics=[]
                    for node,items in responses.items():
                        (logs/f'node{node}.json').write_text(json.dumps(items,indent=2)+'\n')
                        for item in items:
                            if item['exit_code'] or item['sha256']!=expected:raise RuntimeError('remote transfer/hash failure')
                            metrics+=list(csv.DictReader(item['stdout'].splitlines()))
                sr=list(csv.DictReader((logs/'server.csv').open()));sent=sum(int(r['payload_sent']) for r in sr)
                app=sum(int(r['application_sent']) for r in sr)+sum(int(r['application_sent']) for r in metrics)
                useful=sum(int(r['useful_bytes']) for r in metrics)
                row=dict(case=case,mode=mode,clients=n,probability=prob,connections=k,repeat=rep,seconds=elapsed,
                         useful_bytes=useful,payload_sent=sent,redundant_bytes=sent-useful,control_bytes=app-sent,
                         application_bytes=app,received_payload=sum(int(r['payload_received']) for r in metrics),
                         attempts=sum(int(r['attempts']) for r in metrics),failures=sum(r['status']=='injected_failure' for r in sr),
                         goodput_MiB_s=useful/elapsed/1048576,sender_rate_MiB_s=sent/elapsed/1048576,
                         redundancy_percent=100*(sent-useful)/useful,status='complete_SHA256_match',raw_dir=str(logs.relative_to(a.results)))
                rows.append(row)
                with (a.results/'runs.csv').open('w',newline='') as f:
                    w=csv.DictWriter(f,fieldnames=list(row));w.writeheader();w.writerows(rows)
                print(name,row['goodput_MiB_s'],flush=True)
            finally:
                for proc in procs:
                    if proc.poll() is None:proc.kill();proc.wait()
    return 0
if __name__=='__main__':raise SystemExit(main())
