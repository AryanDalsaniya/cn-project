#!/usr/bin/env python3
"""Measure local real TCP transfers. Never label loopback runs as LAN evidence."""
import argparse, concurrent.futures, csv, json, os, pathlib, platform, shutil, subprocess, tempfile, time
from test_project import ROOT, CASES, Server, client, metric, compare, build
from generate_dataset import generate

def main():
    p=argparse.ArgumentParser();p.add_argument('--results',type=pathlib.Path,default=ROOT/'Results'/'Local');p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--extend',action='store_true',help='retain completed configurations and run missing ones');p.add_argument('--topics',type=pathlib.Path);p.add_argument('--topic',default='Cane')
    p.add_argument('--probabilities',type=float,nargs='+',default=[0,.01,.03,.05,.075,.1])
    p.add_argument('--parallel-probability',type=float,default=.03)
    p.add_argument('--client-timeout',type=float,default=180)
    a=p.parse_args()
    if a.repeats<1 or a.client_timeout<=0:p.error('positive repeats and timeout required')
    if any(not 0<=p<=1 for p in a.probabilities) or not 0<=a.parallel_probability<=1:p.error('probabilities must lie in [0,1]')
    if len(set(a.probabilities))!=len(a.probabilities):p.error('duplicate probabilities')
    if a.parallel_probability not in a.probabilities:p.error('include parallel probability in the sweep for K=4 comparison')
    a.results.mkdir(parents=True,exist_ok=True)
    build();rows=[]
    if a.extend and (a.results/'runs.csv').exists(): rows=list(csv.DictReader((a.results/'runs.csv').open()))
    completed={(r['case'],r['mode'],int(r['clients']),float(r['probability']),int(r['connections']),int(r['repeat'])) for r in rows}
    with tempfile.TemporaryDirectory(prefix='it305-benchmark-') as td:
        tmp=pathlib.Path(td)
        if a.topics:data=a.topics.resolve();dataset={'kind':'user-provided dataset','root':str(data),'topic':a.topic,'bytes':sum(f.stat().st_size for f in (data/a.topic).iterdir() if f.is_file() and not f.is_symlink())}
        else:data=tmp/'topics';dataset=generate(data,2,8)
        (a.results/'dataset.json').write_text(json.dumps(dataset,indent=2)+'\n')
        environment={'topology':'single host, IPv4 loopback','platform':platform.platform(),'cpu_count':os.cpu_count(),
                     'compiler':subprocess.check_output(['g++','--version'],text=True).splitlines()[0],
                     'python':platform.python_version(),'date_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),
                     'chunk_bytes':65536,'repeats':a.repeats,'timing':'first client process launch through last client process exit; SHA-256 verification excluded',
                     'cache_policy':'no cache flush; repeated reads may use page cache','protocol':'stop-and-wait ACK per DATA; CRC32 per chunk'}
        (a.results/'environment.json').write_text(json.dumps(environment,indent=2)+'\n')
        configs=[]
        for mode in ['single','multi']:
            for n in [1,2,4,8]:configs.append(('Part1',mode,n,0,1))
        for case in CASES[1:]:
            for prob in a.probabilities:configs.append((case,'multi',1,prob,4 if case==CASES[3] else 1))
        for k in [1,2,8]:configs.append((CASES[3],'multi',1,a.parallel_probability,k))
        for case,mode,n,prob,k in configs:
            for repeat in range(1,a.repeats+1):
                if (case,mode,n,prob,k,repeat) in completed: continue
                name=f'{case}_{mode}_n{n}_p{prob}_k{k}_r{repeat}';logs=a.results/'raw'/name;outs=[tmp/name/str(i) for i in range(n)]
                with Server(case,data,logs,prob,mode,seed=97000+repeat) as s:
                    start=time.monotonic()
                    with concurrent.futures.ThreadPoolExecutor(n) as pool:
                        results=list(pool.map(lambda out:client(case,s.port,a.topic,out,k,1000,a.client_timeout),outs))
                    elapsed=time.monotonic()-start
                    metrics=[metric(r) for r in results]
                    for i,(out,result) in enumerate(zip(outs,results)):
                        compare(data/a.topic,out/a.topic)
                        (logs/f'client_{i}.csv').write_text(result.stdout);(logs/f'client_{i}.log').write_text(result.stderr)
                server_rows=list(csv.DictReader((logs/'server.csv').open()))
                sent=sum(int(r['payload_sent']) for r in server_rows)
                server_app=sum(int(r['application_sent']) for r in server_rows)
                client_app=sum(int(m['application_sent']) for m in metrics)
                useful=sum(int(m['useful_bytes']) for m in metrics);redundant=sent-useful
                assert redundant>=0
                row={'case':case,'mode':mode,'clients':n,'probability':prob,'connections':k,'repeat':repeat,
                     'seconds':elapsed,'useful_bytes':useful,'payload_sent':sent,'redundant_bytes':redundant,
                     'control_bytes':server_app-sent+client_app,'application_bytes':server_app+client_app,
                     'received_payload':sum(int(m['payload_received']) for m in metrics),
                     'attempts':sum(int(m['attempts']) for m in metrics),'failures':sum(r['status']=='injected_failure' for r in server_rows),
                     'goodput_MiB_s':useful/elapsed/1048576,'sender_rate_MiB_s':sent/elapsed/1048576,
                     'redundancy_percent':100*redundant/useful,'status':'complete_SHA256_match','raw_dir':str(logs.relative_to(a.results))}
                rows.append(row);shutil.rmtree(tmp/name)
                with (a.results/'runs.csv').open('w',newline='') as f:
                    w=csv.DictWriter(f,fieldnames=list(row));w.writeheader();w.writerows(rows)
                print(name, f'{elapsed:.3f}s {row["goodput_MiB_s"]:.2f} MiB/s redundant={redundant}',flush=True)
    return 0
if __name__=='__main__':raise SystemExit(main())
