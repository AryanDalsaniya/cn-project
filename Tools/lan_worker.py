#!/usr/bin/env python3
"""Remote helper for a LAN benchmark. Invoked by lan_benchmark.py over SSH."""
import concurrent.futures, csv, hashlib, io, json, pathlib, subprocess, sys, tempfile, time

def main():
    case,host,port,topic,number,connections=sys.argv[1:];n=int(number)
    executable=pathlib.Path(__file__).resolve().parents[1]/case/'client'
    with tempfile.TemporaryDirectory(prefix='it305-lan-') as td:
        root=pathlib.Path(td)
        print('READY',flush=True)
        if sys.stdin.readline().strip()!='GO':raise RuntimeError('missing GO barrier')
        start=time.monotonic()
        def one(i):
            out=root/str(i)
            p=subprocess.run([str(executable),host,port,topic,str(out),connections],capture_output=True,text=True,timeout=600)
            return i,p,out
        with concurrent.futures.ThreadPoolExecutor(max(1,n)) as pool:results=list(pool.map(one,range(n)))
        print('DONE '+json.dumps({'seconds':time.monotonic()-start,'clients':n}),flush=True)
        rows=[]
        for i,p,out in results:
            hashes={}
            if (out/topic).is_dir():
                for f in (out/topic).iterdir():
                    if f.is_file():
                        h=hashlib.sha256()
                        with f.open('rb') as src:
                            for b in iter(lambda:src.read(1048576),b''):h.update(b)
                        hashes[f.name]=h.hexdigest()
            rows.append({'client':i,'exit_code':p.returncode,'stdout':p.stdout,'stderr':p.stderr,'sha256':hashes})
        print('RESULT '+json.dumps(rows),flush=True)
if __name__=='__main__':main()
