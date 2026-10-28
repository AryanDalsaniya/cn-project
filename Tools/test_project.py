#!/usr/bin/env python3
"""Integration tests: actual TCP processes, file hashes, and application-frame faults."""
import argparse, csv, hashlib, os, pathlib, random, shutil, socket, struct, subprocess, tempfile, threading, time
ROOT = pathlib.Path(__file__).resolve().parents[1]
CASES = ['Part1', 'Part2_Case1', 'Part2_Case2', 'Part2_Case2_Enhanced']

def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0)); return s.getsockname()[1]

def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1024*1024), b''): h.update(b)
    return h.hexdigest()

def compare(source, output):
    expected = {f.name: digest(f) for f in source.iterdir() if f.is_file() and not f.is_symlink()}
    actual = {f.name: digest(f) for f in output.iterdir() if f.is_file()}
    assert actual == expected, 'SHA-256/file-set mismatch'

def build():
    for c in CASES:
        p = subprocess.run(['make', '-C', str(ROOT/c), '-j2'], capture_output=True, text=True)
        assert p.returncode == 0, p.stdout+p.stderr
        assert 'warning:' not in p.stderr, p.stderr
        assert sorted(p.name for p in (ROOT/c).iterdir() if p.is_file() and os.access(p, os.X_OK)) == ['client', 'server']

class Server:
    def __init__(self, case, data, logs, probability=0, mode='multi', seed=7301):
        self.port = free_port(); self.logs = pathlib.Path(logs); self.logs.mkdir(parents=True, exist_ok=True)
        self.out = open(self.logs/'server.csv', 'w'); self.err = open(self.logs/'server.log', 'w')
        args = [str(ROOT/case/'server'), str(self.port), str(data), mode if case=='Part1' else str(probability), str(seed)]
        self.process = subprocess.Popen(args, stdout=self.out, stderr=self.err)
        for _ in range(200):
            if self.process.poll() is not None: raise RuntimeError((self.logs/'server.log').read_text())
            try:
                with socket.create_connection(('127.0.0.1', self.port), timeout=.05): return
            except OSError: time.sleep(.01)
        raise RuntimeError('server readiness timeout')
    def stop(self):
        self.process.terminate()
        try: self.process.wait(timeout=35)
        except subprocess.TimeoutExpired: self.process.kill(); self.process.wait(); raise
        self.out.close(); self.err.close()
    def __enter__(self): return self
    def __exit__(self, *_): self.stop()

def client(case, port, topic, out, connections=None, tries=None, timeout=60):
    args = [str(ROOT/case/'client'), '127.0.0.1', str(port), topic, str(out)]
    if connections is not None: args.append(str(connections))
    if tries is not None: args.append(str(tries))
    return subprocess.run(args, text=True, capture_output=True, timeout=timeout)

def metric(p):
    assert p.returncode == 0, p.stdout+p.stderr
    return list(csv.DictReader(p.stdout.splitlines()))[-1]

def exact(s, n):
    b = b''
    while len(b)<n:
        p=s.recv(n-len(b))
        if not p: raise EOFError
        b+=p
    return b

def frame(s):
    h=exact(s,20); magic,typ,sid,n=struct.unpack('!IIQI',h)
    assert magic==0x49543335 and n<=16*1024*1024
    return typ,sid,exact(s,n)

def send_frame(s, typ, sid, b): s.sendall(struct.pack('!IIQI',0x49543335,typ,sid,len(b))+b)
def get_body(topic, index=0, offset=0):
    b=topic.encode();return struct.pack('!I',len(b))+b+struct.pack('!QQQQ',0,1,index,offset)

class FaultProxy:
    """One controlled disconnect at a frame boundary or in a DATA/header body."""
    def __init__(self, server_port, typ, direction='down', partial=False, fragment=False, data_ack=False):
        self.server_port=server_port;self.typ=typ;self.direction=direction;self.partial=partial;self.fragment=fragment
        self.data_ack=data_ack;self.used=False;self.lock=threading.Lock();self.stop_event=threading.Event();self.threads=[]
        self.listener=socket.socket();self.listener.bind(('127.0.0.1',0));self.listener.listen();self.listener.settimeout(.1)
        self.port=self.listener.getsockname()[1]
        self.thread=threading.Thread(target=self.accept,daemon=True);self.thread.start()
    def accept(self):
        while not self.stop_event.is_set():
            try: a,_=self.listener.accept()
            except socket.timeout: continue
            except OSError: return
            b=socket.create_connection(('127.0.0.1',self.server_port))
            for src,dst,direction in [(a,b,'up'),(b,a,'down')]:
                t=threading.Thread(target=self.pump,args=(src,dst,direction),daemon=True);self.threads.append(t);t.start()
    def pump(self, src, dst, direction):
        try:
            while True:
                h=exact(src,20);_,typ,_,n=struct.unpack('!IIQI',h);body=exact(src,n);data=h+body
                with self.lock:
                    cut=not self.used and direction==self.direction and typ==self.typ and (not self.data_ack or (len(body)==16 and struct.unpack("!QQ",body)[1]>0))
                    if cut:self.used=True
                if cut:
                    if self.partial: dst.sendall(data[:max(1,len(data)//2)])
                    return
                if self.fragment:
                    for j in range(0,len(data),317):dst.sendall(data[j:j+317])
                else:dst.sendall(data)
        except (EOFError,OSError): pass
        finally:
            for s in (src,dst):
                try:s.shutdown(socket.SHUT_RDWR)
                except OSError:pass
                s.close()
    def close(self):
        self.stop_event.set();self.listener.close();self.thread.join(timeout=2)
        for t in self.threads:t.join(timeout=2)

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--results',type=pathlib.Path,default=ROOT/'Results'/'Validation');a=ap.parse_args()
    a.results.mkdir(parents=True,exist_ok=True); rows=[];build()
    with tempfile.TemporaryDirectory(prefix='it305-test-') as td:
        tmp=pathlib.Path(td);data=tmp/'topics';topic=data/'Cane';topic.mkdir(parents=True);(data/'Empty').mkdir()
        rng=random.Random(97112)
        for name,n in [('00-empty.bin',0),('a file.bin',65539),('binary.dat',131091),('unicode_ગુજરાતી.txt',97)]:
            (topic/name).write_bytes(rng.randbytes(n))
        (topic/'ignored-link').symlink_to(topic/'binary.dat')
        def check(name,fn):
            start=time.monotonic()
            try:fn();status='PASS';detail=''
            except Exception as e:status='FAIL';detail=str(e)
            rows.append(dict(test=name,status=status,seconds=f'{time.monotonic()-start:.6f}',detail=detail))
            print(status,name,detail,flush=True)
        def basic(case,mode='multi',k=None,p=0):
            tag=f'{case}_{mode}_{k}_{p}';out=tmp/tag
            with Server(case,data,a.results/tag,p,mode) as s:
                r=client(case,s.port,'Cane',out,k);metric(r);compare(topic,out/'Cane')
                (a.results/tag/'client.csv').write_text(r.stdout);(a.results/tag/'client.log').write_text(r.stderr)
        for case in CASES:check(case+'_default_CLI_integrity',lambda c=case:basic(c))
        check('Part1_single_integrity',lambda:basic('Part1','single'))
        for k in [1,2,8]:check(f'Enhanced_K{k}',lambda k=k:basic(CASES[3],k=k))
        for case,p in [(CASES[1],.1),(CASES[2],.6),(CASES[3],.6)]:
            check(case+'_probabilistic_recovery',lambda c=case,p=p:basic(c,p=p))
        for case in CASES:
            def edges(case=case):
                with Server(case,data,a.results/(case+'_edges')) as s:
                    out=tmp/(case+'_empty');metric(client(case,s.port,'Empty',out));assert list((out/'Empty').iterdir())==[]
                    assert client(case,s.port,'Missing',tmp/'missing').returncode!=0
                    assert client(case,s.port,'../Cane',tmp/'traversal').returncode!=0
                    with socket.create_connection(('127.0.0.1',s.port)) as q:
                        q.sendall(struct.pack('!IIQI',0x49543335,1,90,16*1024*1024+1));assert frame(q)[0]==9
                    metric(client(case,s.port,'Cane',tmp/(case+'_after_bad')))
            check(case+'_empty_unknown_traversal_oversized',edges)
        def concurrent():
            from concurrent.futures import ThreadPoolExecutor
            with Server(CASES[3],data,a.results/'concurrent',.15) as s:
                def one(i):
                    out=tmp/f'concurrent_{i}';metric(client(CASES[3],s.port,'Cane',out));compare(topic,out/'Cane')
                with ThreadPoolExecutor(4) as pool:list(pool.map(one,range(4)))
        check('four_concurrent_enhanced_clients',concurrent)
        for typ,direction,partial in [(2,'down',True),(4,'down',True),(5,'down',True),(6,'up',False),(7,'down',False),(8,'down',False)]:
            def interrupted(typ=typ,direction=direction,partial=partial):
                tag=f'interrupt_{typ}_{direction}';out=tmp/tag
                with Server(CASES[2],data,a.results/tag) as s:
                    proxy=FaultProxy(s.port,typ,direction,partial)
                    try:r=client(CASES[2],proxy.port,'Cane',out);metric(r);compare(topic,out/'Cane');assert proxy.used
                    finally:proxy.close()
                    (a.results/tag/'client.csv').write_text(r.stdout);(a.results/tag/'client.log').write_text(r.stderr)
            check(f'interruption_type_{typ}_{direction}',interrupted)
        def lost_data_ack():
            with Server(CASES[2],data,a.results/'lost_data_ack') as s:
                proxy=FaultProxy(s.port,6,'up',data_ack=True)
                try:
                    r=client(CASES[2],proxy.port,'Cane',tmp/'lost_data_ack');metric(r);compare(topic,tmp/'lost_data_ack'/'Cane');assert proxy.used
                finally:proxy.close()
                (a.results/'lost_data_ack'/'client.csv').write_text(r.stdout)
            sent=sum(int(r['payload_sent']) for r in csv.DictReader((a.results/'lost_data_ack'/'server.csv').open()))
            assert sent==sum(f.stat().st_size for f in topic.iterdir() if f.is_file() and not f.is_symlink()), 'lost ACK caused redundant DATA'
        check('lost_DATA_ACK_resume_without_retransmission',lost_data_ack)
        def fragmented():
            with Server(CASES[2],data,a.results/'fragmented') as s:
                proxy=FaultProxy(s.port,999,fragment=True)
                try:metric(client(CASES[2],proxy.port,'Cane',tmp/'fragmented'));compare(topic,tmp/'fragmented'/'Cane')
                finally:proxy.close()
        check('fragmented_TCP_frames',fragmented)
        def bad_checkpoint():
            with Server(CASES[2],data,a.results/'checkpoint') as s:
                with socket.create_connection(('127.0.0.1',s.port)) as q:
                    send_frame(q,1,501,get_body('Cane',1,999999));assert frame(q)[0]==9
                metric(client(CASES[2],s.port,'Cane',tmp/'checkpoint'))
        check('reject_unknown_session_nonzero_checkpoint',bad_checkpoint)
        def destinations():
            with Server(CASES[2],data,a.results/'symlinks') as s:
                out=tmp/'symlink_out';out.mkdir();outside=tmp/'outside';outside.mkdir();(out/'Cane').symlink_to(outside)
                assert client(CASES[2],s.port,'Cane',out).returncode!=0;assert not list(outside.iterdir())
                out2=tmp/'symlink_file';(out2/'Cane').mkdir(parents=True);target=tmp/'protected';target.write_text('KEEP')
                (out2/'Cane'/'00-empty.bin').symlink_to(target)
                assert client(CASES[2],s.port,'Cane',out2).returncode!=0;assert target.read_text()=='KEEP'
        check('destination_symlink_rejection',destinations)
        def p1():
            with Server(CASES[1],data,a.results/'p1_restart',1) as s:assert client(CASES[1],s.port,'Cane',tmp/'p1_restart',1,3).returncode!=0
            with Server(CASES[2],data,a.results/'p1_resume',1) as s:metric(client(CASES[2],s.port,'Cane',tmp/'p1_resume',1,30));compare(topic,tmp/'p1_resume'/'Cane')
        check('p1_restart_exhaustion_and_resume_progress',p1)
        def invalid_cli():
            for c in CASES[1:]:
                for p in ['-0.1','1.1','nan','garbage']:
                    r=subprocess.run([str(ROOT/c/'server'),'12345',str(data),p],capture_output=True,timeout=3);assert r.returncode!=0
            r=subprocess.run([str(ROOT/CASES[0]/'server'),'0',str(data)],capture_output=True,timeout=3);assert r.returncode!=0
        check('invalid_CLI_values',invalid_cli)
    with open(a.results/'tests.csv','w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=['test','status','seconds','detail']);w.writeheader();w.writerows(rows)
    n=sum(r['status']=='PASS' for r in rows);print(f'{n}/{len(rows)} passed');return 0 if n==len(rows) else 1
if __name__=='__main__':raise SystemExit(main())
