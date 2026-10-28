#!/usr/bin/env python3
"""One ASan/UBSan network-recovery smoke run (leak detection disabled)."""
import os,pathlib,shutil,socket,subprocess,tempfile,time
from test_project import ROOT,free_port,compare
from generate_dataset import generate

def main():
 evidence=ROOT/'Results'/'Validation';evidence.mkdir(parents=True,exist_ok=True)
 with tempfile.TemporaryDirectory(prefix='it305-sanitize-') as td:
  root=pathlib.Path(td)
  for name in ['server.cpp','client.cpp','common.hpp']:shutil.copy2(ROOT/'Part2_Case2'/name,root/name)
  for name in ['server','client']:
   subprocess.run(['g++','-DCASE=2','-std=c++17','-O1','-g','-fsanitize=address,undefined','-fno-omit-frame-pointer','-pthread',str(root/(name+'.cpp')),'-o',str(root/name)],check=True)
  generate(root/'topics',.5,4);port=free_port();env={**os.environ,'ASAN_OPTIONS':'detect_leaks=0'}
  with (evidence/'sanitizer_server.log').open('w') as log:
   server=subprocess.Popen([str(root/'server'),str(port),str(root/'topics'),'.5','97112'],stdout=subprocess.PIPE,stderr=log,text=True,env=env)
   try:
    for _ in range(200):
     if server.poll() is not None:raise RuntimeError('sanitized server exited')
     try:
      with socket.create_connection(('127.0.0.1',port),.1):break
     except OSError:time.sleep(.01)
    else:raise RuntimeError('server readiness timeout')
    r=subprocess.run([str(root/'client'),'127.0.0.1',str(port),'Cane',str(root/'out')],capture_output=True,text=True,timeout=30,env=env)
    (evidence/'sanitizer_client.log').write_text(r.stderr);(evidence/'sanitizer_client.csv').write_text(r.stdout)
    if r.returncode:raise RuntimeError(r.stderr)
    compare(root/'topics'/'Cane',root/'out'/'Cane')
   finally:
    server.terminate();out,_=server.communicate(timeout=35);(evidence/'sanitizer_server.csv').write_text(out)
  assert server.returncode==0
  for name in ['sanitizer_client.log','sanitizer_server.log']:
   text=(evidence/name).read_text();assert 'ERROR: AddressSanitizer' not in text and 'runtime error:' not in text
  (evidence/'sanitizer_result.txt').write_text('PASS: Case 2 built with AddressSanitizer + UndefinedBehaviorSanitizer; 0.5 MiB, four files, p=0.5, SHA-256 matched. No ASan/UBSan error reported. LeakSanitizer disabled because the sandbox prevents its process inspection. This is one additional instrumented smoke run, not exhaustive sanitizer coverage.\n')
  print('PASS sanitizer recovery and SHA-256')
if __name__=='__main__':main()
