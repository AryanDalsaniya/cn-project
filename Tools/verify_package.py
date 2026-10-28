#!/usr/bin/env python3
"""Check submission shape, retained evidence and optional strict clean builds."""
import argparse,csv,hashlib,json,pathlib,subprocess
ROOT=pathlib.Path(__file__).resolve().parents[1]
CASES=['Part1','Part2_Case1','Part2_Case2','Part2_Case2_Enhanced']

def check_runs(folder, expected_count):
    runs=list(csv.DictReader((folder/'runs.csv').open()))
    assert len(runs)==expected_count,(folder,len(runs))
    keys=set()
    for r in runs:
        key=tuple(r[k] for k in ['case','mode','clients','probability','connections','repeat'])
        assert key not in keys;keys.add(key)
        useful=int(r['useful_bytes']);sent=int(r['payload_sent']);extra=int(r['redundant_bytes'])
        app=int(r['application_bytes']);control=int(r['control_bytes'])
        assert sent-useful==extra and app-sent==control and extra>=0
        assert r['status']=='complete_SHA256_match'
        assert abs(float(r['goodput_MiB_s'])-useful/float(r['seconds'])/1048576)<1e-8
        assert (folder/r['raw_dir']/'server.csv').is_file()
    return runs

def main():
    p=argparse.ArgumentParser();p.add_argument('--build',action='store_true');a=p.parse_args()
    for name in ['FinalReport_Group_097_112.pdf','README.md','Report/main.tex','Results/Animals10/runs.csv','Results/Validation/tests.csv']:
        assert (ROOT/name).is_file(),name
    for case in CASES:
        for name in ['server.cpp','client.cpp','common.hpp','Makefile']:assert (ROOT/case/name).is_file()
        if a.build:
            subprocess.run(['make','-C',str(ROOT/case),'clean'],check=True,stdout=subprocess.DEVNULL)
            r=subprocess.run(['make','-C',str(ROOT/case),'-j2','CXXFLAGS=-O2 -std=c++17 -Wall -Wextra -Wpedantic -Werror -pthread'],capture_output=True,text=True)
            assert r.returncode==0,r.stdout+r.stderr
            assert {p.name for p in (ROOT/case).iterdir() if p.is_file() and p.stat().st_mode&0o111}=={'server','client'}
    check_runs(ROOT/'Results/Local',87)
    animals=check_runs(ROOT/'Results/Animals10',87)
    metadata=json.loads((ROOT/'Results/Animals10/animals10_manifest.json').read_text())
    assert len(metadata['files'])==4863 and sum(f['bytes'] for f in metadata['files'])==60686716
    assert len({f['name'] for f in metadata['files']})==4863
    assert all(len(f['sha256'])==64 for f in metadata['files'])
    for r in animals:
        assert int(r['useful_bytes'])==int(r['clients'])*60686716
    tests=list(csv.DictReader((ROOT/'Results/Validation/tests.csv').open()))
    assert len(tests)==28 and all(r['status']=='PASS' for r in tests)
    manifest=ROOT/'SHA256SUMS.txt'
    if manifest.exists():
        for line in manifest.read_text().splitlines():
            h,name=line.split('  ',1)
            assert hashlib.sha256((ROOT/name).read_bytes()).hexdigest()==h,name
    print('PASS: required structure, 87 Animals10 + 87 synthetic runs, 28 integration outcomes, 4,863-file manifest, byte arithmetic and package checksums'+('; four clean -Werror builds' if a.build else ''))
if __name__=='__main__':main()
