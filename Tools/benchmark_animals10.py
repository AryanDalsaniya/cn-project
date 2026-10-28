#!/usr/bin/env python3
"""Run the existing measured-transfer harness on a complete Animals10 topic.

Preserves the synthetic benchmark and records actual single-host TCP results.
Fault probabilities are supplied explicitly to keep the full-topic restart case
tractable. This is not a three-computer network experiment.
"""
import argparse
import csv
import hashlib
import json
import pathlib
import sys

import benchmark


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--topics', type=pathlib.Path, required=True)
    parser.add_argument('--topic', default='cane')
    parser.add_argument('--results', type=pathlib.Path, default=benchmark.ROOT/'Results'/'Animals10')
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--provenance', type=pathlib.Path)
    args = parser.parse_args()
    source = args.topics.resolve()/args.topic
    files = sorted(p for p in source.iterdir() if p.is_file() and not p.is_symlink())
    if not files:
        parser.error('selected topic must contain regular files')
    if (args.results/'runs.csv').exists():
        parser.error('choose a fresh results directory to preserve previous runs')
    args.results.mkdir(parents=True, exist_ok=True)
    manifest = []
    for p in files:
        manifest.append({'name':p.name, 'bytes':p.stat().st_size, 'sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
    chunks = sum((f['bytes']+65535)//65536 for f in manifest)
    metadata = {'kind':'Animals10 original image files; complete cane topic' if args.topic=='cane' else 'Animals10 original image files; complete selected topic',
                'topic':args.topic, 'file_count':len(files), 'bytes':sum(f['bytes'] for f in manifest),
                'data_chunks':chunks, 'files':manifest,
                'source_url':'https://www.kaggle.com/datasets/alessiocorrado99/animals10'}
    if args.provenance:
        metadata['download_provenance'] = json.loads(args.provenance.read_text())
    # Six predeclared probabilities, identical across all recovery cases.
    # The largest rate gives M*p <= 1.5 after rounding down.
    maximum = min(0.001, 1.5/max(1,chunks))
    base = 10**(-7)
    maximum = max(base, int(maximum/base)*base)
    probabilities = [0.0, maximum/10, maximum/4, maximum/2, maximum*3/4, maximum]
    parallel_probability = probabilities[2]
    metadata['probabilities'] = probabilities
    metadata['parallel_probability'] = parallel_probability
    (args.results/'animals10_manifest.json').write_text(json.dumps(metadata,indent=2)+'\n')
    print('Animals10 files:',len(files),'bytes:',metadata['bytes'],'chunks:',chunks,flush=True)
    print('Declared probabilities:',probabilities,flush=True)
    sys.argv = ['benchmark.py','--topics',str(args.topics.resolve()),'--topic',args.topic,
                '--results',str(args.results),'--repeats',str(args.repeats),
                '--probabilities',*[str(p) for p in probabilities],
                '--parallel-probability',str(parallel_probability),'--client-timeout','600']
    result = benchmark.main()
    (args.results/'dataset.json').write_text(json.dumps({k:v for k,v in metadata.items() if k!='files'},indent=2)+'\n')
    return result


if __name__=='__main__':
    raise SystemExit(main())
