#!/usr/bin/env python3
"""Deterministic synthetic data for reproducible tests; this is not Animals10."""
import argparse, hashlib, json, pathlib, random

def generate(root, mib=2, files=8, seed=97112):
    root=pathlib.Path(root);topic=root/'Cane';topic.mkdir(parents=True,exist_ok=True)
    if list(topic.iterdir()): raise ValueError('Refusing to overwrite a nonempty topic directory')
    rng=random.Random(seed);total=int(mib*1024*1024);manifest=[]
    for i in range(files):
        n=total//files+(i<total%files);p=topic/f'image_{i:03d}.bin'
        with open(p,'wb') as out:
            remain=n
            while remain:
                b=rng.randbytes(min(remain,1048576));out.write(b);remain-=len(b)
        manifest.append({'name':p.name,'bytes':n,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
    metadata={'kind':'synthetic binary files; not Animals10','seed':seed,'topic':'Cane','bytes':total,'files':manifest}
    (root/'manifest.json').write_text(json.dumps(metadata,indent=2)+'\n');return metadata
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root');p.add_argument('--mib',type=float,default=2);p.add_argument('--files',type=int,default=8);p.add_argument('--seed',type=int,default=97112);a=p.parse_args()
    if a.mib<=0 or a.files<1:p.error('positive size and file count required')
    print(json.dumps(generate(a.root,a.mib,a.files,a.seed),indent=2))
