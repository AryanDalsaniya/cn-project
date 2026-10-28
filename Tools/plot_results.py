#!/usr/bin/env python3
"""Create CSV summaries and labelled figures from actual recorded measurements."""
import argparse,csv,json,pathlib,statistics
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from collections import defaultdict
ROOT=pathlib.Path(__file__).resolve().parents[1]
LABELS={'Part2_Case1':'Case 1: restart','Part2_Case2':'Case 2: resume','Part2_Case2_Enhanced':'Enhanced: 4 connections'}
COLORS=['#203864','#167d8d','#d88723']

def main():
 p=argparse.ArgumentParser();p.add_argument('--results',type=pathlib.Path,default=ROOT/'Results'/'Local');a=p.parse_args()
 data=list(csv.DictReader((a.results/'runs.csv').open()));groups=defaultdict(list)
 for row in data:
  for k in ['clients','connections','repeat']:row[k]=int(row[k])
  for k in ['probability','seconds','goodput_MiB_s','sender_rate_MiB_s','redundancy_percent','redundant_bytes','control_bytes','payload_sent','useful_bytes','failures','attempts']:row[k]=float(row[k])
  groups[tuple(row[k] for k in ['case','mode','clients','probability','connections'])].append(row)
 summaries=[]
 for key,rs in groups.items():
  s=dict(zip(['case','mode','clients','probability','connections'],key));s['runs']=len(rs)
  for f in ['seconds','goodput_MiB_s','sender_rate_MiB_s','redundancy_percent','redundant_bytes','control_bytes','payload_sent','useful_bytes','failures','attempts']:
   vals=[r[f] for r in rs];s[f+'_mean']=statistics.mean(vals);s[f+'_std']=statistics.stdev(vals) if len(vals)>1 else 0
  summaries.append(s)
 with (a.results/'summary.csv').open('w',newline='') as f:
  w=csv.DictWriter(f,fieldnames=list(summaries[0]));w.writeheader();w.writerows(summaries)
 figdir=a.results/'figures';figdir.mkdir(exist_ok=True)
 environment=json.loads((a.results/'environment.json').read_text());local='loopback' in environment['topology']
 dataset=json.loads((a.results/'dataset.json').read_text()) if (a.results/'dataset.json').exists() else {}
 descriptor=(f'{dataset["bytes"]/1048576:g} MiB synthetic topic' if dataset.get('kind','').startswith('synthetic') and 'bytes' in dataset else 'user-provided topic; see metadata')
 subtitle=('Single-host loopback' if local else 'Three computers')+' | '+descriptor+f' | mean +/- sample SD, n={environment.get("repeats","?")}'
 plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'axes.titleweight':'bold','figure.dpi':150,'savefig.dpi':180})
 def finish(fig,ax,name,title,ylabel):
  ax.set_title(title,pad=22);fig.text(.5,.89,subtitle,ha='center',fontsize=8,color='#555555');ax.set_ylabel(ylabel);ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
  fig.tight_layout(rect=[0,0,1,.92]);fig.savefig(figdir/(name+'.png'),bbox_inches='tight');fig.savefig(figdir/(name+'.pdf'),bbox_inches='tight');plt.close(fig)
 for field,title,ylabel,suffix in [('goodput_MiB_s','Part I: useful aggregate throughput','Useful throughput (MiB/s)','throughput'),('sender_rate_MiB_s','Part I: aggregate payload send rate','Payload send rate (MiB/s)','sender_rate'),('control_bytes','Part I: application protocol overhead','Application control bytes','overhead')]:
  fig,ax=plt.subplots(figsize=(7.2,4.2))
  for mode,c in zip(['single','multi'],COLORS):
   rows=sorted([s for s in summaries if s['case']=='Part1' and s['mode']==mode],key=lambda s:s['clients'])
   ax.errorbar([s['clients'] for s in rows],[s[field+'_mean'] for s in rows],yerr=[s[field+'_std'] for s in rows],label=mode.capitalize()+' threaded',color=c,marker='o',capsize=3)
  ax.set_xlabel('Concurrent client processes');ax.legend();finish(fig,ax,'Part1_'+suffix,title,ylabel)
 for field,title,ylabel,suffix in [('goodput_MiB_s','Part II: throughput under connection failures','Useful throughput (MiB/s)','throughput'),('redundancy_percent','Part II: redundant payload overhead','Redundant payload / useful payload (%)','overhead'),('seconds','Part II: completion time','Completion time (s)','completion'),('control_bytes','Part II: application control traffic','Control traffic (bytes, both directions)','control')]:
  fig,ax=plt.subplots(figsize=(7.2,4.2))
  for case,color in zip(LABELS,COLORS):
   rows=sorted([s for s in summaries if s['case']==case and s['connections']==(4 if case.endswith('Enhanced') else 1)],key=lambda s:s['probability'])
   ax.errorbar([s['probability'] for s in rows],[s[field+'_mean'] for s in rows],yerr=[s[field+'_std'] for s in rows],label=LABELS[case],color=color,marker='o',capsize=3)
  ax.set_xlabel('Failure probability per acknowledged DATA chunk');ax.legend(fontsize=8);finish(fig,ax,'Part2_'+suffix,title,ylabel)
 # Separate named copies satisfy per-case result discovery without combining run files.
 for case in LABELS:
  with (a.results/(case+'_runs.csv')).open('w',newline='') as f:
   rows=[r for r in data if r['case']==case];w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
  for field,suffix,ylabel in [('goodput_MiB_s','throughput','Useful throughput (MiB/s)'),('redundancy_percent','overhead','Redundant payload (%)')]:
   rs=sorted([s for s in summaries if s['case']==case and s['connections']==(4 if case.endswith('Enhanced') else 1)],key=lambda s:s['probability'])
   fig,ax=plt.subplots(figsize=(7.2,4.2));ax.errorbar([r['probability'] for r in rs],[r[field+'_mean'] for r in rs],yerr=[r[field+'_std'] for r in rs],marker='o',capsize=3,color=COLORS[list(LABELS).index(case)])
   ax.set_xlabel('Failure probability per acknowledged DATA chunk');finish(fig,ax,case+'_'+suffix,LABELS[case],ylabel)
 parallel_probs={s['probability'] for s in summaries if s['case'].endswith('Enhanced')}
 parallel_p=max(parallel_probs,key=lambda prob:len({s['connections'] for s in summaries if s['case'].endswith('Enhanced') and s['probability']==prob})) if parallel_probs else None
 rs=sorted([s for s in summaries if s['case'].endswith('Enhanced') and s['probability']==parallel_p],key=lambda s:s['connections'])
 if rs:
  fig,ax=plt.subplots(figsize=(7.2,4.2));ax.errorbar([r['connections'] for r in rs],[r['goodput_MiB_s_mean'] for r in rs],yerr=[r['goodput_MiB_s_std'] for r in rs],marker='o',capsize=3,color=COLORS[2]);ax.set_xlabel('Parallel connections per client');ax.set_xticks([1,2,4,8]);finish(fig,ax,'Enhanced_connections',f'Enhanced case: parallelism at p = {parallel_p:g}','Useful throughput (MiB/s)')
 print(f'{len(data)} measured runs; {len(summaries)} configurations; figures in {figdir}')
if __name__=='__main__':main()
