#!/usr/bin/env python3
"""Refresh embedded result tables/plots in the standalone editable LaTeX report.
Only named AUTO regions are replaced; group details and report prose are preserved.
"""
import argparse,csv,json,pathlib,re
ROOT=pathlib.Path(__file__).resolve().parents[1]
def main():
 parser=argparse.ArgumentParser();parser.add_argument('--results',type=pathlib.Path,default=ROOT/'Results/Animals10');args=parser.parse_args()
 rows=list(csv.DictReader((args.results/'summary.csv').open()))
 metadata=json.loads((args.results/'dataset.json').read_text())
 probabilities=metadata['probabilities'];high=probabilities[-1];middle=probabilities[-2];parallel=metadata['parallel_probability']
 def select(case,p=0,k=1,n=1,mode='multi'):
  return next(r for r in rows if r['case']==case and float(r['probability'])==p and int(r['connections'])==k and int(r['clients'])==n and r['mode']==mode)
 def val(r,k):return float(r[k+'_mean'])
 def fmt(x,d=2):return f'{x:.{d}f}'
 def row(*cells):return ' & '.join(map(str,cells))+r'\rowend'
 auto={}
 part1=[];plot=[]
 for i,n in enumerate([1,2,4,8],1):
  m=select('Part1',n=n);s=select('Part1',n=n,mode='single')
  part1.append(row(n,f'{int(val(m,"useful_bytes")):,}',f'{val(m,"control_bytes"):,.0f}',fmt(val(m,'seconds'),4),fmt(val(m,'goodput_MiB_s')),fmt(val(s,'goodput_MiB_s'))))
  plot.append(f'({i},{val(m,"goodput_MiB_s"):.8f}) +- (0,{float(m["goodput_MiB_s_std"]):.8f})')
 auto['part1']='\n'.join(part1)
 auto['part1plot']=r'\addplot+[fill=barblue,draw=barblue,error bars/error bar style={black}] coordinates {'+' '.join(plot)+'};'
 a=select('Part2_Case1',high);b=select('Part2_Case2',high)
 total=val(a,'payload_sent');useful=val(a,'useful_bytes');extra=val(a,'redundant_bytes')
 auto['case1detail']='\n'.join([
  row('Attempts',fmt(val(a,'attempts'))),row('Injected failures',fmt(val(a,'failures'))),
  row('Useful payload',f'{useful:,.0f} bytes'),row('Total payload sent',f'{total:,.0f} bytes'),
  row('Redundant payload',f'{extra:,.0f} bytes'),row('Application control overhead',f'{val(a,"control_bytes"):,.2f} bytes'),row('Redundant share of sent payload',fmt(100*extra/total)+r'\%'),
  row('Bytes sent per useful byte',fmt(total/useful)),row('Completion time',fmt(val(a,'seconds'),4)+' s'),
  row('Useful throughput',fmt(val(a,'goodput_MiB_s'))+' MiB/s')])
 auto['case1plot']=r'\addplot+[fill=barblue,draw=barblue] coordinates {(1,'+fmt(useful/1048576,8)+') ['+fmt(useful/1048576)+']};'+'\n'+r'\addplot+[fill=barorange,draw=barorange] coordinates {(2,'+fmt(extra/1048576,8)+') ['+fmt(extra/1048576)+']};'
 case2=[];plot=[]
 for i,p in enumerate([0,middle,high],1):
  r=select('Part2_Case2',p)
  case2.append(row(fmt(p*1e4,3),fmt(val(r,'failures')),f'{val(r,"redundant_bytes"):.0f}',f'{val(r,"control_bytes"):,.0f}',fmt(val(r,'seconds'),4),fmt(val(r,'goodput_MiB_s'))))
  plot.append(f'({i},{val(r,"goodput_MiB_s"):.8f}) +- (0,{float(r["goodput_MiB_s_std"]):.8f})')
 auto['case2results']='\n'.join(case2)
 auto['case2plot']=r'\addplot+[fill=barblue,draw=barblue,error bars/error bar style={black}] coordinates {'+' '.join(plot)+'};'
 auto['casecomparison']='\n'.join(row(label,fmt(val(a,key),d),fmt(val(b,key),d)) for label,key,d in [
  ('Attempts','attempts',2),('Payload sent (bytes)','payload_sent',0),('Redundant bytes','redundant_bytes',0),('Application control bytes','control_bytes',2),('Completion time (s)','seconds',4),('Useful throughput (MiB/s)','goodput_MiB_s',2)])
 auto['comparisonbytes']=r'\addplot+[fill=barblue,draw=barblue] coordinates {(1,'+fmt(total/1048576,8)+') (2,'+fmt(val(b,'payload_sent')/1048576,8)+')};'
 auto['comparisontime']=r'\addplot+[fill=barorange,draw=barorange] coordinates {(1,'+fmt(val(a,'seconds'),8)+') (2,'+fmt(val(b,'seconds'),8)+')};'
 enhanced=[];plot=[]
 for p in [0,middle,high]:
  r=select('Part2_Case2_Enhanced',p,k=4)
  enhanced.append(row(fmt(p*1e4,3),fmt(val(r,'failures')),f'{val(r,"control_bytes"):,.0f}',fmt(val(r,'seconds'),4),fmt(val(r,'goodput_MiB_s'))))
 auto['enhancedresults']='\n'.join(enhanced)
 for i,k in enumerate([1,2,4,8],1):
  r=select('Part2_Case2_Enhanced',parallel,k=k)
  plot.append(f'({i},{val(r,"goodput_MiB_s"):.8f}) +- (0,{float(r["goodput_MiB_s_std"]):.8f})')
 auto['enhancedplot']=r'\addplot+[fill=barblue,draw=barblue,error bars/error bar style={black}] coordinates {'+' '.join(plot)+'};'
 path=ROOT/'Report/main.tex';text=path.read_text()
 for key,content in auto.items():
  pattern=r'(% BEGIN AUTO:'+re.escape(key)+r'\n).*?(% END AUTO:'+re.escape(key)+r')'
  text,n=re.subn(pattern,lambda m:m[1]+content+'\n'+m[2],text,flags=re.S)
  assert n==1,key
 path.write_text(text)
 print('Updated 11 result regions from recorded CSVs; group details and prose retained.')
if __name__=='__main__':main()
