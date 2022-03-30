import argparse
import datetime as dt
import math
import re
import os
import subprocess
import statistics as stats
import sys

now_str = dt.datetime.utcnow().astimezone().strftime('%Y.%m.%d.%H.%M.%S_%Z')

parser = argparse.ArgumentParser(description='Sample cpu utilization and memory consumption with top for given processes')

perf_tag = os.environ.get('PERF_TAG', None)
if perf_tag is None:
    print(f'{os.argv[0]}: PERF_TAG environment variable is not defined, aborting.', file=sys.stderr)
    sys.exit(1)

parser.add_argument('nsamples', metavar='NSAMPLES', type=int,
                    help='number of samples')
parser.add_argument('delay_s', metavar='DELAY_MS', type=str,
                    help='delay between samples, in seconds')
parser.add_argument('proc_specs_strs', metavar='PROCSPEC', type=str, nargs='+',
                    help='a string of the form "name:pid"')

args = parser.parse_args()

proc_specs = {}
pids = []
pids_args = []
name_pidre = {}
names = []
for proc_spec_str in args.proc_specs_strs:
    name, pid = proc_spec_str.split(':', maxsplit=1)
    names.append(name)
    pids.append(pid)
    pids_args.append('-p')
    pids_args.append(pid)
    name_pidre[name] = re.compile(f'^{pid} ')
    proc_specs[name] = pid

names.sort()
top_out = subprocess.run(
    ['top', '-Eg', '-n', f'{args.nsamples}', '-b', '-c', '-d', f'{args.delay_s}'] + pids_args,
    stdout=subprocess.PIPE).stdout.decode('utf-8').splitlines()

out_dir = f'./logs/{perf_tag}'
with open(f'{out_dir}/{now_str}_top_details.log', 'w') as f:
    for line in top_out:
        f.write(line)
        f.write('\n')

cputag = 'CPU_PCT'   # CPU utilization in percentage
restag = 'RES_GiB'   # Resident size in GiB
tags = [cputag, restag]

kib_2_gib = 1.0/(1024 * 1024)

results={}
for line in top_out:
    line = line.strip()
    for name, regex in name_pidre.items():
        if re.search(regex, line) is not None:
            cols = line.split(maxsplit=12)
            pid = proc_specs[name]
            d = results.get(name, None)
            if d is None:
                d = results[name] = { cputag : [], restag : [] }
            cpu = float(cols[8])
            d[cputag].append(cpu)
            res_str = cols[5]
            if res_str.endswith('g'):
                res_kib = float(res_str[:-1])*1024*1024
            elif res_str.endswith('m'):
                res_kib = float(res_str[:-1])*1024
            else:
                res_kib = int(res_str)
            d[restag].append(res_kib * kib_2_gib)

def format_samples(precision : int, samples):
    first = True
    s = ''
    for sample in samples:
        if not first:
            s += ', '
        if precision != -1:
            s += f'{sample:.{precision}}'
        else:
            s += f'{sample}'
        first = False
    return s

with open(f'{out_dir}/{now_str}_top_samples.log', 'w') as f:
    line = f'nsamples={args.nsamples}, delay_s={args.delay_s}'
    print(line)
    f.write(line + '\n')
    for name in names:
        result = results[name]
        for tag in tags:
            samples = result[tag]
            mean = stats.mean(samples)
            sd = stats.stdev(samples, mean)
            sem = sd / math.sqrt(len(samples))
            precision = 4 if tag == restag else -1
            samples_str = format_samples(precision, samples)
            line = f'name={name}, tag={tag}, mean={mean:.2f}, sd={sd:.2f}, sem={sem:.2f}, samples=[{samples_str}]'
            print(line)
            f.write(line + '\n')
