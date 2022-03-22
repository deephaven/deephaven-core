import argparse
import datetime as dt
import re
import subprocess
import statistics as stats

now_str = dt.datetime.now().astimezone().strftime('%Y%m%d%H%M%S_%Z')

parser = argparse.ArgumentParser(description='Sample cpu utilization and memory consumption with top for given processes')

parser.add_argument('tag', metavar='TAG', type=str,
                    help='experiment tag')
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
for proc_spec_str in args.proc_specs_strs:
    name, pid = proc_spec_str.split(':', maxsplit=1)
    pids.append(pid)
    pids_args.append('-p')
    pids_args.append(pid)
    name_pidre[name] = re.compile(f'^{pid} ')
    proc_specs[name] = pid

top_out = subprocess.run(
    ['top', '-Eg', '-n', f'{args.nsamples}', '-b', '-c', '-d', f'{args.delay_s}'] + pids_args,
    stdout=subprocess.PIPE).stdout.decode('utf-8').splitlines()

with open(f'logs/{now_str}_{args.tag}_top_details.log', 'w') as f:
    for line in top_out:
        f.write(line)
        f.write('\n')

cputag = 'CPU_PCT'
restag = 'RES_GiB'

kib_2_gib = 1.0/(1024 * 1024)

results={}
for line in top_out:
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

def format_samples(precision, samples):
    first = True
    s = ''
    for sample in samples:
        if not first:
            s += ', '
        s += f'%.{precision}f' % sample
        first = False

with open(f'logs/{now_str}_{args.tag}_top_samples.log', 'w') as f:
    for name, result in results.items():
        for tag, samples in result.items():
            mean = stats.mean(samples)
            samples_str = format_samples(samples)
            line = f'name={name}, tag={tag}, mean={mean:.2f}, samples={samples_str}'
            print(line)
            f.write(line + '\n')
