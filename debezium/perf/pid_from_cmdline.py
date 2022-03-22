import argparse
import datetime as dt
import os
import re
import subprocess
import sys

parser = argparse.ArgumentParser(description='Match process command line regex to pid')
parser.add_argument(
    'proc_specs_strs',
    metavar='PROCSPEC',
    type=str, nargs='+',
    help='a string of the form "name:regex" where regex should only match one process in `ps -o command` output')

args = parser.parse_args()

proc_specs = {}
for proc_spec_str in args.proc_specs_strs:
    name, regex_str = proc_spec_str.split(':', maxsplit=1)
    proc_specs[name] = re.compile(regex_str)
    
ps_lines = subprocess.run(
    ['ps', '-ahxww', '-o', 'pid,command' ],
    stdout=subprocess.PIPE).stdout.decode('utf-8').splitlines()

matches = {}
nmatches = 0
my_pid = f'{os.getpid()}'

for ps_line in ps_lines:
    pid, cmd = ps_line.split(maxsplit=1)
    if pid == my_pid:
        continue
    for name, regex in proc_specs.items():
        if re.search(regex, cmd) is not None:
            prev = matches.get(name, None)
            if prev is not None:
                print(f"{sys.argv[0]}: found more than one match for '{name}': {prev}, {pid}, aborting",
                    file=sys.stderr)
                sys.exit(1)
            matches[name] = pid

for name in proc_specs.keys():
    if matches.get(name, None) is None:
        print(f"{sys.argv[0]}: couldn't find a match for {name}, aborting", file=sys.stderr)
        sys.exit(1)

first = True
for name, pid in matches.items():
    s = f'{name}:{pid}'
    if not first:
        s = ' ' + s
    print(s, end='')
    first = False
print()
