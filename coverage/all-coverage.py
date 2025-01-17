#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
import sys, glob, csv, os, subprocess

# Aggregate coverage data for all languages. Each language has a different way of doing 
# coverage and each normalization mechanism is called here. Class/file exclusions are
# handled here, since coverage tools are inconsistent or non-functional in that regard.

proj_root_dir = sys.argv[1]
script_dir = os.path.dirname(os.path.abspath(__file__))
coverage_dir = proj_root_dir + '/build/reports/coverage'
coverage_output_path = coverage_dir + '/all-coverage.csv'
coverage_input_glob = coverage_dir + '/*-coverage.csv'
exclude_path = script_dir + '/exclude-packages.txt'

if os.path.exists(coverage_output_path):
    os.remove(coverage_output_path)
  
def pycall(lang):
    lang_cov = f'{lang}-coverage'
    cmd = f'python {script_dir}/{lang_cov}.py {proj_root_dir} {coverage_dir}/{lang_cov}.csv'
    result = subprocess.check_output(cmd, shell=True, text=True)
    print(result)

# Aggregate and normalize coverage for projects that use each language
pycall('java')
#pycall('python')

# Load packages to be excluded from the aggregated coverage CSV
with open(exclude_path) as f:
    excludes = [line.strip() for line in f]

# Collect coverage CSVs into a single CSV without lines containing exclusions
with open(coverage_output_path, 'w', newline='') as outfile:
    csv_writer = csv.writer(outfile)
    for csv_file in glob.glob(coverage_input_glob):
        with open(csv_file, 'r') as csv_in:
            for row in csv.reader(csv_in):
                if row[2] in excludes: continue
                new_row = [row[0],row[1],row[2],row[3],row[4],row[5]]
                csv_writer.writerow(new_row)

