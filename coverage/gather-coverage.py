#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
import sys, glob, csv, os, shutil, re

# Aggregate coverage data for all languages. Each language has a different way of doing 
# coverage and each normalization mechanism is used here. Class/file exclusions are
# handled here, since coverage tools are inconsistent or non-functional in that regard.

proj_root_dir = sys.argv[1]
script_dir = os.path.dirname(os.path.abspath(__file__))
coverage_dir = proj_root_dir + '/build/reports/coverage'
coverage_output_path = coverage_dir + '/all-coverage.csv'
coverage_input_glob = coverage_dir + '/*-coverage.csv'
exclude_path = script_dir + '/exclude-packages.txt'

if os.path.exists(coverage_dir):
    shutil.rmtree(coverage_dir)
os.makedirs(coverage_dir)

coverage_header = ['Language','Package','Class','Missed','Covered']

# Aggregate and normalize coverage for Java projects
print("Aggregating Java Coverage")
input_glob = proj_root_dir + '/build/reports/java/java-coverage.csv'
with open(f'{coverage_dir}/java-coverage.csv', 'w', newline='') as outfile:
    csv_writer = csv.writer(outfile)
    csv_writer.writerow(coverage_header)
    for filename in glob.glob(input_glob, recursive = True):
        with open(filename, 'r') as csv_in:
            csv_reader = csv.reader(csv_in)
            next(csv_reader, None)
            for row in csv_reader:
                new_row = ['java',row[1],row[2],row[3],row[4]]
                csv_writer.writerow(new_row)

# Aggregate and normalize coverage for Python projects
print("Aggregating Python Coverage")
input_glob = proj_root_dir + '/build/reports/python/python-coverage.tsv'
with open(f'{coverage_dir}/python-coverage.csv', 'w', newline='') as outfile:
    csv_writer = csv.writer(outfile)
    csv_writer.writerow(coverage_header)
    for filename in glob.glob(input_glob, recursive = True):
        with open(filename, 'r') as tsv_in:
            for line in tsv_in:
                line = line.strip()
                if line == '' or line.startswith('---') or line.startswith('TOTAL'): continue
                row = re.split(r'\s+', line)
                if row[0] == 'Name' and row[1] == 'Stmts': continue
                new_row = ['python', os.path.dirname(row[0]), os.path.basename(row[0]),
                    row[2], str(int(row[1]) - int(row[2]))]
                csv_writer.writerow(new_row)


# Load packages to be excluded from the aggregated coverage CSV
with open(exclude_path) as f:
    excludes = [line.strip() for line in f]

# Collect coverage CSVs into a single CSV without lines containing exclusions
with open(coverage_output_path, 'w', newline='') as outfile:
    do_header = True
    csv_writer = csv.writer(outfile)
    for csv_file in glob.glob(coverage_input_glob):
        if os.path.basename(csv_file) == "all-coverage.csv": continue
        print('Merging', os.path.basename(csv_file))
        with open(csv_file, 'r') as csv_in:
            for row in csv.reader(csv_in):
                if row[1] in excludes: continue
                if not do_header and row[0] == 'Language' and row[1] == 'Package': continue
                new_row = [row[0],row[1],row[2],row[3],row[4]]
                csv_writer.writerow(new_row)
        do_header = False
