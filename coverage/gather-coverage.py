#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
import sys, glob, csv, os, shutil

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

# Aggregate and normalize coverage for java projects
print("Aggregating Java Coverage")
input_glob = proj_root_dir + '/build/reports/jacoco/jacoco-merge/jacoco-merge.csv'
with open(f'{coverage_dir}/java-coverage.csv', 'w', newline='') as outfile:
    csv_writer = csv.writer(outfile)
    csv_writer.writerow(['Language','Package','Class','Missed','Covered'])
    for filename in glob.glob(input_glob, recursive = True):
        with open(filename, 'r') as csv_in:
            csv_reader = csv.reader(csv_in)
            next(csv_reader, None)
            for row in csv_reader:
                new_row = ['java',row[1],row[2],row[3],row[4]]
                csv_writer.writerow(new_row)

# Load packages to be excluded from the aggregated coverage CSV
with open(exclude_path) as f:
    excludes = [line.strip() for line in f]

# Collect coverage CSVs into a single CSV without lines containing exclusions
with open(coverage_output_path, 'w', newline='') as outfile:
    csv_writer = csv.writer(outfile)
    for csv_file in glob.glob(coverage_input_glob):
        if os.path.basename(csv_file) == "all-coverage.csv": continue
        print('Merging', os.path.basename(csv_file))
        with open(csv_file, 'r') as csv_in:
            for row in csv.reader(csv_in):
                if row[1] in excludes: continue
                new_row = [row[0],row[1],row[2],row[3],row[4]]
                csv_writer.writerow(new_row)
