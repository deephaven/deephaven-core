#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
import sys, glob, csv

# Convert java coverage CSV files to the normalized form from a multi-project
# root, merge, and write to the given CSV output path

input_glob = sys.argv[1] + '/**/build/reports/jacoco/java-coverage.csv'
output_path = sys.argv[2]

with open(output_path, 'w', newline='') as outfile:
    csv_writer = csv.writer(outfile)
    csv_writer.writerow(['Language','Project','Package','Class','Missed','Covered'])
    for filename in glob.glob(input_glob, recursive = True):
        with open(filename, 'r') as csv_in:
            csv_reader = csv.reader(csv_in)
            next(csv_reader, None)
            for row in csv_reader:
                new_row = ['java',row[0],row[1],row[2],row[3],row[4]]
                csv_writer.writerow(new_row)
