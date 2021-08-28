#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

import io
import string

import itertools

import numpy as np
import pyarrow as pa
from pyarrow import csv


def make_random_csv(num_cols=2, num_rows=10, linesep='\r\n', write_names=True, output_file='csv_data'):
    arr = np.random.RandomState(42).randint(0, 1000, size=(num_cols, num_rows))
    csv_writer = io.StringIO()
    col_names = list(itertools.islice(generate_col_names(), num_cols))
    if write_names:
        csv_writer.write(",".join(col_names))
        csv_writer.write(linesep)
    for row in arr.T:
        csv_writer.write(",".join(map(str, row)))
        csv_writer.write(linesep)
    csv_data = csv_writer.getvalue().encode()
    columns = [pa.array(a, type=pa.int64()) for a in arr]
    pa_table = pa.Table.from_arrays(columns, col_names)
    csv.write_csv(pa_table, output_file=output_file)
    # return csv_data, pa_table


def generate_col_names():
    # 'a', 'b'... 'z', then 'aa', 'ab'...
    letters = string.ascii_lowercase
    yield from letters
    for first in letters:
        for second in letters:
            yield first + second