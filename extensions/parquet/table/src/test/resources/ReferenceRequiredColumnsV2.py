"""Writes ReferenceRequiredColumnsV2.parquet for SparsePageReadTest and
ParquetTableReadWriteTest.testReadUncompressedParquetV2Pages.

SparsePageReadTest.generateRequiredColumnsV1File writes the V1 file, but parquet-java can't write this one: it always
writes an offset index. This file has non-nullable columns, so pages carry no
definition levels, in snappy V2 pages and with no offset index. pyarrow stores a V2 page's values uncompressed
(`is_compressed=false`) when compressing doesn't shrink them: with pyarrow 25.0.1, every page of `I` is stored that
way, and the other columns' pages are compressed.

To regenerate the file:

1. Requirements: `python3.10` (pyarrow 25.0.1 needs Python 3.10 or later) with its `venv` module (on Debian/Ubuntu,
   `apt install python3.10-venv`), and Git LFS installed in this clone (`git lfs install`).

2. Create a throwaway virtual environment holding pyarrow, pinned to the version that wrote the checked-in file so
   that the output stays byte-for-byte reproducible:

       python3.10 -m venv /tmp/pyarrow-venv
       /tmp/pyarrow-venv/bin/pip install pyarrow==25.0.1

3. Run this script from its own directory; it writes `ReferenceRequiredColumnsV2.parquet` next to itself:

       cd extensions/parquet/table/src/test/resources
       /tmp/pyarrow-venv/bin/python ReferenceRequiredColumnsV2.py

4. From the repository root, check that the file still reads as expected. The second test is in the OutOfBand
   category, so it runs only under `testOutOfBand`:

       ./gradlew :extensions-parquet-table:test --tests '*SparsePageReadTest'
       ./gradlew :extensions-parquet-table:testOutOfBand \
           --tests '*ParquetTableReadWriteTest.testReadUncompressedParquetV2Pages'

5. Commit the new file; `git status` shows it modified, and Git LFS stores it. Afterwards, remove the environment
   with `rm -rf /tmp/pyarrow-venv`.

A newer pyarrow may choose differently which pages to store uncompressed. If `I` no longer has such pages,
`testReadUncompressedParquetV2Pages` stops covering them, so prefer the pinned version.
"""

import pyarrow as pa
import pyarrow.parquet as pq

N = 5_000

table = pa.table(
    {
        "I": pa.array(range(N), pa.int32()),
        "L": pa.array([ii * 3 for ii in range(N)], pa.int64()),
        "D": pa.array([ii / 3.0 for ii in range(N)], pa.float64()),
        "Str": pa.array([f"value-{ii}" for ii in range(N)], pa.string()),
        "Sym": pa.array([f"sym-{ii % 50}" for ii in range(N)], pa.string()),
    }
)
table = table.cast(pa.schema([pa.field(f.name, f.type, nullable=False) for f in table.schema]))

pq.write_table(
    table,
    "ReferenceRequiredColumnsV2.parquet",
    data_page_version="2.0",
    data_page_size=4 * 1024,
    use_dictionary=["Sym"],
    write_page_index=False,
    compression="snappy",
)
