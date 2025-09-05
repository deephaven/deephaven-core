---
title: Parquet Cheat Sheet
sidebar_label: Parquet
---

- [`read`](../data-import-export/Parquet/readTable.md)
- [`write`](../data-import-export/Parquet/writeTable.md)

Optional instructions for customizations while writing. Valid values are:

- `SNAPPY`: Aims for high speed and a reasonable amount of compression. Based on [Google](https://github.com/google/snappy/blob/main/format_description.txt)'s Snappy compression format. If `ParquetInstructions` is not specified, defaults to `SNAPPY`.
- `UNCOMPRESSED`: The output will not be compressed.
- `LZ4_RAW`: A codec based on the [LZ4 block format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md). Should always be used instead of `LZ4`.
- `LZ4`: **Deprecated** Compression codec loosely based on the [LZ4 compression algorithm](https://github.com/lz4/lz4), but with an additional undocumented framing scheme. The framing is part of the original Hadoop compression library and was historically copied first in parquet-mr, then emulated with mixed results by parquet-cpp. Note that `LZ4` is deprecated; use `LZ4_RAW` instead.
- `LZO`: Compression codec based on or interoperable with the [LZO compression library](https://www.oberhumer.com/opensource/lzo/).
- `GZIP`: Compression codec based on the GZIP format (not the closely-related "zlib" or "deflate" formats) defined by [RFC 1952](https://tools.ietf.org/html/rfc1952).
- `ZSTD`: Compression codec with the highest compression ratio based on the Zstandard format defined by [RFC 8478](https://tools.ietf.org/html/rfc8478).

Reading instructions have all the above plus `LEGACY` avaialable:

- `LEGACY`: Load any binary fields as strings. Helpful to load files written in older versions of Parquet that lacked a distinction between binary and string.

```python order=source,source_read
# Create a table
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven import parquet as parquet

source = new_table(
    [
        string_col("X", ["A", "B", "B", "C", "B", "A", "B", "B", "C"]),
        int_col("Y", [2, 4, 2, 1, 2, 3, 4, 2, 3]),
        int_col("Z", [55, 76, 20, 4, 230, 50, 73, 137, 214]),
    ]
)

# Write to a local file
parquet.write(source, "/data/output.parquet")

# Read from a local file
source_read = parquet.read("/data/output.parquet")
```

## Related documentation

- [Parquet import](../../how-to-guides/data-import-export/parquet-import.md)
- [Parquet export](../../how-to-guides/data-import-export/parquet-export.md)
- [`read`](../data-import-export/Parquet/readTable.md)
- [`write`](../data-import-export/Parquet/writeTable.md)
- [Docker data volumes](../../conceptual/docker-data-volumes.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#readTable(java.lang.String))
- [Pydoc](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write)
