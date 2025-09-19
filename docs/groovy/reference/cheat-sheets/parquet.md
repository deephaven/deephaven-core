---
title: Parquet Cheat Sheet
sidebar_label: Parquet
---

- [`readTable`](../data-import-export/Parquet/readTable.md)
- [`writeTable`](../data-import-export/Parquet/writeTable.md)

Optional instructions for customizations while writing. Valid values are:

- `SNAPPY`: Aims for high speed, and a reasonable amount of compression. Based on [Google](https://github.com/google/snappy/blob/main/format_description.txt)'s Snappy compression format. If `ParquetInstructions` is not specified, it defaults to `SNAPPY`.
- `UNCOMPRESSED`: The output will not be compressed.
- `LZ4_RAW`: A codec based on the [LZ4 block format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md). Should always be used instead of `LZ4`.
- `LZ4`: **Deprecated** Compression codec loosely based on the [LZ4 compression algorithm](https://github.com/lz4/lz4), but with an additional undocumented framing scheme. The framing is part of the original Hadoop compression library and was historically copied first in parquet-mr, then emulated with mixed results by parquet-cpp. Note that `LZ4` is deprecated; use `LZ4_RAW` instead.
- `LZO`: Compression codec based on or interoperable with the [LZO compression library](https://www.oberhumer.com/opensource/lzo/).
- `GZIP`: Compression codec based on the GZIP format (not the closely-related "zlib" or "deflate" formats) defined by [RFC 1952](https://tools.ietf.org/html/rfc1952).
- `ZSTD`: Compression codec with the highest compression ratio based on the Zstandard format defined by [RFC 8478](https://tools.ietf.org/html/rfc8478).

Reading instructions have all the above plus `LEGACY` avaialable:

- `LEGACY`: Load any binary fields as strings. Helpful to load files written in older versions of Parquet that lacked a distinction between binary and string.

```groovy skip-test
// Create a table
source = newTable(
    stringCol("X", "A", "B",  "B", "C", "B", "A", "B", "B", "C"),
    intCol("Y",2, 4, 2, 1, 2, 3, 4, 2, 3),
    intCol("Z", 55, 76, 20, 4, 230, 50, 73, 137, 214),
)

// Write to a local file
import static io.deephaven.parquet.table.ParquetTools.writeTable

writeTable(source, new File("/data/output.parquet"))

// Write to a local file with compression
writeTable(source, new File("/data/output_GZIP.parquet"), ParquetTools.GZIP)

// Read from a local file
import static io.deephaven.parquet.table.ParquetTools.readTable
source = readTable("/data/output.parquet")

// Read from a local compressed file
source = readTable("/data/output_GZIP.parquet", ParquetTools.GZIP)

// Read en entire directory or parquet files
// Only files with a `.parquet` extension or `_common_metadata` and `_metadata` files should be located in these directories.
// All files ending with `.parquet` need the same schema.
source = readTable("/data/examples/Pems/parquet/pems")
```

## Related documentation

- [Import Parquet files](../../how-to-guides/data-import-export/parquet-import.md)
- [Export Parquet files](../../how-to-guides/data-import-export/parquet-export.md)
- [`readTable`](../data-import-export/Parquet/readTable.md)
- [`writeTable`](../data-import-export/Parquet/writeTable.md)
- [Docker data volumes](../../conceptual/docker-data-volumes.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#readTable(java.lang.String))
