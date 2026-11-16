---
title: Parquet cheat sheet
---

This cheat sheet shows common ways to read and write Parquet files. The examples use these functions from the [`deephaven.parquet`](/core/pydoc/code/deephaven.parquet.html) module:

- [`read`](../data-import-export/Parquet/readTable.md)
- [`write`](../data-import-export/Parquet/writeTable.md)
- [`write_partitioned`](../data-import-export/Parquet/writePartitioned.md)

The examples read and write files under `/data`, the data directory in Deephaven's Docker images. To map it to a directory on your computer, see [Docker bind mounts](../../conceptual/docker-data-volumes.md). In other deployments, use any path the server can read and write.

## Create a table

The examples on this page write and read this sample table.

```python test-set=1 order=source
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("X", ["A", "B", "B", "C", "B", "A", "B", "B", "C"]),
        int_col("Y", [2, 4, 2, 1, 2, 3, 4, 2, 3]),
        int_col("Z", [55, 76, 20, 4, 230, 50, 73, 137, 214]),
    ]
)
```

## Write Parquet files

This block writes `source` to a single file twice, once with the default codec and once with GZIP, and then writes it as a key-value partitioned directory.

```python test-set=1 order=null
from deephaven import parquet

# Write to a local file (SNAPPY compression by default)
parquet.write(source, "/data/output.parquet")

# Write to a local file with GZIP compression
parquet.write(source, "/data/output_GZIP.parquet", compression_codec_name="GZIP")

# Write a partitioned table to a key-value partitioned directory (X=A/, X=B/, ...)
# base_name="data" names each file data.parquet, so a rerun overwrites the files instead of adding more
parquet.write_partitioned(
    source.partition_by("X"), "/data/partitioned_output", base_name="data"
)
```

### Compression codecs

Pass one of these values as the `compression_codec_name` argument to [`write`](../data-import-export/Parquet/writeTable.md) or [`write_partitioned`](../data-import-export/Parquet/writePartitioned.md). They all work in a default installation. `BROTLI` also exists, but it needs a Brotli codec added to the server classpath. See [Write Parquet files](../../how-to-guides/data-import-export/parquet-export.md).

- `SNAPPY` (default): A codec based on Google's [Snappy compression format](https://github.com/google/snappy/blob/main/format_description.txt) that aims for high speed and reasonable compression.
- `UNCOMPRESSED`: No compression.
- `LZ4_RAW`: A codec based on the [LZ4 block format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md).
- `LZ4`: A deprecated codec. Use `LZ4_RAW` instead.
- `LZO`: A codec based on or interoperable with the [LZO compression library](https://www.oberhumer.com/opensource/lzo/).
- `GZIP`: A codec based on the GZIP format defined by [RFC 1952](https://tools.ietf.org/html/rfc1952). It differs from the related zlib and deflate formats.
- `ZSTD`: A codec based on the Zstandard format defined by [RFC 8478](https://tools.ietf.org/html/rfc8478) that aims for a high compression ratio.

## Read Parquet files

The `pems` example reads the PEMS traffic dataset at `/data/examples/Pems/parquet/pems` from [Deephaven's example data](https://github.com/deephaven/examples). To download it, see [Find example data](../../getting-started/docker-install.md#find-example-data).

```python test-set=1 order=source_read,source_read_gzip,pems,partitioned
from deephaven import parquet

# Read from a local file
source_read = parquet.read("/data/output.parquet")

# Read a GZIP-compressed file
source_read_gzip = parquet.read("/data/output_GZIP.parquet")

# Read a directory that has a _metadata file
pems = parquet.read("/data/examples/Pems/parquet/pems")

# Read a key-value partitioned directory
partitioned = parquet.read("/data/partitioned_output")
```

[`read`](../data-import-export/Parquet/readTable.md) detects the codec from the file, so reading `/data/output_GZIP.parquet` needs no codec argument.

### How `read` handles a directory

When you pass a directory to [`read`](../data-import-export/Parquet/readTable.md), it finds the data files, the schema, and the partitioning columns in one of two ways, depending on whether the directory has a [`_metadata` file](../../how-to-guides/data-import-export/parquet-formats.md#metadata-partitioned-directories).

With a `_metadata` file:

- [`read`](../data-import-export/Parquet/readTable.md) takes the data files and the schema from `_metadata`.
- The partitioning columns are the columns that the `_common_metadata` file in the same directory lists and `_metadata` does not. Without `_common_metadata`, the `key=value` directories do not become columns unless you pass a `table_definition`.
- The PEMS directory has both files, but its `_common_metadata` lists no columns beyond those in `_metadata`, so its `station=...` directories do not become a column.

Without a `_metadata` file:

- [`read`](../data-import-export/Parquet/readTable.md) loads every `.parquet` file under the directory and skips names that start with `.`.
- It takes the schema from one of those files, so all the files should share a schema.
- Each `key=value` subdirectory becomes a partitioning column, and every subdirectory that holds Parquet files must use a `key=value` name. Reading `/data/partitioned_output` adds the partitioning column `X` from the `X=A/`, `X=B/`, and `X=C/` directories that [`write_partitioned`](../data-import-export/Parquet/writePartitioned.md) created.

### Read a directory that receives new files

Pass `is_refreshing=True` to [`read`](../data-import-export/Parquet/readTable.md) to get a [refreshing table](../../conceptual/table-types.md) that picks up new Parquet files added to a directory and adds their rows. Add data as new files rather than changing files that have already been read. Refreshing reads don't work on a single file or with the metadata-file layout, which `read` uses when the directory has a `_metadata` file and no `file_layout` is set. To read such a directory as a refreshing table, pass `file_layout` set to [`ParquetFileLayout.KV_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout) or `ParquetFileLayout.FLAT_PARTITIONED`. See [Refreshing tables](../data-import-export/Parquet/readTable.md#refreshing-tables) for an example that adds files.

```python test-set=1 order=refreshing
from deephaven import parquet

refreshing = parquet.read("/data/partitioned_output", is_refreshing=True)
```

### Read legacy string columns

Some Parquet writers store string columns as byte-array columns (`BINARY` or `FIXED_LEN_BYTE_ARRAY`) with no logical type annotation. To read those columns as strings instead of byte arrays, pass `is_legacy_parquet=True` to [`read`](../data-import-export/Parquet/readTable.md), as in `parquet.read(path, is_legacy_parquet=True)`.

## More options

The how-to guides cover more options: reading from [S3](../../how-to-guides/data-import-export/parquet-import.md#from-s3) and writing to [S3](../../how-to-guides/data-import-export/parquet-export.md#to-s3), setting the [`file_layout` and `table_definition`](../../how-to-guides/data-import-export/parquet-import.md#arguments) arguments, and [generating metadata files](../../how-to-guides/data-import-export/parquet-export.md#optional-arguments).

## Related documentation

- [Docker bind mounts](../../conceptual/docker-data-volumes.md)
- [Install and run with Docker](../../getting-started/docker-install.md)
- [Read Parquet files](../../how-to-guides/data-import-export/parquet-import.md)
- [Write Parquet files](../../how-to-guides/data-import-export/parquet-export.md)
- [Supported Parquet formats](../../how-to-guides/data-import-export/parquet-formats.md)
- [`read`](../data-import-export/Parquet/readTable.md)
- [`write`](../data-import-export/Parquet/writeTable.md)
- [`write_partitioned`](../data-import-export/Parquet/writePartitioned.md)
- [`new_table`](../table-operations/create/newTable.md)
- [`partition_by`](../table-operations/group-and-aggregate/partitionBy.md)
- [Pydoc](/core/pydoc/code/deephaven.parquet.html)
