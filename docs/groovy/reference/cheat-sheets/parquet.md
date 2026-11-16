---
title: Parquet cheat sheet
---

This cheat sheet shows common ways to read and write Parquet files. The examples use these methods:

- [`readTable`](../data-import-export/Parquet/readTable.md)
- [`writeTable`](../data-import-export/Parquet/writeTable.md)
- [`writeKeyValuePartitionedTable`](../data-import-export/Parquet/writeKeyValuePartitionedTable.md)

The examples read and write files under `/data`, the data directory in Deephaven's Docker images. To map it to a directory on your computer, see [Docker data volumes](../../conceptual/docker-data-volumes.md). In other deployments, use any path the server can read and write.

## Create a table

The examples on this page write and read this sample table.

```groovy test-set=1 order=source
source = newTable(
    stringCol("X", "A", "B", "B", "C", "B", "A", "B", "B", "C"),
    intCol("Y", 2, 4, 2, 1, 2, 3, 4, 2, 3),
    intCol("Z", 55, 76, 20, 4, 230, 50, 73, 137, 214)
)
```

## Write Parquet files

This block writes `source` to a single file twice, once with the default codec and once with GZIP, and then writes it as a key-value partitioned directory.

```groovy test-set=1 order=null
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import static io.deephaven.parquet.table.ParquetTools.writeTable

// Write to a local file (SNAPPY compression by default)
writeTable(source, "/data/output.parquet")

// Write to a local file with GZIP compression
writeTable(source, "/data/output_GZIP.parquet", ParquetTools.GZIP)

// Write a partitioned table to a key-value partitioned directory (X=A/, X=B/, ...)
// The base name "data" names each file data.parquet, so a rerun overwrites the files instead of adding more
partitionedInstructions = ParquetInstructions.builder().setBaseNameForPartitionedParquetData("data").build()
ParquetTools.writeKeyValuePartitionedTable(
    source.partitionBy("X"), "/data/partitioned_output", partitionedInstructions)
```

### Compression codecs

To set the codec, pass a [`ParquetTools`](/core/javadoc/io/deephaven/parquet/table/ParquetTools.html) constant as the [`ParquetInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html) argument to [`writeTable`](../data-import-export/Parquet/writeTable.md). The example above passes [`ParquetTools.GZIP`](/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#GZIP). The constants that work in a default installation are listed below. `ParquetTools.BROTLI` also exists, but it needs a Brotli codec added to the server classpath. See [Parquet instructions](../../how-to-guides/data-import-export/parquet-instructions.md).

- `SNAPPY` (default): A codec based on Google's [Snappy compression format](https://github.com/google/snappy/blob/main/format_description.txt) that aims for high speed and reasonable compression.
- `UNCOMPRESSED`: No compression.
- `LZ4_RAW`: A codec based on the [LZ4 block format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md).
- `LZ4`: A deprecated codec. Use `LZ4_RAW` instead.
- `LZO`: A codec based on or interoperable with the [LZO compression library](https://www.oberhumer.com/opensource/lzo/).
- `GZIP`: A codec based on the GZIP format defined by [RFC 1952](https://tools.ietf.org/html/rfc1952). It differs from the related zlib and deflate formats.
- `ZSTD`: A codec based on the Zstandard format defined by [RFC 8478](https://tools.ietf.org/html/rfc8478) that aims for a high compression ratio.

## Read Parquet files

The `pems` example reads the PEMS traffic dataset at `/data/examples/Pems/parquet/pems` from [Deephaven's example data](https://github.com/deephaven/examples). To download it, see [Find example data](../../getting-started/docker-install.md#find-example-data).

```groovy test-set=1 order=sourceRead,sourceReadGzip,pems,partitioned
import static io.deephaven.parquet.table.ParquetTools.readTable

// Read from a local file
sourceRead = readTable("/data/output.parquet")

// Read a GZIP-compressed file
sourceReadGzip = readTable("/data/output_GZIP.parquet")

// Read a directory that has a _metadata file
pems = readTable("/data/examples/Pems/parquet/pems")

// Read a key-value partitioned directory
partitioned = readTable("/data/partitioned_output")
```

[`readTable`](../data-import-export/Parquet/readTable.md) detects the codec from the file, so reading `/data/output_GZIP.parquet` needs no codec setting.

### How `readTable` handles a directory

When you pass a directory to [`readTable`](../data-import-export/Parquet/readTable.md), it finds the data files, the schema, and the partitioning columns in one of two ways, depending on whether the directory has a [`_metadata` file](../../how-to-guides/data-import-export/parquet-formats.md#metadata-partitioned-directories).

With a `_metadata` file:

- [`readTable`](../data-import-export/Parquet/readTable.md) takes the data files and the schema from `_metadata`.
- The partitioning columns are the columns that the `_common_metadata` file in the same directory lists and `_metadata` does not. Without `_common_metadata`, the `key=value` directories do not become columns unless the [`ParquetInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html) set a table definition with [`setTableDefinition`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setTableDefinition(io.deephaven.engine.table.TableDefinition)).
- The PEMS directory has both files, but its `_common_metadata` lists no columns beyond those in `_metadata`, so its `station=...` directories do not become a column.

Without a `_metadata` file:

- [`readTable`](../data-import-export/Parquet/readTable.md) loads every `.parquet` file under the directory and skips names that start with `.`.
- It takes the schema from one of those files, so all the files should share a schema.
- Each `key=value` subdirectory becomes a partitioning column, and every subdirectory that holds Parquet files must use a `key=value` name. Reading `/data/partitioned_output` adds the partitioning column `X` from the `X=A/`, `X=B/`, and `X=C/` directories that [`writeKeyValuePartitionedTable`](../data-import-export/Parquet/writeKeyValuePartitionedTable.md) created.

### Read a directory that receives new files

Pass [`ParquetInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html) built with [`setIsRefreshing(true)`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setIsRefreshing(boolean)) to [`readTable`](../data-import-export/Parquet/readTable.md) to get a [refreshing table](../../conceptual/table-types.md) that picks up new Parquet files added to a directory and adds their rows. Add data as new files rather than changing files that have already been read. Refreshing reads don't work on a single file or with the metadata-file layout, which `readTable` uses when the directory has a `_metadata` file and no file layout is set. To read such a directory as a refreshing table, also call [`setFileLayout`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setFileLayout(io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout)) on the builder with [`ParquetInstructions.ParquetFileLayout.KV_PARTITIONED`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html) or `FLAT_PARTITIONED`. See [Parquet instructions](../../how-to-guides/data-import-export/parquet-instructions.md#reading-options) for the other reading options.

```groovy test-set=1 order=refreshing
import io.deephaven.parquet.table.ParquetInstructions
import static io.deephaven.parquet.table.ParquetTools.readTable

refreshingInstructions = ParquetInstructions.builder().setIsRefreshing(true).build()
refreshing = readTable("/data/partitioned_output", refreshingInstructions)
```

### Read legacy string columns

Some Parquet writers store string columns as byte-array columns (`BINARY` or `FIXED_LEN_BYTE_ARRAY`) with no logical type annotation. To read those columns as strings instead of byte arrays, pass [`ParquetTools.LEGACY`](/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#LEGACY) to [`readTable`](../data-import-export/Parquet/readTable.md), as in `readTable(path, ParquetTools.LEGACY)`.

## More options

The how-to guides cover more options: reading from [S3](../../how-to-guides/data-import-export/parquet-import.md#from-s3) and writing to [S3](../../how-to-guides/data-import-export/parquet-export.md#to-s3), setting the [file layout and table definition](../../how-to-guides/data-import-export/parquet-import.md#optional-arguments), and [generating metadata files](../../how-to-guides/data-import-export/parquet-instructions.md#writing-options).

## Related documentation

- [Docker data volumes](../../conceptual/docker-data-volumes.md)
- [Install and run with Docker](../../getting-started/docker-install.md)
- [Read Parquet files](../../how-to-guides/data-import-export/parquet-import.md)
- [Write Parquet files](../../how-to-guides/data-import-export/parquet-export.md)
- [Supported Parquet formats](../../how-to-guides/data-import-export/parquet-formats.md)
- [Parquet instructions](../../how-to-guides/data-import-export/parquet-instructions.md)
- [`readTable`](../data-import-export/Parquet/readTable.md)
- [`writeTable`](../data-import-export/Parquet/writeTable.md)
- [`writeKeyValuePartitionedTable`](../data-import-export/Parquet/writeKeyValuePartitionedTable.md)
- [`newTable`](../table-operations/create/newTable.md)
- [`partitionBy`](../table-operations/group-and-aggregate/partitionBy.md)
- [`ParquetTools` Javadoc](/core/javadoc/io/deephaven/parquet/table/ParquetTools.html)
- [`ParquetInstructions` Javadoc](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html)
