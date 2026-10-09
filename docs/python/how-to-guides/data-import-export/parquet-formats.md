---
title: Supported Parquet formats
---

[Apache Parquet](https://parquet.apache.org/) is an open-source, column-oriented data file format for efficient data storage and retrieval. Parquet is designed for complex, large-scale data and supports several compression codecs.

Parquet files are composed of row groups, a header, and a footer. Each row group contains data from every column, stored in a columnar format. This column-oriented structure optimizes performance and minimizes I/O.

Parquet is a standard interchange format for batch and interactive workloads. Deephaven reads most standard Parquet files without extra plugins or configuration. This page describes the file layouts Deephaven can read, the Parquet logical types whose Deephaven column types may be unexpected, and the column types Deephaven can't read.

## File layouts

Deephaven reads four Parquet file layouts, one for each value of [`ParquetFileLayout`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout). When you call [`read`](../../reference/data-import-export/Parquet/readTable.md) without a `file_layout` argument, Deephaven infers the layout from the path:

- A path ending in `.parquet` is read as a single file.
- A path ending in `_metadata` or `_common_metadata` is read as metadata-partitioned.
- Any other path is treated as a directory. Deephaven reads it as metadata-partitioned if it contains a `_metadata` file, and as key-value partitioned otherwise. Deephaven also reads flat directories this way. To read a single Parquet file whose name doesn't end in `.parquet`, set the [single-file layout](#single-parquet-file) explicitly.

### Single Parquet file

Deephaven supports single Parquet files. Using [a single large Parquet file](./parquet-import.md#read-a-single-parquet-file) may be more storage efficient than many smaller files with accompanying [metadata files](#metadata-partitioned-directories). It can be faster to read and process because there is less overhead in opening and closing files.

To read this format explicitly, pass `file_layout=ParquetFileLayout.SINGLE_FILE` to [`read`](../../reference/data-import-export/Parquet/readTable.md).

### Flat partitioned directories

A directory can contain multiple Parquet files, and Deephaven can [load them as sections of a single table](./parquet-import.md#read-a-flat-partitioned-parquet-directory). In a _flat_ partitioned directory, the Parquet files sit directly in one directory. Unlike a [key-value partitioned directory](#key-value-partitioned-directories), it has no `key=value` subdirectories. Partitioning columns take their values from `key=value` directory names, so a table read from a flat directory has no partitioning columns.

When you write data, a flat layout may be useful if:

- You want to control the size of each file
- You write one file at a time, for example, once per hour
- Several systems write files to the same directory

To read this format explicitly, pass `file_layout=ParquetFileLayout.FLAT_PARTITIONED` to [`read`](../../reference/data-import-export/Parquet/readTable.md).

### Key-value partitioned directories

In a key-value partitioned directory, Parquet files are hierarchically [partitioned](./parquet-import.md#read-partitioned-parquet-directories) into nested directories named `key=value`. All of the Parquet files sit at the same nesting level.

Deephaven takes each partitioning column's name from the directory keys and infers its type from the directory values. Write data in this layout when it is partitioned hierarchically and you don't write [metadata files](#metadata-partitioned-directories) that record the partitioning column types.

If Deephaven cannot infer a more specific type for a partition value, the column becomes a `String` column. To choose the type yourself, pass a table definition with the `table_definition` argument to [`read`](../../reference/data-import-export/Parquet/readTable.md).

To read this format explicitly, pass `file_layout=ParquetFileLayout.KV_PARTITIONED` to [`read`](../../reference/data-import-export/Parquet/readTable.md). See [Read a key-value partitioned Parquet directory](./parquet-import.md#read-a-key-value-partitioned-parquet-directory) for examples.

### Metadata-partitioned directories

A metadata-partitioned directory is a Parquet dataset with a `_metadata` file, and optionally a `_common_metadata` file, at its root:

- `_metadata` describes every row group in every Parquet file in the dataset. Deephaven reads it to find all of the data files at once, without listing the directory tree.
- `_common_metadata` supplies the full schema, including the partitioning columns, which `_metadata` does not describe. If a dataset has `_metadata` but no `_common_metadata`, Deephaven reads it without partitioning columns unless you pass a table definition with the `table_definition` argument to [`read`](../../reference/data-import-export/Parquet/readTable.md).

Write these files by passing `generate_metadata_files=True` to [`write`](../../reference/data-import-export/Parquet/writeTable.md) or [`write_partitioned`](../../reference/data-import-export/Parquet/writePartitioned.md), as shown in [Export Parquet files](./parquet-export.md).

This layout doesn't support refreshing reads, so reading such a directory with `is_refreshing=True` fails.

To read this format explicitly, pass `file_layout=ParquetFileLayout.METADATA_PARTITIONED` to [`read`](../../reference/data-import-export/Parquet/readTable.md).

## Unexpected logical type mappings

In addition to the file layout, each Parquet column carries a type. Deephaven maps Parquet [logical types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md) to Deephaven column types on read. Some of these mappings are ones you might not expect:

- **`ENUM`**: Read as `String`. Some Parquet writers use `ENUM` to mark columns that hold a finite set of string values. Physically, an `ENUM` column is stored the same way as a `STRING` column, as UTF-8 bytes in a `BINARY` column.
- **`UINT_8` and `UINT_16`**: Read as `char`, Java's unsigned 16-bit type.
- **`UINT_32`**: Read as `long`.
- **`UINT_64`**: Read as `java.math.BigInteger` by default, because no Java primitive holds the full `UINT_64` range. To read it as a `long` instead, see [Reading `UINT_64` as a `long`](#reading-uint_64-as-a-long).

### Reading `UINT_64` as a `long`

`BigInteger` represents every `UINT_64` value exactly but allocates an object per value. To read the column as a primitive `long` instead, pass [`read`](../../reference/data-import-export/Parquet/readTable.md) a [`ColumnInstruction`](../../reference/data-import-export/Parquet/ColumnInstruction.md) that sets `parquet_column_name`, `column_name`, and `unsigned_long_target`. `read` requires `parquet_column_name` on every `ColumnInstruction`. If you don't set `column_name`, Deephaven ignores `unsigned_long_target`. Set `unsigned_long_target` to one of two [`UnsignedLongTarget`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.UnsignedLongTarget) options:

- `UnsignedLongTarget.LONG` reads values that fit in a `long` and raises an error when Deephaven reads a page that contains a value greater than 2<sup>63</sup> - 1. The call to `read` itself succeeds; the error comes up when that data is accessed. Use this option when you know the data stays within the `long` range and an unnoticed overflow is unacceptable.
- `UnsignedLongTarget.SIGNED_LONG` reinterprets the bit pattern as signed, so values greater than 2<sup>63</sup> - 1 read as negative numbers. This option never fails. The value 2<sup>63</sup> reads as [`NULL_LONG`](../../reference/query-language/types/nulls.md). Deephaven reserves that `long` value for null, so a stored 2<sup>63</sup> is indistinguishable from a null.

The following example reads a `UINT_64` column as a `long`, rejecting any value that does not fit:

```python skip-test
from deephaven.parquet import ColumnInstruction, UnsignedLongTarget, read

result = read(
    "/data/unsigned.parquet",
    col_instructions=[
        ColumnInstruction(
            column_name="UInt64Column",
            parquet_column_name="UInt64Column",
            unsigned_long_target=UnsignedLongTarget.LONG,
        )
    ],
)
```

Deephaven never writes `UINT_64`, so these options apply only to reads. For how Arrow Flight handles unsigned 64-bit integers, see the [Arrow type support matrix](./arrow-flight.md#arrow-type-support-matrix).

## Unsupported column types

Deephaven can't infer a column type for the following Parquet columns:

- `JSON`, `BSON`, `UUID`, `INTERVAL`, `FLOAT16`, `GEOMETRY`, `GEOGRAPHY`, and `MAP` columns
- Group columns, also called structs, that have more than one field
- Nested repeated columns, such as lists of lists

Reading a file that contains such a column fails with an error unless you pass [`read`](../../reference/data-import-export/Parquet/readTable.md) a `table_definition` that leaves that column out.

## Related documentation

- [Import Parquet into Deephaven video](https://youtu.be/k4gI6hSZ2Jc)
- [Import Parquet files](./parquet-import.md)
- [Export Parquet files](./parquet-export.md)
- [`ColumnInstruction`](../../reference/data-import-export/Parquet/ColumnInstruction.md)
