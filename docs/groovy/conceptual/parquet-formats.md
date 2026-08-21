---
title: Supported Parquet formats
---

[Apache Parquet](https://parquet.apache.org/) is an open-source, column-oriented data file format for efficient data storage and retrieval. Parquet is designed for complex, large-scale data, with several types of data compression formats available.

Parquet files are composed of row groups, a header, and a footer. Each row group contains data from every column, stored in a columnar format. This column-oriented structure optimizes performance and minimizes I/O. Parquet partitioning works by dividing the data into separate files based on specified criteria. For example, data may be partitioned by a date column, resulting in separate files for each day.

Parquet is meant to be a standard interchange format for batch and interactive workloads. Deephaven supports standard Parquet file formats out of the box.

## Single Parquet file

Deephaven supports single Parquet files. Using [a single large Parquet file](../how-to-guides/data-import-export/parquet-import.md#read-a-single-parquet-file) may be more storage efficient than many smaller files with accompanying metadata. It can be faster to read and process because there is less overhead in opening and closing files.

## Parquet file directories

Directories can contain multiple Parquet files. These can be [loaded as sections of a single table](../how-to-guides/data-import-export/parquet-import.md#partitioned-parquet-directories). They must be _flat_ Parquet files -- they have no partitioning columns.

A flat layout may be useful if:

- you want to control the size of your file.
- you want to write one piece at a time.
- you are writing files sequentially; e.g., writing once per hour.
- you have different systems publishing.
- you are using a third-party index.

## Nested Parquet files

These Parquet files are hierarchically [partitioned](../reference/data-import-export/Parquet/readTable.md#partitioned-datasets), nested in directories with names in the form of "key=value". All Parquet files are stored at the same nesting level. Choose this format if you have hierarchical partitioning and want Deephaven to determine the partitioning columns and value types automatically. Deephaven infers partitioning column names from the “keys” and parses the value type. This is useful if you do not have metadata files with information about expected types.

Note that if Deephaven does not know how to parse a value, it becomes a string. Support for unknown types can be accomplished with [`codecArgs`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/metadata/CodecInfo.html#codecArg()).

## Metadata files

Deephaven supports optional metadata files that let you specify the types of your partitioning columns, which may not be obvious otherwise. Top-level metadata files can supply the full table schema and information about partitioning columns, while leaf-level files provide additional information to the engine (such as grouping/indexing). Deephaven can discover your Parquet files without looking at the entire file system, and all metadata is loaded at once.

## Logical type support

Deephaven maps Parquet [logical types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md) to Deephaven column types on read. The following non-obvious mappings are supported:

- **`UINT_64`**: Read as `java.math.BigInteger`. Deephaven promotes the narrower unsigned integer types to a wider signed type that holds their full range — `UINT_8` and `UINT_16` become `char`, and `UINT_32` becomes `long` — but no Java primitive holds the full `UINT_64` range, so Deephaven reads these columns as `BigInteger` by default. Use `setUnsignedLongTarget` on a [`ParquetInstructions.Builder`](../how-to-guides/data-import-export/parquet-instructions.md) to choose a different type.

### Reading `UINT_64` as a long

`BigInteger` represents every `UINT_64` value exactly, at the cost of an object per value. Two alternatives read the column as a primitive `long` instead:

- `UnsignedLongTarget.LONG` reads values that fit in a `long` and raises an error on any value greater than 2<sup>63</sup> - 1. Use this option when the data is known to stay within the `long` range and an unnoticed overflow is unacceptable.
- `UnsignedLongTarget.SIGNED_LONG` reinterprets the bit pattern as signed, so values greater than 2<sup>63</sup> - 1 read as negative numbers. This option never fails, but it is imprecise in one further respect: 2<sup>63</sup> reads as `NULL_LONG`, which is indistinguishable from a null.

The following example reads a `UINT_64` column as a `long`, rejecting any value that does not fit:

```groovy skip-test
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions.UnsignedLongTarget

instructionsInstance = ParquetInstructions.builder()
    .setUnsignedLongTarget("UInt64Column", UnsignedLongTarget.LONG)
    .build()

result = ParquetTools.readTable("/data/unsigned.parquet", instructionsInstance)
```

Deephaven never writes `UINT_64`, so these options apply only to reads. Deephaven applies the same default and offers the same coercions when reading Arrow data; see the notes on integral coercion in the [Arrow Flight guide](../how-to-guides/data-import-export/arrow-flight.md).

## Related documentation

- [Import Parquet into Deephaven video](https://youtu.be/k4gI6hSZ2Jc)
- [Export Parquet files](../how-to-guides/data-import-export/parquet-export.md)
- [Import Parquet files](../how-to-guides/data-import-export/parquet-import.md)
- [Parquet instructions](../how-to-guides/data-import-export/parquet-instructions.md)
