---
title: Export Deephaven Tables to Parquet Files
sidebar_label: Export to Parquet
---

The [Deephaven Parquet module](/core/javadoc/io/deephaven/parquet/table/package-summary.html) provides tools to integrate Deephaven with the Parquet file format. This module makes it easy to write Deephaven tables to Parquet files and directories. This document covers writing Deephaven tables to single Parquet files, flat partitioned Parquet directories, and key-value partitioned Parquet directories.

By default, Deephaven tables are written to Parquet files using `SNAPPY` compression when writing the data. This default can be changed with the [`ParquetInstructions.Builder.setCompressionCodecName`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setCompressionCodecName(java.lang.String)) method in any of the writing functions discussed here.

First, create some tables that will be used for the examples in this guide.

```groovy test-set=1 order=grades,mathGrades,scienceGrades,historyGrades
mathGrades = newTable(
    stringCol("Name", "Ashley", "Jeff", "Rita", "Zach"),
    stringCol("Class", "Math", "Math", "Math", "Math"),
    intCol("Test1", 92, 78, 87, 74),
    intCol("Test2", 94, 88, 81, 70),
)

scienceGrades = newTable(
    stringCol("Name", "Ashley", "Jeff", "Rita", "Zach"),
    stringCol("Class", "Science", "Science", "Science", "Science"),
    intCol("Test1", 87, 90, 99, 80),
    intCol("Test2", 91, 83, 95, 78),
)

historyGrades = newTable(
    stringCol("Name", "Ashley", "Jeff", "Rita", "Zach"),
    stringCol("Class", "History", "History", "History", "History"),
    intCol("Test1", 82, 87, 84, 76),
    intCol("Test2", 88, 92, 85, 78),
)

grades = merge(mathGrades, scienceGrades, historyGrades)

gradesPartitioned = grades.partitionBy("Class")
```

## Write to a single Parquet file

Write a Deephaven table to a single Parquet file with [`ParquetTools.writeTable`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeTable(io.deephaven.engine.table.Table,java.lang.String,io.deephaven.parquet.table.ParquetInstructions)). Supply the `sourceTable` argument with the Deephaven table to be written, and the `destination` argument with the file path for the resulting Parquet file. This file path should end with the `.parquet` file extension. A compression codec enum can be specified to compress the output file.

```groovy test-set=1
import io.deephaven.parquet.table.ParquetTools

// write to a standard, uncompressed Parquet file
ParquetTools.writeTable(grades, "/data/grades/grades.parquet")

// write to a GZIP-compressed Parquet file
ParquetTools.writeTable(grades, "/data/grades/grades_gzip.parquet", ParquetTools.GZIP)
```

Write `_metadata` and `_common_metadata` files by calling [`Builder.setGenerateMetadataFiles(true)`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setGenerateMetadataFiles(boolean)). Parquet metadata files are useful for reading very large datasets, as they enhance the performance of the read operation significantly. If the data might be read in the future, consider writing metadata files.

```groovy test-set=1
import io.deephaven.parquet.table.ParquetInstructions

ParquetTools.writeTable(
    grades,
    "/data/grades_meta/grades.parquet",
    ParquetInstructions.builder().setGenerateMetadataFiles(true).build()
)
```

<!--- TODO: Add S3 when supported-->

## Partitioned Parquet directories

Deephaven supports writing tables to partitioned Parquet directories. A partitioned Parquet directory organizes data into subdirectories based on one or more partitioning columns. This structure allows for more efficient data querying by pruning irrelevant partitions, leading to faster read times than a single Parquet file. Deephaven tables can be written to _flat_ partitioned directories or _key-value_ partitioned directories.

Data can be written to partitioned directories from Deephaven tables or from Deephaven's [partitioned tables](../../how-to-guides/partitioned-tables.md). Partitioned tables have partitioning columns built into the API, so Deephaven can use those partitioning columns to create partitioned directories. Regular Deephaven tables do not have partitioning columns, so the user must provide that information using the `table_definition` argument to any of the writing functions.

Table definitions represent a table's schema. They are constructed from lists of Deephaven [`ColumnDefinition`](/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html) objects that specify a column's name and type. Additionally, [`ColumnDefinition`](/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html) objects are used to specify whether a particular column is a partitioning column by calling the `withPartitioning()` method.

Create a table definition for the `grades` table defined above.

```groovy test-set=1
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.ColumnDefinition

gradesDef = TableDefinition.of(ColumnDefinition.ofString("Name"),
    // Class is declared to be a partitioning column
    ColumnDefinition.ofString("Class").withPartitioning(),
    ColumnDefinition.ofInt("Test1"),
    ColumnDefinition.ofInt("Test2")
)
```

## Write to a key-value partitioned Parquet directory

Key-value partitioned Parquet directories extend partitioning by organizing data based on key-value pairs in the directory structure. This allows for highly granular and flexible data access patterns, providing efficient querying for complex datasets. The downside is the added complexity in managing and maintaining the key-value pairs, which can be more intricate than other partitioning methods.

Use [`ParquetTools.writeKeyValuePartitionedTable`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeKeyValuePartitionedTable(io.deephaven.engine.table.PartitionedTable,java.lang.String,io.deephaven.parquet.table.ParquetInstructions)) to write Deephaven tables to key-value partitioned Parquet directories. Supply a Deephaven table or a [partitioned table](../../how-to-guides/partitioned-tables.md) to the `partitionedTable` argument, and set the `destinationDir` argument to the destination root directory where the partitioned Parquet data will be stored. Non-existing directories in the provided path will be created.

```groovy test-set=1
// write a standard Deephaven table, must specify table_definition
ParquetTools.writeTable(
    grades, "/data/grades_kv_1.parquet", ParquetInstructions.builder().setTableDefinition(gradesDef).build()
)

// or write a partitioned table
ParquetTools.writeKeyValuePartitionedTable(gradesPartitioned, "/data/grades_kv_2.parquet", ParquetInstructions.builder().setTableDefinition(gradesDef).build())
```

Call the `setGenerateMetadataFiles` method to write metadata files.

```groovy test-set=1
ParquetTools.writeKeyValuePartitionedTable(
    gradesPartitioned,
    "/data/grades_kv_2_md.parquet",
    ParquetInstructions.builder().setGenerateMetadataFiles(true).build()
)
```

<!--- TODO: Add S3 when supported -->

## Write to a flat partitioned Parquet directory

A flat partitioned Parquet directory stores data without nested subdirectories. Each file contains partition information within its filename or as metadata. This approach simplifies directory management compared to hierarchical partitioning but can lead to larger directory listings, which might affect performance with many partitions.

Use [`ParquetTools.writeTable`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeTable(io.deephaven.engine.table.Table,java.lang.String,io.deephaven.parquet.table.ParquetInstructions)) or [`ParquetTools.writeTables`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeTables(io.deephaven.engine.table.Table%5B%5D,java.lang.String%5B%5D,io.deephaven.parquet.table.ParquetInstructions)) to write Deephaven tables to Parquet files in flat partitioned directories. [`ParquetTools.writeTable`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeTable(io.deephaven.engine.table.Table,java.lang.String,io.deephaven.parquet.table.ParquetInstructions)) requires multiple calls to write multiple tables to the destination, while [`ParquetTools.writeTables`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeTables(io.deephaven.engine.table.Table%5B%5D,java.lang.String%5B%5D,io.deephaven.parquet.table.ParquetInstructions)) can write multiple tables to multiple paths in a single call.

Supply [`ParquetTools.writeTable`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeTable(io.deephaven.engine.table.Table,java.lang.String,io.deephaven.parquet.table.ParquetInstructions)) with the Deephaven table to be written and the destination file path with the `table` and `path` arguments. The `path` must end with the `.parquet` file extension.

```groovy test-set=1
ParquetTools.writeTable(grades, "/data/grades_flat_1/math.parquet")
ParquetTools.writeTable(grades, "/data/grades_flat_1/science.parquet")
ParquetTools.writeTable(grades, "/data/grades_flat_1/history.parquet")
```

Use [`ParquetTools.writeTables`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeTables(io.deephaven.engine.table.Table%5B%5D,java.lang.String%5B%5D,io.deephaven.parquet.table.ParquetInstructions)) to accomplish the same thing by passing multiple tables to the `tables` argument and multiple destination paths to the `paths` argument. This requires the `table_definition` argument to be specified.

```groovy test-set=1
ParquetTools.writeTables(
    new Table[] {mathGrades, scienceGrades, historyGrades},
    new String[] {
        "/data/grades_flat_2/math.parquet",
        "/data/grades_flat_2/science.parquet",
        "/data/grades_flat_2/history.parquet",
    },
    ParquetInstructions.builder().setTableDefinition(gradesDef).build(),
)
```

To write a [Deephaven partitioned table](../../how-to-guides/partitioned-tables.md) to a flat partitioned Parquet directory, the table must first be broken into a list of constituent tables, such as by calling `PartitionedTable.constituents()`. Then [`ParquetTools.writeTables`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeTables(io.deephaven.engine.table.Table%5B%5D,java.lang.String%5B%5D,io.deephaven.parquet.table.ParquetInstructions)) can be used to write all of the resulting constituent tables to Parquet. Again, the `table_definition` argument must be specified.

```groovy test-set=1
ParquetTools.writeTables(
    gradesPartitioned.constituents(),
    new String[] {
        "/data/grades_flat_2/math.parquet",
        "/data/grades_flat_2/science.parquet",
        "/data/grades_flat_2/history.parquet",
    },
    ParquetInstructions.builder().setTableDefinition(gradesDef).build(),
)
```

<!--- TODO: Add S3 when supported -->

## Related documentation

- [Import Parquet files](./parquet-import.md)
- [Parquet file formats](./parquet-formats.md)
- [Parquet instructions](./parquet-instructions.md)
