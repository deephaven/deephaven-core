---
title: Export Deephaven tables to Parquet files
---

The [Deephaven Parquet module](/core/javadoc/io/deephaven/parquet/table/package-summary.html) provides tools to integrate Deephaven with the Parquet file format. This document covers writing Deephaven tables to single Parquet files, key-value partitioned Parquet directories, and flat partitioned Parquet directories. You can write each layout to local storage or to S3.

By default, Deephaven writes Parquet files with `SNAPPY` compression, and the codec applies to the whole file. [Write to a single Parquet file](#write-to-a-single-parquet-file) shows how to choose a different codec, and the [Parquet instructions](./parquet-instructions.md) document lists the available options.

> [!NOTE]
>
> When writing to S3, run Deephaven in the same AWS region as the S3 bucket for the best performance. To improve performance further, store the data in a directory bucket in a single AWS Availability Zone, and run Deephaven in that same Availability Zone. See the [AWS article on S3 Express One Zone directory buckets](https://community.aws/content/2ZDARM0xDoKSPDNbArrzdxbO3ZZ/s3-express-one-zone?lang=en) for more information.
>
> The S3 examples use placeholder credentials and endpoints. Replace them with the values for your S3 instance.

First, create some tables to use in the examples in this guide.

```groovy test-set=1 order=grades,mathGrades,scienceGrades,historyGrades docker-config=rustfs
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

### To local storage

Write a Deephaven table to a single Parquet file with [`ParquetTools.writeTable`](../../reference/data-import-export/Parquet/writeTable.md). Pass the table as the `sourceTable` argument and the destination file path as the `destination` argument. The `destination` must end with the `.parquet` file extension.

To set compression and other options, pass a [`ParquetInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html) object as the optional `writeInstructions` argument. [`ParquetTools`](/core/javadoc/io/deephaven/parquet/table/ParquetTools.html) provides ready-made instructions for common codecs. For example, [`ParquetTools.GZIP`](/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#GZIP) is equivalent to `ParquetInstructions.builder().setCompressionCodecName("GZIP").build()`.

```groovy test-set=1
import io.deephaven.parquet.table.ParquetTools

// write to a Parquet file with the default SNAPPY compression
ParquetTools.writeTable(grades, "/data/grades/grades.parquet")

// write to a GZIP-compressed Parquet file
ParquetTools.writeTable(grades, "/data/grades/gradesGzip.parquet", ParquetTools.GZIP)
```

Write `_metadata` and `_common_metadata` files by calling [`ParquetInstructions.Builder.setGenerateMetadataFiles(true)`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setGenerateMetadataFiles(boolean)). These files hold the schema and other metadata for the Parquet files that the write produces, and Deephaven places them in the destination directory. Readers use them to find the data files and the full schema without listing the directory tree, so they matter most for the [partitioned Parquet directories](#partitioned-parquet-directories) described later in this guide. See [Metadata-partitioned directories](./parquet-formats.md#metadata-partitioned-directories) for what each file contains.

```groovy test-set=1
import io.deephaven.parquet.table.ParquetInstructions

ParquetTools.writeTable(
    grades,
    "/data/gradesMeta/grades.parquet",
    ParquetInstructions.builder().setGenerateMetadataFiles(true).build()
)
```

### To S3

Use [`ParquetTools.writeTable`](../../reference/data-import-export/Parquet/writeTable.md) to write Deephaven tables to Parquet files on S3. The `destination` should be the URI of the destination file in S3. Supply an instance of the [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) class to the [`setSpecialInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setSpecialInstructions(java.lang.Object)) method of [`ParquetInstructions.Builder`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html) to specify the details of the connection to the S3 instance.

```groovy test-set=1
import io.deephaven.extensions.s3.S3Instructions
import io.deephaven.extensions.s3.Credentials

credentials = Credentials.basic("example_username", "example_password")

ParquetTools.writeTable(
    grades,
    "s3://example-bucket/grades.parquet",
    ParquetInstructions.builder()
        .setSpecialInstructions(
            S3Instructions.builder()
                .regionName("us-east-1")
                .endpointOverride("http://rustfs.example.com:9000")
                .credentials(credentials)
                .build()
        )
        .build()
)
```

## Partitioned Parquet directories

Deephaven can also write tables to a directory of Parquet files instead of a single file. It supports two directory layouts:

- A _key-value_ partitioned directory nests its Parquet files in subdirectories named `key=value`, one level per _partitioning column_. A partitioning column's values name the subdirectories, such as `Class=Math`, instead of being stored inside each file.
- A _flat_ partitioned directory holds its Parquet files side by side in a single directory. It has no partitioning columns.

When a query filters on a partitioning column, Deephaven can skip the subdirectories the filter excludes. A flat directory has no nested subdirectories, so it is simpler to manage, but with many files the single directory listing grows large, which can slow reads.

## Write to a key-value partitioned Parquet directory

A key-value partitioned directory stores each partition in a `key=value` subdirectory, as described in [Partitioned Parquet directories](#partitioned-parquet-directories). Writing the `grades` table with `Class` as the partitioning column produces the subdirectories `Class=Math`, `Class=Science`, and `Class=History`.

You can write a key-value partitioned directory from a regular Deephaven table or from a [partitioned table](../../how-to-guides/partitioned-tables.md). When you write a partitioned table, its key columns become the partitioning columns.

A regular table needs a table definition, which is the table's schema with the partitioning columns marked. A regular table's own definition usually has no partitioning columns, so the write fails unless you pass a definition that has them. Pass it to [`ParquetInstructions.Builder.setTableDefinition`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setTableDefinition(io.deephaven.engine.table.TableDefinition)).

Build a table definition with [`TableDefinition.of`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html#of(io.deephaven.engine.table.ColumnDefinition...)), passing one [`ColumnDefinition`](/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html) per column. Factory methods such as [`ColumnDefinition.ofString`](/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html#ofString(java.lang.String)) take the column name. To mark a column as a partitioning column, call its [`withPartitioning`](/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html#withPartitioning()) method.

Create a table definition for the `grades` table defined above.

```groovy test-set=1
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.ColumnDefinition

gradesDef = TableDefinition.of(
    ColumnDefinition.ofString("Name"),
    // Class is declared to be a partitioning column
    ColumnDefinition.ofString("Class").withPartitioning(),
    ColumnDefinition.ofInt("Test1"),
    ColumnDefinition.ofInt("Test2")
)
```

### To local storage

[`ParquetTools.writeKeyValuePartitionedTable`](../../reference/data-import-export/Parquet/writeKeyValuePartitionedTable.md) takes three arguments:

- `sourceTable` or `partitionedTable`: The table to write, either a regular table or a partitioned table.
- `destinationDir`: The root directory for the partitioned Parquet data. Deephaven creates any missing directories in the path.
- `writeInstructions`: A [`ParquetInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html) object.

```groovy test-set=1
// write a regular Deephaven table; setTableDefinition is required
ParquetTools.writeKeyValuePartitionedTable(
    grades,
    "/data/gradesKv/",
    ParquetInstructions.builder().setTableDefinition(gradesDef).build()
)

// or write a partitioned table
ParquetTools.writeKeyValuePartitionedTable(
    gradesPartitioned,
    "/data/gradesKvPartitioned/",
    ParquetInstructions.builder().build()
)
```

Call [`setGenerateMetadataFiles(true)`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setGenerateMetadataFiles(boolean)) on the builder to write `_metadata` and `_common_metadata` files at the root of the directory, as described in [Write to a single Parquet file](#write-to-a-single-parquet-file).

```groovy test-set=1
ParquetTools.writeKeyValuePartitionedTable(
    gradesPartitioned,
    "/data/gradesKvPartitionedMeta/",
    ParquetInstructions.builder().setGenerateMetadataFiles(true).build()
)
```

### To S3

Use [`ParquetTools.writeKeyValuePartitionedTable`](../../reference/data-import-export/Parquet/writeKeyValuePartitionedTable.md) to write key-value partitioned Parquet directories to S3. The `destinationDir` should be the URI of the destination directory in S3. Supply an instance of the [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) class to the [`setSpecialInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setSpecialInstructions(java.lang.Object)) method of [`ParquetInstructions.Builder`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html) to specify the details of the connection to the S3 instance.

```groovy test-set=1
import io.deephaven.extensions.s3.S3Instructions
import io.deephaven.extensions.s3.Credentials

credentials = Credentials.basic("example_username", "example_password")

ParquetTools.writeKeyValuePartitionedTable(
    gradesPartitioned,
    "s3://example-bucket/partitioned-directory/",
    ParquetInstructions.builder()
        .setSpecialInstructions(
            S3Instructions.builder()
                .regionName("us-east-1")
                .endpointOverride("http://rustfs.example.com:9000")
                .credentials(credentials)
                .build()
        )
        .build()
)
```

## Write to a flat partitioned Parquet directory

A flat partitioned directory holds one Parquet file per table in a single directory.

### To local storage

Use [`ParquetTools.writeTable`](../../reference/data-import-export/Parquet/writeTable.md) or [`ParquetTools.writeTables`](/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeTables(io.deephaven.engine.table.Table%5B%5D,java.lang.String%5B%5D,io.deephaven.parquet.table.ParquetInstructions)) to write Deephaven tables to Parquet files in flat partitioned directories.

Call [`ParquetTools.writeTable`](../../reference/data-import-export/Parquet/writeTable.md) once per table, giving each file its own path in the same directory, as in [Write to a single Parquet file](#write-to-a-single-parquet-file).

```groovy test-set=1
ParquetTools.writeTable(mathGrades, "/data/gradesFlat1/math.parquet")
ParquetTools.writeTable(scienceGrades, "/data/gradesFlat1/science.parquet")
ParquetTools.writeTable(historyGrades, "/data/gradesFlat1/history.parquet")
```

Use [`ParquetTools.writeTables`](/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeTables(io.deephaven.engine.table.Table%5B%5D,java.lang.String%5B%5D,io.deephaven.parquet.table.ParquetInstructions)) to accomplish the same thing by passing multiple tables to the `sources` argument and multiple destination paths to the `destinations` argument. If the tables have different definitions, also call [`setTableDefinition`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setTableDefinition(io.deephaven.engine.table.TableDefinition)) on the [`ParquetInstructions.Builder`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html).

```groovy test-set=1
ParquetTools.writeTables(
    new Table[] {mathGrades, scienceGrades, historyGrades},
    new String[] {
        "/data/gradesFlat2/math.parquet",
        "/data/gradesFlat2/science.parquet",
        "/data/gradesFlat2/history.parquet",
    },
    ParquetInstructions.builder().build(),
)
```

To write a [Deephaven partitioned table](../../how-to-guides/partitioned-tables.md) to a flat partitioned Parquet directory, get its constituent tables with [`constituents`](../../reference/table-operations/partitioned-tables/constituents.md) and pass them to [`ParquetTools.writeTables`](/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeTables(io.deephaven.engine.table.Table%5B%5D,java.lang.String%5B%5D,io.deephaven.parquet.table.ParquetInstructions)).

```groovy test-set=1
ParquetTools.writeTables(
    gradesPartitioned.constituents(),
    new String[] {
        "/data/gradesFlat3/math.parquet",
        "/data/gradesFlat3/science.parquet",
        "/data/gradesFlat3/history.parquet",
    },
    ParquetInstructions.builder().build(),
)
```

### To S3

Use [`ParquetTools.writeTables`](/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeTables(io.deephaven.engine.table.Table%5B%5D,java.lang.String%5B%5D,io.deephaven.parquet.table.ParquetInstructions)) to write an array of Deephaven tables to a flat partitioned Parquet directory in S3. The `destinations` should be the URIs of the destination files in S3. Supply an instance of the [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) class to the [`setSpecialInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setSpecialInstructions(java.lang.Object)) method of [`ParquetInstructions.Builder`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html) to specify the details of the connection to the S3 instance.

```groovy test-set=1
import io.deephaven.extensions.s3.S3Instructions
import io.deephaven.extensions.s3.Credentials

credentials = Credentials.basic("example_username", "example_password")

ParquetTools.writeTables(
    new Table[] {mathGrades, scienceGrades, historyGrades},
    new String[] {
        "s3://example-bucket/grades-flat/math.parquet",
        "s3://example-bucket/grades-flat/science.parquet",
        "s3://example-bucket/grades-flat/history.parquet",
    },
    ParquetInstructions.builder()
        .setSpecialInstructions(
            S3Instructions.builder()
                .regionName("us-east-1")
                .endpointOverride("http://rustfs.example.com:9000")
                .credentials(credentials)
                .build()
        )
        .build()
)
```

## Related documentation

- [Import Parquet files](./parquet-import.md)
- [Parquet formats](./parquet-formats.md)
- [Parquet instructions](./parquet-instructions.md)
