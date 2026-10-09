---
title: Read Parquet files into Deephaven tables
---

Deephaven reads Parquet files directly into Deephaven tables with the [`ParquetTools`](/core/javadoc/io/deephaven/parquet/table/ParquetTools.html) class. This document covers reading data into tables from single Parquet files, key-value partitioned Parquet directories, and flat partitioned Parquet directories. This document also covers reading Parquet files from [S3](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) into Deephaven tables.

## Read a single Parquet file

Read a single Parquet file when one file holds all of the table's data.

### From local storage

Read single Parquet files into Deephaven tables with [`ParquetTools.readTable`](../../reference/data-import-export/Parquet/readTable.md). The method takes a single required argument, `source`, which gives the full file path of the Parquet file.

```groovy test-set=1
import io.deephaven.parquet.table.ParquetTools

// pass the path of the local Parquet file to `readTable`
grades = ParquetTools.readTable("/data/examples/ParquetExamples/grades/grades.parquet")
```

### From S3

The [`io.deephaven.extensions.s3`](/core/javadoc/io/deephaven/extensions/s3/package-summary.html) package supports reading from S3. This package contains the [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) class, which holds the settings Deephaven uses to connect to the S3 instance. Learn more about this class in the [Parquet instructions document](./parquet-instructions.md#s3instructions-methods).

Use [`ParquetTools.readTable`](../../reference/data-import-export/Parquet/readTable.md) to read a single Parquet file from S3, where the `source` argument is the S3 URI of the Parquet file (for example, `s3://bucket/key.parquet`). Pass connection details in the optional second argument, `readInstructions`, which takes a [`ParquetInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html) object. To build it, call [`ParquetInstructions.builder`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html#builder()), pass an [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) instance to the builder's [`setSpecialInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setSpecialInstructions(java.lang.Object)) method, and call [`build`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#build()). [Optional arguments](#optional-arguments) lists the other builder methods that apply when reading.

```groovy test-set=2 docker-config=rustfs
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.extensions.s3.S3Instructions
import io.deephaven.extensions.s3.Credentials

// This example uses basic credentials - other options are available
credentials = Credentials.basic("example_username", "example_password")

// Pass the S3 URI as well as instructions on how to talk to the S3 instance
grades = ParquetTools.readTable(
    "s3://example-bucket/grades/grades.parquet",
    ParquetInstructions.builder().setSpecialInstructions(
        S3Instructions.builder()
            .regionName("us-east-1")
            .endpointOverride("http://rustfs.example.com:9000")
            .credentials(credentials)
            .build()
    ).build()
)
```

> [!NOTE]
> When reading from S3, run the Deephaven instance in the same AWS region as the S3 bucket for the best performance. To improve performance further, store the data in a directory bucket in a single AWS Availability Zone, and run the Deephaven instance in that same Availability Zone. For more information, see the [AWS article on S3 Express One Zone directory buckets](https://community.aws/content/2ZDARM0xDoKSPDNbArrzdxbO3ZZ/s3-express-one-zone?lang=en).

## Read partitioned Parquet directories

A partitioned Parquet directory spreads the data for one table across many Parquet files. Deephaven reads two kinds of partitioned directory:

- A _key-value_ partitioned directory names each subdirectory after a partition column and its value, such as `Year=2024/`.
- A _flat_ partitioned directory keeps all of its Parquet files in a single directory, with no subdirectories and no partition columns.

Either kind can also contain the Parquet metadata files `_metadata` and `_common_metadata`, which describe the whole dataset. Reading through these files is faster than reading each Parquet file's own metadata. When Deephaven infers the layout of a directory that contains a `_metadata` file, it uses the metadata files automatically.

When Deephaven reads a partitioned Parquet directory, it returns a single table that contains the data from every file. For a key-value partitioned directory, each partition column appears as a column in the result. To work with each partition as a separate table, call [`partitionBy`](../../reference/table-operations/group-and-aggregate/partitionBy.md) on the result. See the [guide on partitioned tables](../../how-to-guides/partitioned-tables.md) for more information.

### Read a key-value partitioned Parquet directory

A key-value partitioned directory stores each partition in a `column=value` subdirectory, such as `Year=2024/`. Compared with a [flat partitioned directory](#read-a-flat-partitioned-parquet-directory), it lets filters on partition columns skip reading the files in non-matching subdirectories, at the cost of a deeper directory tree to maintain.

#### From local storage

Use [`ParquetTools.readTable`](../../reference/data-import-export/Parquet/readTable.md) to read a key-value partitioned Parquet directory into a Deephaven table. `ParquetTools.readTable` can infer the directory layout. Alternatively, pass [`ParquetFileLayout.KV_PARTITIONED`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#KV_PARTITIONED) to the [`setFileLayout`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setFileLayout(io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout)) builder method. Then pass the resulting [`ParquetInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html) as the `readInstructions` argument. Providing the layout skips only the check for a `_metadata` file. Deephaven still infers the partition columns and schema from the files unless you also pass a schema to [`setTableDefinition`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setTableDefinition(io.deephaven.engine.table.TableDefinition)).

```groovy test-set=3 order=gradesInferred,gradesProvided
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout

// directory layout may be inferred
gradesInferred = ParquetTools.readTable("/data/examples/ParquetExamples/grades_kv/")

// or provided by user, skipping the check for a _metadata file
gradesProvided = ParquetTools.readTable(
    "/data/examples/ParquetExamples/grades_kv/",
    ParquetInstructions.builder().setFileLayout(ParquetFileLayout.KV_PARTITIONED).build()
)
```

If the key-value partitioned Parquet directory contains `_common_metadata` and `_metadata` files, Deephaven uses them automatically when it infers the layout. To state the layout explicitly, pass [`ParquetFileLayout.METADATA_PARTITIONED`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#METADATA_PARTITIONED) to the [`setFileLayout`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setFileLayout(io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout)) method. Reading through the metadata files is the most performant option when they are available.

```groovy test-set=3
// read through the metadata files
gradesMetadata = ParquetTools.readTable(
    "/data/examples/ParquetExamples/grades_kv_meta/",
    ParquetInstructions.builder().setFileLayout(ParquetFileLayout.METADATA_PARTITIONED).build()
)
```

#### From S3

Use [`ParquetTools.readTable`](../../reference/data-import-export/Parquet/readTable.md) to read a key-value partitioned Parquet directory from S3. Pass an instance of the [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) class to the [`setSpecialInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setSpecialInstructions(java.lang.Object)) method. To skip the check for a `_metadata` file, pass [`ParquetFileLayout.KV_PARTITIONED`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#KV_PARTITIONED) to the [`setFileLayout`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setFileLayout(io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout)) method. Pass the built [`ParquetInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html) as the `readInstructions` argument. For performance, run Deephaven in the same AWS region as the bucket, as described in [Read a single Parquet file from S3](#from-s3).

```groovy test-set=4 order=gradesInferred,gradesProvided docker-config=rustfs
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout
import io.deephaven.extensions.s3.S3Instructions
import io.deephaven.extensions.s3.Credentials

credentials = Credentials.basic("example_username", "example_password")

// directory layout may be inferred
gradesInferred = ParquetTools.readTable(
    "s3://example-bucket/grades_kv/",
    ParquetInstructions.builder().setSpecialInstructions(
        S3Instructions.builder()
            .regionName("us-east-1")
            .endpointOverride("http://rustfs.example.com:9000")
            .credentials(credentials)
            .build()
    ).build()
)

// or provided
gradesProvided = ParquetTools.readTable(
    "s3://example-bucket/grades_kv/",
    ParquetInstructions.builder()
        .setFileLayout(ParquetFileLayout.KV_PARTITIONED)
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

S3-hosted key-value partitioned Parquet datasets may also have `_common_metadata` and `_metadata` files. Deephaven uses them automatically when it infers the layout. To state the layout explicitly, pass [`ParquetFileLayout.METADATA_PARTITIONED`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#METADATA_PARTITIONED) to the [`setFileLayout`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setFileLayout(io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout)) method.

```groovy test-set=4 docker-config=rustfs
// read through the metadata files
gradesMetadata = ParquetTools.readTable(
    "s3://example-bucket/grades_kv_meta/",
    ParquetInstructions.builder()
        .setFileLayout(ParquetFileLayout.METADATA_PARTITIONED)
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

### Read a flat partitioned Parquet directory

A flat partitioned Parquet directory is a single directory of Parquet files with no partition subdirectories. Deephaven reads the `.parquet` files in the directory into one table. For a local directory, it skips hidden files whose names start with `.`. A flat layout is simpler to manage than the nested subdirectories of a [key-value partitioned directory](#read-a-key-value-partitioned-parquet-directory). Because a flat directory has no partition columns, filters can't skip its files by partition value the way they can with a key-value partitioned directory.

#### From local storage

Read local flat partitioned Parquet directories into Deephaven tables with [`ParquetTools.readTable`](../../reference/data-import-export/Parquet/readTable.md). `ParquetTools.readTable` can infer the directory layout. To skip the check for a `_metadata` file, pass [`ParquetFileLayout.FLAT_PARTITIONED`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#FLAT_PARTITIONED) to the [`setFileLayout`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setFileLayout(io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout)) method and pass the resulting [`ParquetInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html) as the `readInstructions` argument.

```groovy test-set=5 order=gradesInferred,gradesProvided
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout

// directory layout may be inferred
gradesInferred = ParquetTools.readTable("/data/examples/ParquetExamples/grades_flat/")

// or provided by user, skipping the check for a _metadata file
gradesProvided = ParquetTools.readTable(
    "/data/examples/ParquetExamples/grades_flat/",
    ParquetInstructions.builder().setFileLayout(ParquetFileLayout.FLAT_PARTITIONED).build()
)
```

If the flat partitioned directory contains `_common_metadata` and `_metadata` files, Deephaven uses them automatically when it infers the layout. To state the layout explicitly, pass [`ParquetFileLayout.METADATA_PARTITIONED`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#METADATA_PARTITIONED) to the [`setFileLayout`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setFileLayout(io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout)) method.

```groovy test-set=5
// read through the metadata files
gradesMetadata = ParquetTools.readTable(
    "/data/examples/ParquetExamples/grades_flat_meta/",
    ParquetInstructions.builder().setFileLayout(ParquetFileLayout.METADATA_PARTITIONED).build()
)
```

#### From S3

Use [`ParquetTools.readTable`](../../reference/data-import-export/Parquet/readTable.md) to read a flat partitioned Parquet directory from S3. Pass an instance of the [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) class to the [`setSpecialInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setSpecialInstructions(java.lang.Object)) method. To skip the check for a `_metadata` file, pass [`ParquetFileLayout.FLAT_PARTITIONED`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#FLAT_PARTITIONED) to the [`setFileLayout`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setFileLayout(io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout)) method. Pass the built [`ParquetInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html) as the `readInstructions` argument. For performance, run Deephaven in the same AWS region as the bucket, as described in [Read a single Parquet file from S3](#from-s3).

```groovy test-set=6 order=gradesInferred,gradesProvided docker-config=rustfs
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout
import io.deephaven.extensions.s3.S3Instructions
import io.deephaven.extensions.s3.Credentials

credentials = Credentials.basic("example_username", "example_password")

// directory layout may be inferred
gradesInferred = ParquetTools.readTable(
    "s3://example-bucket/grades_flat/",
    ParquetInstructions.builder().setSpecialInstructions(
        S3Instructions.builder()
            .regionName("us-east-1")
            .endpointOverride("http://rustfs.example.com:9000")
            .credentials(credentials)
            .build()
    ).build()
)

// or provided
gradesProvided = ParquetTools.readTable(
    "s3://example-bucket/grades_flat/",
    ParquetInstructions.builder()
        .setFileLayout(ParquetFileLayout.FLAT_PARTITIONED)
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

If the S3-hosted flat partitioned Parquet dataset has `_common_metadata` and `_metadata` files, Deephaven uses them automatically when it infers the layout. To state the layout explicitly, pass [`ParquetFileLayout.METADATA_PARTITIONED`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#METADATA_PARTITIONED) to the [`setFileLayout`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setFileLayout(io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout)) method.

```groovy test-set=6 docker-config=rustfs
// read through the metadata files
gradesMetadata = ParquetTools.readTable(
    "s3://example-bucket/grades_flat_meta/",
    ParquetInstructions.builder()
        .setFileLayout(ParquetFileLayout.METADATA_PARTITIONED)
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

## Optional arguments

The required first argument to [`ParquetTools.readTable`](../../reference/data-import-export/Parquet/readTable.md), `source`, is the Parquet file, metadata file (`_metadata` or `_common_metadata`), or directory to read. This is typically a string containing a local path, an S3 URI, or a Google Cloud Storage (`gs://`) URI.

The method also takes an optional second argument, `readInstructions`, which is a [`ParquetInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html) object built with [`ParquetInstructions.builder`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.html#builder()). The [`ParquetInstructions.Builder`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html) methods that apply when reading include:

- [`setFileLayout`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setFileLayout(io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout)): The Parquet file or directory layout, provided as a [`ParquetFileLayout`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html). If you don't call this method, Deephaven infers the layout.
- [`setTableDefinition`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setTableDefinition(io.deephaven.engine.table.TableDefinition)): The table definition or schema. If you don't call this method, Deephaven infers the definition from the Parquet file(s).
- [`setSpecialInstructions`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setSpecialInstructions(java.lang.Object)): Special instructions for reading Parquet files from S3, an S3-compatible store, or Google Cloud Storage (`gs://` URIs), provided as an instance of [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html).
- [`setIsRefreshing`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setIsRefreshing(boolean)): Whether the Parquet data represents a refreshing source. When `true`, Deephaven checks a partitioned directory for new Parquet files and adds their rows to the table, so the result is a [refreshing table](../../conceptual/table-types.md). Refreshing reads aren't supported for a single Parquet file or for a directory read through its metadata files. The default is `false`.
- [`setIsLegacyParquet`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setIsLegacyParquet(boolean)): Whether the Parquet data is in legacy Parquet format. When `true`, Deephaven reads binary columns that have no logical type annotation and no recorded codec as strings rather than as byte arrays. Some older Parquet writers store strings this way. The default is `false`.
- [`addColumnNameMapping`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#addColumnNameMapping(java.lang.String,java.lang.String)): Maps a column name in the Parquet data to a column name in the Deephaven table.
- [`addColumnCodec`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#addColumnCodec(java.lang.String,java.lang.String)): Sets the [`ObjectCodec`](/core/javadoc/io/deephaven/util/codec/ObjectCodec.html) class that converts a column's values to and from bytes, for types with no language-agnostic Parquet representation. This is not a compression codec.
- [`setUnsignedLongTarget`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html#setUnsignedLongTarget(java.lang.String,io.deephaven.parquet.table.ParquetInstructions.UnsignedLongTarget)): Sets the Deephaven type to read an unsigned 64-bit integer (`UINT_64`) column as, provided as an [`UnsignedLongTarget`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.UnsignedLongTarget.html). If you don't call this method, Deephaven reads such columns as `BigInteger`.

See the [Parquet instructions guide](./parquet-instructions.md) for the full list of [`ParquetInstructions.Builder`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.Builder.html) and [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) methods.

## Related documentation

- [Parquet formats](./parquet-formats.md)
- [Parquet export](./parquet-export.md)
- [Parquet instructions](./parquet-instructions.md)
