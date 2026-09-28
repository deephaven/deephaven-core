---
title: Read Parquet files into Deephaven tables
sidebar_label: Read Parquet files
---

Deephaven integrates seamlessly with Parquet via the [Parquet Groovy module](/core/javadoc/io/deephaven/parquet/table/ParquetTools.html), making it easy to read Parquet files directly into Deephaven tables. This document covers reading data into tables from single Parquet files, flat Parquet directories, and partitioned key-value Parquet directories. This document also covers reading Parquet files from [S3](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) into Deephaven tables, a common use case.

> [!NOTE]
> Much of this document covers reading Parquet files from S3. For the best performance, the Deephaven instance should be running in the same AWS region as the S3 bucket. Additional performance improvements can be made by using directory buckets to localize all data to a single AWS sub-region, and running the Deephaven instance in that same sub-region. See [this article](https://community.aws/content/2ZDARM0xDoKSPDNbArrzdxbO3ZZ/s3-express-one-zone?lang=en) for more information on S3 directory buckets.

## Read a single Parquet file

Reading a single Parquet file involves loading data from one specific file into a table. This is straightforward and efficient when dealing with a relatively small dataset or when the data is consolidated into one file.

### From local storage

Read single Parquet files into Deephaven tables with [`ParquetTools.readTable`](../../reference/data-import-export/Parquet/readTable.md). The function takes a single required argument `source`, which gives the full file path of the Parquet file.

```groovy test-set=1
import io.deephaven.parquet.table.ParquetTools

// pass the path of the local parquet file to `readTable`
grades = ParquetTools.readTable("/data/examples/ParquetExamples/grades/grades.parquet")
```

### From S3

Deephaven provides some tooling around reading from S3 with the [`io.deephaven.extensions.s3`](/core/javadoc/io/deephaven/extensions/s3/package-summary.html) Groovy module. This module contains the [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) class, which is used to establish communication with the S3 instance. Learn more about this class in the [Parquet instructions document](./parquet-instructions.md#s3instructions-methods).

Use [`ParquetTools.readTable`](../../reference/data-import-export/Parquet/readTable.md) to read a single Parquet file from S3, where the `source` argument is provided as the endpoint to the Parquet file on the S3 instance. Supply an instance of the [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) class to the `ParquetInstructions.Builder` to specify the details of the connection to the S3 instance. Learn more
about this class in the [Parquet instructions document](./parquet-instructions.md#s3instructions-methods).

```groovy test-set=2 docker-config=minio
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.extensions.s3.S3Instructions
import io.deephaven.extensions.s3.Credentials

// This example uses basic credentials - other options are available
credentials = Credentials.basic("example_username", "example_password")

// Pass the S3 URL as well as instructions on how to talk to the S3 instance
grades = ParquetTools.readTable(
    "s3://example-bucket/grades/grades.parquet",
    ParquetInstructions.builder().setSpecialInstructions(
        S3Instructions.builder()
            .regionName("us-east-1")
            .endpointOverride("http://minio.example.com:9000")
            .credentials(credentials)
            .build()
    ).build()
)
```

## Partitioned Parquet directories

Deephaven supports reading partitioned Parquet directories. A partitioned Parquet directory organizes data into subdirectories based on one or more partition columns. This structure allows for more efficient data querying by pruning irrelevant partitions, leading to faster read times than a single Parquet file. Parquet data can be read into Deephaven tables from a _flat_ partitioned directory or a _key-value_ partitioned directory. Deephaven can also use Parquet metadata files, which boosts performance significantly.

When a partitioned Parquet directory is read into a Deephaven table, Deephaven represents the ingested data as a [partitioned table](/core/javadoc/io/deephaven/engine/table/PartitionedTable.html). Deephaven's partitioned tables are efficient representations of partitioned datasets and provide many useful methods for working with such data. See the [guide on partitioned tables](../../how-to-guides/partitioned-tables.md) for more information.

## Read a key-value partitioned Parquet directory

Key-value partitioned Parquet directories extend partitioning by organizing data based on key-value pairs in the directory structure. This allows for highly granular and flexible data access patterns, providing efficient querying for complex datasets. The downside is the added complexity in managing and maintaining the key-value pairs, which can be more intricate than other partitioning methods.

### From local storage

Use [`ParquetTools.readTable`](../../reference/data-import-export/Parquet/readTable.md) to read a key-value partitioned Parquet directory into a Deephaven partitioned table. The directory structure may be automatically inferred by [`ParquetTools.readTable`](../../reference/data-import-export/Parquet/readTable.md). Alternatively, provide the appropriate directory structure to the `readInstructions` argument using [`ParquetFileLayout.valueOf("KV_PARTITIONED")`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#KV_PARTITIONED). Providing this argument will boost performance, as no computation is required to infer the directory layout.

```groovy test-set=3 order=gradesInferred,gradesProvided
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout

// directory layout may be inferred
gradesInferred = ParquetTools.readTable("/data/examples/ParquetExamples/grades_kv/")

// or provided by user, yielding a performance boost
gradesProvided = ParquetTools.readTable(
    "/data/examples/ParquetExamples/grades_kv/",
    ParquetInstructions.builder().setFileLayout(ParquetFileLayout.valueOf("KV_PARTITIONED")).build()
)
```

If the key-value partitioned Parquet directory contains `_common_metadata` and `_metadata` files, utilize them by setting the `readInstructions` argument to [`ParquetFileLayout.valueOf("METADATA_PARTITIONED")`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#METADATA_PARTITIONED). This is the most performant option if the metadata files are available.

```groovy test-set=3
// use metadata files for maximum performance
gradesMetadata = ParquetTools.readTable(
    "/data/examples/ParquetExamples/grades_kv_meta/",
    ParquetInstructions.builder().setFileLayout(ParquetFileLayout.valueOf("METADATA_PARTITIONED")).build()
)
```

### From S3

Use [`ParquetTools.readTable`](../../reference/data-import-export/Parquet/readTable.md) to read a key-value partitioned Parquet directory from S3. Supply the `setSpecialInstructions` method with an instance of the [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) class, and supply the `setFileLayout` method with [`ParquetFileLayout.KV_PARTITIONED`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#KV_PARTITIONED) for maximum performance.

```groovy test-set=4 order=gradesInferred,gradesProvided docker-config=minio
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
            .endpointOverride("http://minio.example.com:9000")
            .credentials(credentials)
            .build()
    ).build()
)

// or provided
gradesProvided = ParquetTools.readTable(
    "s3://example-bucket/grades_kv/",
    ParquetInstructions.builder()
        .setFileLayout(ParquetFileLayout.valueOf("KV_PARTITIONED"))
        .setSpecialInstructions(
            S3Instructions.builder()
                .regionName("us-east-1")
                .endpointOverride("http://minio.example.com:9000")
                .credentials(credentials)
                .build()
        )
        .build()
)
```

S3-hosted key-value partitioned Parquet datasets may also have `_common_metadata` and `_metadata` files. Utilize them by setting the `setFileLayout` argument to [`ParquetFileLayout.valueOf("METADATA_PARTITIONED")`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html).

```groovy test-set=4 docker-config=minio
credentials = Credentials.basic("example_username", "example_password")

// use metadata files for maximum performance
gradesMetadata = ParquetTools.readTable(
    "s3://example-bucket/grades_kv_meta/",
    ParquetInstructions.builder()
        .setFileLayout(ParquetFileLayout.valueOf("METADATA_PARTITIONED"))
        .setSpecialInstructions(
            S3Instructions.builder()
                .regionName("us-east-1")
                .endpointOverride("http://minio.example.com:9000")
                .credentials(credentials)
                .build()
        )
        .build()
)
```

## Read a flat partitioned Parquet directory

A flat partitioned Parquet directory stores data without nested subdirectories. Each file contains partition information within its filename or as metadata. This approach simplifies directory management compared to hierarchical partitioning but can lead to larger directory listings, which might affect performance with many partitions.

### From local storage

Read local flat partitioned Parquet directories into Deephaven tables with [`ParquetTools.readTable`](../../reference/data-import-export/Parquet/readTable.md). Set the `readInstructions` argument to [`ParquetFileLayout.valueOf("FLAT_PARTITIONED")`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#FLAT_PARTITIONED) for maximum performance.

```groovy test-set=5 order=gradesInferred,gradesProvided
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout

// directory layout may be inferred
gradesInferred = ParquetTools.readTable("/data/examples/ParquetExamples/grades_flat/")

// or provided by user, yielding a performance boost
gradesProvided = ParquetTools.readTable(
    "/data/examples/ParquetExamples/grades_flat/",
    ParquetInstructions.builder().setFileLayout(ParquetFileLayout.valueOf("FLAT_PARTITIONED")).build()
)
```

### From S3

Use [`ParquetTools.readTable`](../../reference/data-import-export/Parquet/readTable.md) to read a flat partitioned Parquet directory from S3. Supply the `special_instructions` argument with an instance of the [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) class, and set the `file_layout` argument to [`ParquetFileLayout.FLAT_PARTITIONED`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#FLAT_PARTITIONED) for maximum performance.

```groovy test-set=6 order=gradesInferred,gradesProvided docker-config=minio
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
            .endpointOverride("http://minio.example.com:9000")
            .credentials(credentials)
            .build()
    ).build()
)

// or provided
gradesProvided = ParquetTools.readTable(
    "s3://example-bucket/grades_flat/",
    ParquetInstructions.builder()
        .setFileLayout(ParquetFileLayout.valueOf("FLAT_PARTITIONED"))
        .setSpecialInstructions(
            S3Instructions.builder()
                .regionName("us-east-1")
                .endpointOverride("http://minio.example.com:9000")
                .credentials(credentials)
                .build()
        )
        .build()
)
```

If the S3-hosted flat partitioned Parquet dataset has `_common_metadata` and `_metadata` files, utilize them by supplying the `setFileLayout` method with [`ParquetFileLayout.METADATA_PARTITIONED`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#METADATA_PARTITIONED).

```groovy test-set=6 docker-config=minio
credentials = Credentials.basic("example_username", "example_password")

// use metadata files for maximum performance
gradesMetadata = ParquetTools.readTable(
    "s3://example-bucket/grades_flat_meta/",
    ParquetInstructions.builder()
        .setFileLayout(ParquetFileLayout.valueOf("METADATA_PARTITIONED"))
        .setSpecialInstructions(
            S3Instructions.builder()
                .regionName("us-east-1")
                .endpointOverride("http://minio.example.com:9000")
                .credentials(credentials)
                .build()
        )
        .build()
)
```

## Related documentation

- [Parquet formats](./parquet-formats.md)
- [Parquet export](./parquet-export.md)
- [Parquet instructions](./parquet-instructions.md)
