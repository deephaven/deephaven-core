---
title: Read Parquet files into Deephaven tables
sidebar_label: Read Parquet files
---

Deephaven integrates seamlessly with Parquet via the [Parquet Groovy module](/core/javadoc/io/deephaven/parquet/table/ParquetTools.html), making it easy to read Parquet files directly into Deephaven tables. This document covers reading data into tables from single Parquet files, flat Parquet directories, and partitioned key-value Parquet directories. This document also covers reading Parquet files from [S3 servers](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) into Deephaven tables, a common use case.

## Read a single Parquet file

Reading a single Parquet file involves loading data from one specific file into a table. This is straightforward and efficient when dealing with a relatively small dataset or when the data is consolidated into one file.

The basic syntax follows:

- `readTable("/data/output.parquet")`
- `readTable("/data/output_GZIP.parquet", "GZIP")`

### From local storage

Read single Parquet files into Deephaven tables with [`ParquetTools.readTable`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#readTable(io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder,io.deephaven.parquet.table.ParquetInstructions)). The function takes a single required argument `source`, which gives the full file path of the Parquet file.

```groovy test-set=1
import io.deephaven.parquet.table.ParquetTools

// pass the path of the local parquet file to `readTable`
sensors = ParquetTools.readTable("/data/examples/SensorData/parquet/SensorData_gzip.parquet")
```

### From an S3 server

Deephaven provides some tooling around reading from S3 servers with the [`io.deephaven.extensions.s3`](/core/javadoc/io/deephaven/extensions/s3/package-summary.html) Groovy module. This module contains the [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) class, which is used to establish communication with the S3 server. Learn more about this class in the [Parquet instructions document](./parquet-instructions.md#s3instructions-methods).

Use [`ParquetTools.readTable`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#readTable(io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder,io.deephaven.parquet.table.ParquetInstructions)) to read a single Parquet file from an S3 server, where the `source` argument is provided as the endpoint to the Parquet file on the server. Supply an instance of the [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) class to the `ParquetInstructions.Builder` to specify the details of the connection to the server.

```groovy test-set=2
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout
import io.deephaven.extensions.s3.S3Instructions
import io.deephaven.extensions.s3.Credentials

// pass URL to an S3 server, as well as instructions on how to talk to the server
drivestats = ParquetTools.readTable(
    "s3://drivestats-parquet/drivestats/year=2023/month=02/2023-02-1.parquet",
    ParquetInstructions.builder().setSpecialInstructions(S3Instructions.builder().regionName("us-west-004").endpointOverride("https://s3.us-west-004.backblazeb2.com").credentials(Credentials.anonymous()).readTimeout(parseDuration("PT10S")).build()).build()
)
```

## Partitioned Parquet directories

Deephaven supports reading partitioned Parquet directories. A partitioned Parquet directory organizes data into subdirectories based on one or more partition columns. This structure allows for more efficient data querying by pruning irrelevant partitions, leading to faster read times than a single Parquet file. Parquet data can be read into Deephaven tables from a _flat_ partitioned directory or a _key-value_ partitioned directory. Deephaven can also use Parquet metadata files, which boosts performance significantly.

When a partitioned Parquet directory is read into a Deephaven table, Deephaven represents the ingested data as a [partitioned table](/core/javadoc/io/deephaven/engine/table/PartitionedTable.html). Deephaven's partitioned tables are efficient representations of partitioned datasets and provide many useful methods for working with such data. See the [guide on partitioned tables](../../how-to-guides/partitioned-tables.md) for more information.

## Read a key-value partitioned Parquet directory

Key-value partitioned Parquet directories extend partitioning by organizing data based on key-value pairs in the directory structure. This allows for highly granular and flexible data access patterns, providing efficient querying for complex datasets. The downside is the added complexity in managing and maintaining the key-value pairs, which can be more intricate than other partitioning methods.

### From local storage

Use [`ParquetTools.readTable`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#readTable(io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder,io.deephaven.parquet.table.ParquetInstructions)) to read a key-value partitioned Parquet directory into a Deephaven partitioned table. The directory structure may be automatically inferred by [`ParquetTools.readTable`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#readTable(io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder,io.deephaven.parquet.table.ParquetInstructions)). Alternatively, provide the appropriate directory structure to the `readInstructions` argument using [`ParquetFileLayout.valueOf("KV_PARTITIONED")`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#KV_PARTITIONED). Providing this argument will boost performance, as no computation is required to infer the directory layout.

```groovy test-set=3 order=pemsInferred,pemsProvided
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout

// directory layout may be inferred
pemsInferred = ParquetTools.readTable("/data/examples/Pems/parquet/pems")

// or provided by user, yielding a performance boost
pemsProvided = ParquetTools.readTable(
    "/data/examples/Pems/parquet/pems",
    ParquetInstructions.builder().setFileLayout(ParquetFileLayout.valueOf("KV_PARTITIONED")).build()
)
```

If the key-value partitioned Parquet directory contains `_common_metadata` and `_metadata` files, utilize them by setting the `readInstructions` argument to [`ParquetFileLayout.valueOf("METADATA_PARTITIONED")`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#METADATA_PARTITIONED). This is the most performant option if the metadata files are available.

```groovy test-set=3
// use metadata files for maximum performance
pemsMetadata = ParquetTools.readTable(
    "/data/examples/Pems/parquet/pems",
    ParquetInstructions.builder().setFileLayout(ParquetFileLayout.valueOf("METADATA_PARTITIONED")).build()
)
```

### From an S3 server

Use [`ParquetTools.readTable`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#readTable(io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder,io.deephaven.parquet.table.ParquetInstructions)) to read a key-value partitioned Parquet directory from an S3 server. Supply the `setSpecialInstructions` method with an instance of the [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) class, and supply the `setFileLayout` method with [`ParquetFileLayout.KV_PARTITIONED`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#KV_PARTITIONED) for maximum performance.

```groovy test-set=4 order=performanceInferred,performanceProvided
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout
import io.deephaven.extensions.s3.S3Instructions
import io.deephaven.extensions.s3.Credentials

// directory layout may be inferred
performanceInferred = ParquetTools.readTable(
    "s3://ookla-open-data/parquet/performance/type=mobile/year=2023/",
    ParquetInstructions.builder().setSpecialInstructions(S3Instructions.builder().regionName("us-east-1").credentials(Credentials.anonymous()).readTimeout(parseDuration("PT10S")).build()).build()
)

// or provided
performanceProvided = ParquetTools.readTable(
    "s3://ookla-open-data/parquet/performance/type=mobile/year=2023/",
    ParquetInstructions.builder().setFileLayout(ParquetFileLayout.valueOf("KV_PARTITIONED")).setSpecialInstructions(S3Instructions.builder().regionName("us-east-1").credentials(Credentials.anonymous()).readTimeout(parseDuration("PT10S")).build()).build()
)
```

S3-hosted key-value partitioned Parquet datasets may also have `_common_metadata` and `_metadata` files. Utilize them by setting the `setFileLayout` argument to [`ParquetFileLayout.valueOf("KV_PARTITIONED")`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html).

> [!NOTE]
> This example is for illustrative purposes and is not runnable.

```groovy skip-test
// use metadata files for maximum performance
kvMetadata = ParquetTools.readTable(
    "/s3/path/to/kvDirectory",
    ParquetInstructions.builder().setFileLayout(
        ParquetFileLayout.valueOf("METADATA_PARTITIONED")
    ).setSpecialInstructions(
        S3Instructions.builder().regionName("kvDirectoryRegion").credentials(Credentials.anonymous()).readTimeout(parseDuration("PT10S")).build()
    ).build()
)
```

## Read a flat partitioned Parquet directory

A flat partitioned Parquet directory stores data without nested subdirectories. Each file contains partition information within its filename or as metadata. This approach simplifies directory management compared to hierarchical partitioning but can lead to larger directory listings, which might affect performance with many partitions.

### From local storage

Read local flat partitioned Parquet directories into Deephaven tables with [`ParquetTools.readTable`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#readTable(io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder,io.deephaven.parquet.table.ParquetInstructions)). Set the `readInstructions` argument to [`ParquetFileLayout.valueOf("FLAT_PARTITIONED")`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#FLAT_PARTITIONED) for maximum performance.

> [!NOTE]
> This example is for illustrative purposes and is not runnable.

```groovy skip-test
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.parquet.table.ParquetInstructions.ParquetFileLayout

// directory layout may be inferred
flatDataInferred = ParquetTools.readTable("/local/path/to/flat_directory")

// or provided by user, yielding a performance boost
flatDataProvided = parquet.read(
    "/local/path/to/flat_directory",
    ParquetInstructions.builder().setFileLayout(ParquetFileLayout.valueOf("FLAT_PARTITIONED")).build()
)
```

### From an S3 server

Use [`ParquetTools.readTable`](https://deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#readTable(io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder,io.deephaven.parquet.table.ParquetInstructions)) to read a flat partitioned Parquet directory from an S3 server. Supply the `special_instructions` argument with an instance of the [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) class, and set the `file_layout` argument to [`ParquetFileLayout.FLAT_PARTITIONED`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#FLAT_PARTITIONED) for maximum performance.

> [!NOTE]
> This example is for illustrative purposes and is not runnable.

```groovy skip-test
// directory layout may be inferred
flatS3Inferred = ParquetTools.readTable(
    "/s3/path/to/flatDirectory",
    ParquetInstructions.builder().setSpecialInstructions(
        S3Instructions.builder().regionName("flatDirectoryRegion").credentials(Credentials.anonymous()).readTimeout(parseDuration("PT10s"))
    ).build()
)

// or provided
flatS3Provided = ParquetTools.readTable(
    "/s3/path/to/flatDirectory",
    ParquetInstructions.builder().setFileLayout(
        ParquetFileLayout.valueOf("FLAT_PARTITIONED")
    ).setSpecialInstructions(
        S3Instructions.builder().regionName("flatDirectoryRegion").credentials(Credentials.anonymous()).readTimeout(parseDuration("PT10S")).build()
    ).build()
)
```

If the S3-hosted flat partitioned Parquet dataset has `_common_metadata` and `_metadata` files, utilize them by supplying the `setFileLayout` method with [`ParquetFileLayout.METADATA_PARTITIONED`](/core/javadoc/io/deephaven/parquet/table/ParquetInstructions.ParquetFileLayout.html#METADATA_PARTITIONED).

> [!NOTE]
> This example is for illustrative purposes and is not runnable.

```groovy skip-test
// use metadata files for maximum performance
flatMetadata = ParquetTools.readTable(
    "/s3/path/to/flatDirectory",
    ParquetInstructions.builder().setFileLayout(
        ParquetFileLayout.valueOf("METADATA_PARTITIONED")
    ).setSpecialInstructions(
        S3Instructions.builder().regionName("kvDirectoryRegion").credentials(Credentials.anonymous()).readTimeout(parseDuration("PT10S")).build()
    ).build()
)
```

## Related documentation

- [Parquet formats](./parquet-formats.md)
- [Parquet export](./parquet-export.md)
- [Parquet instructions](./parquet-instructions.md)
