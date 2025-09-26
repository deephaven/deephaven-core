---
title: Import Parquet Files to Deephaven Tables
sidebar_label: Import Parquet files
---

Deephaven integrates seamlessly with Parquet via the [Parquet Python module](/core/pydoc/code/deephaven.parquet.html#module-deephaven.parquet), making it easy to read Parquet files directly into Deephaven tables. This document covers reading data into tables from single Parquet files, flat Parquet directories, and partitioned key-value Parquet directories. This document also covers reading Parquet files from [S3](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) into Deephaven tables, a common use case.

> [!NOTE]
> Much of this document covers reading Parquet files from S3. For the best performance, the Deephaven instance should be running in the same AWS region as the S3 bucket. Additional performance improvements can be made by using directory buckets to localize all data to a single AWS sub-region, and running the Deephaven instance in that same sub-region. See [this article](https://community.aws/content/2ZDARM0xDoKSPDNbArrzdxbO3ZZ/s3-express-one-zone?lang=en) for more information on S3 directory buckets.

## Read a single Parquet file

Reading a single Parquet file involves loading data from one specific file into a table. This is straightforward and efficient when dealing with a relatively small dataset or when the data is consolidated into one file.

### From local storage

Read single Parquet files into Deephaven tables with [`parquet.read`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.read). The function takes a single required argument `path`, which gives the full file path of the Parquet file.

```python test-set=1
from deephaven import parquet

# pass the path of the local parquet file to `read`
grades = parquet.read(path="/data/examples/ParquetExamples/grades/grades.parquet")
```

### From S3

> [!CAUTION]
> The [`deephaven.experimental.s3`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) integration is currently experimental, so the API is subject to change.

Deephaven provides some tooling around reading from S3 with the [`deephaven.experimental.s3`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) Python module. This module contains the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class, which is used to establish communication with the S3 instance. Learn more about this class in the [special instructions section of this document](#special-instructions-s3-only).

Use [`parquet.read`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.read) to read a single Parquet file from S3, where the `path` argument is provided as the endpoint to the Parquet file on the S3 instance. Supply an instance of the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class to the `special_instructions` argument to specify the details of the connection to the S3 instance.

```python test-set=2 docker-config=minio
from deephaven import parquet
from deephaven.experimental import s3

# This example uses basic credentials - other options are available
credentials = s3.Credentials.basic(
    access_key_id="example_username", secret_access_key="example_password"
)

# Pass the S3 URL as well as instructions on how to talk to the S3 instance
grades = parquet.read(
    path="s3://example-bucket/grades/grades.parquet",
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://minio.example.com:9000",
        credentials=credentials,
    ),
)
```

## Partitioned Parquet directories

Deephaven supports reading partitioned Parquet directories. A partitioned Parquet directory organizes data into subdirectories based on one or more partition columns. This structure allows for more efficient data querying by pruning irrelevant partitions, leading to faster read times than a single Parquet file. Parquet data can be read into Deephaven tables from a _flat_ partitioned directory or a _key-value_ partitioned directory. Deephaven can also use Parquet metadata files, which boosts performance significantly.

When a partitioned Parquet directory is read into a Deephaven table, Deephaven represents the ingested data as a [partitioned table](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable). Deephaven's partitioned tables are efficient representations of partitioned datasets and provide many useful methods for working with such data. See the [guide on partitioned tables](../../how-to-guides/partitioned-tables.md) for more information.

## Read a key-value partitioned Parquet directory

Key-value partitioned Parquet directories extend partitioning by organizing data based on key-value pairs in the directory structure. This allows for highly granular and flexible data access patterns, providing efficient querying for complex datasets. The downside is the added complexity in managing and maintaining the key-value pairs, which can be more intricate than other partitioning methods.

### From local storage

Use [`parquet.read`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.read) to read a key-value partitioned Parquet directory into a Deephaven partitioned table. The directory structure may be automatically inferred by [`parquet.read`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.read). Alternatively, provide the appropriate directory structure to the `file_layout` argument using [`parquet.ParquetFileLayout.KV_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout.KV_PARTITIONED). Providing this argument will boost performance, as no computation is required to infer the directory layout.

```python test-set=3 order=grades_inferred,grades_provided
from deephaven import parquet

# directory layout may be inferred
grades_inferred = parquet.read(path="/data/examples/ParquetExamples/grades_kv/")

# or provided by user, yielding a performance boost
grades_provided = parquet.read(
    path="/data/examples/ParquetExamples/grades_kv/",
    file_layout=parquet.ParquetFileLayout.KV_PARTITIONED,
)
```

If the key-value partitioned Parquet directory contains `_common_metadata` and `_metadata` files, utilize them by setting the `file_layout` argument to [`parquet.ParquetFileLayout.METADATA_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout.METADATA_PARTITIONED). This is the most performant option if the metadata files are available.

```python test-set=3
# use metadata files for maximum performance
grades_metadata = parquet.read(
    path="/data/examples/ParquetExamples/grades_kv_meta/",
    file_layout=parquet.ParquetFileLayout.METADATA_PARTITIONED,
)
```

### From S3

Use [`parquet.read`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.read) to read a key-value partitioned Parquet directory from S3. Supply the `special_instructions` argument with an instance of the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class, and set the `file_layout` argument to [`parquet.ParquetFileLayout.KV_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout.KV_PARTITIONED) for maximum performance.

```python test-set=4 order=grades_inferred,grades_provided docker-config=minio
from deephaven import parquet
from deephaven.experimental import s3

# directory layout may be inferred
grades_inferred = parquet.read(
    path="s3://example-bucket/grades_kv/",
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://minio.example.com:9000",
        access_key_id="example_username",
        secret_access_key="example_password",
    ),
)

# or provided
grades_provided = parquet.read(
    path="s3://example-bucket/grades_kv/",
    file_layout=parquet.ParquetFileLayout.KV_PARTITIONED,
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://minio.example.com:9000",
        access_key_id="example_username",
        secret_access_key="example_password",
    ),
)
```

S3-hosted key-value partitioned Parquet datasets may also have `_common_metadata` and `_metadata` files. Utilize them by setting the `file_layout` argument to [`parquet.ParquetFileLayout.METADATA_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout.METADATA_PARTITIONED).

```python test-set=4 docker-config=minio
credentials = s3.Credentials.basic(
    access_key_id="example_username", secret_access_key="example_password"
)

# use metadata files for maximum performance
grades_metadata = parquet.read(
    path="s3://example-bucket/grades_kv_meta/",
    file_layout=parquet.ParquetFileLayout.METADATA_PARTITIONED,
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://minio.example.com:9000",
        credentials=credentials,
    ),
)
```

## Read a flat partitioned Parquet directory

A flat partitioned Parquet directory stores data without nested subdirectories. Each file contains partition information within its filename or as metadata. This approach simplifies directory management compared to hierarchical partitioning but can lead to larger directory listings, which might affect performance with many partitions.

### From local storage

Read local flat partitioned Parquet directories into Deephaven tables with [`parquet.read`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.read). Set the `file_layout` argument to [`parquet.ParquetFileLayout.FLAT_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout.FLAT_PARTITIONED) for maximum performance.

```python test-set=5 order=grades_inferred,grades_provided
from deephaven import parquet

# directory layout may be inferred
grades_inferred = parquet.read(path="/data/examples/ParquetExamples/grades_flat/")

# or provided by user, yielding a performance boost
grades_provided = parquet.read(
    path="/data/examples/ParquetExamples/grades_flat/",
    file_layout=parquet.ParquetFileLayout.FLAT_PARTITIONED,
)
```

### From S3

Use [`parquet.read`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.read) to read a flat partitioned Parquet directory from S3. Supply the `special_instructions` argument with an instance of the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class, and set the `file_layout` argument to [`parquet.ParquetFileLayout.FLAT_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout.FLAT_PARTITIONED) for maximum performance.

```python test-set=6 order=grades_inferred,grades_provided docker-config=minio
from deephaven import parquet
from deephaven.experimental import s3

credentials = s3.Credentials.basic(
    access_key_id="example_username", secret_access_key="example_password"
)

# directory layout may be inferred
grades_inferred = parquet.read(
    path="s3://example-bucket/grades_flat/",
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://minio.example.com:9000",
        credentials=credentials,
    ),
)

# or provided
grades_provided = parquet.read(
    path="s3://example-bucket/grades_flat/",
    file_layout=parquet.ParquetFileLayout.FLAT_PARTITIONED,
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://minio.example.com:9000",
        credentials=credentials,
    ),
)
```

If the S3-hosted flat partitioned Parquet dataset has `_common_metadata` and `_metadata` files, utilize them by setting the `file_layout` argument to [`parquet.ParquetFileLayout.METADATA_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout.METADATA_PARTITIONED).

```python test-set=6
credentials = s3.Credentials.basic(
    access_key_id="example_username", secret_access_key="example_password"
)

# use metadata files for maximum performance
grades_metadata = parquet.read(
    path="s3://example-bucket/grades_flat_meta/",
    file_layout=parquet.ParquetFileLayout.METADATA_PARTITIONED,
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://minio.example.com:9000",
        credentials=credentials,
    ),
)
```

## Optional arguments

The [`parquet.read`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.read) function takes many optional arguments, many of which were not included in these examples. Here are all of the arguments that [`parquet.read`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.read) accepts:

- `path`: The Parquet file or directory to read. This is typically a string containing a local file path or directory, or an endpoint for an S3 bucket.
- `col_instructions`: Instructions for customizations while reading particular columns, provided as a [`ColumnInstruction`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ColumnInstruction) or a list of [`ColumnInstruction`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ColumnInstruction)s. The default is `None`, which means no specialization for any column.
- `is_legacy_parquet`: `True` or `False` indicating if the Parquet data is in legacy Parquet format.
- `is_refreshing`: `True` or `False` indicating if the Parquet data represents a refreshing source.
- `file_layout`: The Parquet file or directory layout, provided as a [`ParquetFileLayout`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout). Default is `None`, which means the layout is inferred.
- `table_definition`: The table definition or schema, provided as a dictionary of string-[`DType`](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.DType) pairs, or as a list of [`ColumnDefinition`](/core/pydoc/code/deephaven.column.html#deephaven.column.ColumnDefinition) instances. When not provided, the column definitions implied by the table(s) are used.
- `special_instructions`: Special instructions for reading Parquet files, useful when reading files from a non-local S3 instance. These instructions are provided as an instance of [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions). Default is `None`.

## Column instructions and special instructions

### Column instructions

The `col_instructions` argument to [`parquet.read`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.read) must be an instance of the [`ColumnInstruction`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ColumnInstruction) class. This class maps specific columns in the Parquet data to specific columns in the Deephaven table, as well as specifies the method of compression used for that column.

[`ColumnInstruction`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ColumnInstruction) has the following arguments:

- `column_name`: The column name in the Deephaven table to apply these instructions.
- `parquet_column_name`: The name of the corresponding column in the Parquet dataset.
- `codec_name`: The name of the [compression codec](https://www.javadoc.io/doc/org.apache.parquet/parquet-hadoop/1.8.1/org/apache/parquet/hadoop/metadata/CompressionCodecName.html) to use.
- `codec_args`: An implementation-specific string that maps types to/from bytes. It is typically used in cases where there is no obvious language-agnostic representation in Parquet.
- `use_dictionary`: `True` or `False` indicating whether or not to use [dictionary-based encoding](https://en.wikipedia.org/wiki/Dictionary_coder) for string columns.

Of particular interest is the `codec_name` argument. This defines the particular type of compression used for the given column and can have significant implications for the speed of the import. The options are:

- `SNAPPY`: (default) Aims for high speed and a reasonable amount of compression. Based on [Google](https://github.com/google/snappy/blob/main/format_description.txt)'s Snappy compression format.
- `UNCOMPRESSED`: The output will not be compressed.
- `LZ4_RAW`: A codec based on the [LZ4 block format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md). Should always be used instead of `LZ4`.
- `LZO`: Compression codec based on or interoperable with the [LZO compression library](https://www.oberhumer.com/opensource/lzo/).
- `GZIP`: Compression codec based on the GZIP format (not the closely-related "zlib" or "deflate" formats) defined by [RFC 1952](https://tools.ietf.org/html/rfc1952).
- `ZSTD`: Compression codec with the highest compression ratio based on the Zstandard format defined by [RFC 8478](https://tools.ietf.org/html/rfc8478).
- `LZ4`: **Deprecated** Compression codec loosely based on the [LZ4 compression algorithm](https://github.com/lz4/lz4), but with an additional undocumented framing scheme. The framing is part of the original Hadoop compression library and was historically copied first in parquet-mr, then emulated with mixed results by parquet-cpp. Note that `LZ4` is deprecated; use `LZ4_RAW` instead.

### Special instructions (S3 only)

The `special_instructions` argument to [`parquet.read`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.read) is relevant when reading from an S3 instance and takes an instance of the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class. This class specifies details for connecting to the S3 instance.

[`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) has the following arguments:

- `region_name`: The region name of the AWS S3 bucket where the Parquet data exists. If not provided, the region name is picked by the AWS SDK from the 'aws.region' system property, the "AWS_REGION" environment variable, the \{user.home}/.aws/credentials, \{user.home}/.aws/config files, or from EC2 metadata service, if running in EC2. If no region name is derived from the above chain or the region name derived is incorrect for the bucket accessed, the correct region name will be derived internally, at the cost of one additional request.
- `credentials` : The [credentials object](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.Credentials) for authenticating to the S3 instance. The default is `None`.
- `endpoint_override`: The endpoint to connect to. Callers connecting to AWS do not typically need to set this; it is most useful when connecting to non-AWS, S3-compatible APIs. The default is `None`
- `anonymous_access`: `True` or `False` indicating the use of anonymous credentials. The default is `False`.
- `read_ahead_count`: The number of fragments asynchronously read ahead of the current fragment as the current fragment is being read. The default is `1`.
- `fragment_size`: The maximum size of each fragment to read in bytes. The default is 5 MB.
- `read_timeout`: The amount of time it takes to time out while reading a fragment. The default is 2 seconds.
- `max_concurrent_requests`: The maximum number of concurrent requests to make to S3. The default is 50.
- `max_cache_size`: The maximum number of fragments to cache in memory while reading. The default is 32.
- `connection_timeout`: Time to wait for a successful S3 connection before timing out. The default is 2 seconds.
- `access_key_id`: The access key for reading files. If set, `secret_access_key` must also be set.
- `secret_access_key`: The secret access key for reading files.

## Related documentation

- [Parquet formats](./parquet-formats.md)
- [Parquet export](./parquet-export.md)
