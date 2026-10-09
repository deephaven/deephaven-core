---
title: Read Parquet files into Deephaven tables
---

Deephaven reads Parquet files directly into Deephaven tables with the [Parquet Python module](/core/pydoc/code/deephaven.parquet.html#module-deephaven.parquet). This document covers reading data into tables from single Parquet files, key-value partitioned Parquet directories, and flat partitioned Parquet directories. This document also covers reading Parquet files from [S3](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) into Deephaven tables.

## Read a single Parquet file

Read a single Parquet file when one file holds all of the table's data.

### From local storage

Read single Parquet files into Deephaven tables with [`parquet.read`](../../reference/data-import-export/Parquet/readTable.md). The function takes a single required argument `path`, which gives the full file path of the Parquet file.

```python test-set=1
from deephaven import parquet

# pass the path of the local Parquet file to `read`
grades = parquet.read(path="/data/examples/ParquetExamples/grades/grades.parquet")
```

### From S3

The [`deephaven.experimental.s3`](/core/pydoc/code/deephaven.experimental.s3.html#module-deephaven.experimental.s3) Python module supports reading from S3. The module is experimental, so its API is subject to change. It contains the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class, which holds the settings Deephaven uses to connect to the S3 instance. Learn more about this class in the [special instructions section of this document](#special-instructions-s3-and-gcs).

Use [`parquet.read`](../../reference/data-import-export/Parquet/readTable.md) to read a single Parquet file from S3, where the `path` argument is the S3 URI of the Parquet file (for example, `s3://bucket/key.parquet`). Supply an instance of the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class to the `special_instructions` argument to specify the details of the connection to the S3 instance.

```python test-set=2 docker-config=rustfs
from deephaven import parquet
from deephaven.experimental import s3

# This example uses basic credentials - other options are available
credentials = s3.Credentials.basic(
    access_key_id="example_username", secret_access_key="example_password"
)

# Pass the S3 URI as well as instructions on how to talk to the S3 instance
grades = parquet.read(
    path="s3://example-bucket/grades/grades.parquet",
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://rustfs.example.com:9000",
        credentials=credentials,
    ),
)
```

> [!NOTE]
> When reading from S3, run the Deephaven instance in the same AWS region as the S3 bucket for the best performance. To improve performance further, store the data in a directory bucket in a single AWS Availability Zone, and run the Deephaven instance in that same Availability Zone. For more information, see the [AWS article on S3 Express One Zone directory buckets](https://community.aws/content/2ZDARM0xDoKSPDNbArrzdxbO3ZZ/s3-express-one-zone?lang=en).

## Read partitioned Parquet directories

A partitioned Parquet directory spreads the data for one table across many Parquet files. Deephaven reads two kinds of partitioned directory:

- A _key-value_ partitioned directory names each subdirectory after a partition column and its value, such as `Year=2024/`.
- A _flat_ partitioned directory keeps all of its Parquet files in a single directory, with no subdirectories and no partition columns.

Either kind can also contain the Parquet metadata files `_metadata` and `_common_metadata`, which describe the whole dataset. Reading through these files is faster than reading each Parquet file's own metadata. When Deephaven infers the layout of a directory that contains a `_metadata` file, it uses the metadata files automatically.

When Deephaven reads a partitioned Parquet directory, it returns a single table that contains the data from every file. For a key-value partitioned directory, each partition column appears as a column in the result. To work with each partition as a separate table, call [`partition_by`](../../reference/table-operations/group-and-aggregate/partitionBy.md) on the result. See the [guide on partitioned tables](../../how-to-guides/partitioned-tables.md) for more information.

### Read a key-value partitioned Parquet directory

A key-value partitioned directory stores each partition in a `column=value` subdirectory, such as `Year=2024/`. Compared with a [flat partitioned directory](#read-a-flat-partitioned-parquet-directory), it lets filters on partition columns skip reading the files in non-matching subdirectories, at the cost of a deeper directory tree to maintain.

#### From local storage

Use [`parquet.read`](../../reference/data-import-export/Parquet/readTable.md) to read a key-value partitioned Parquet directory into a Deephaven table. `parquet.read` can infer the directory layout. Alternatively, set the `file_layout` argument to [`parquet.ParquetFileLayout.KV_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout.KV_PARTITIONED). Providing the layout skips only the check for a `_metadata` file. Deephaven still infers the partition columns and schema from the files unless you also pass a schema in the [`table_definition`](#arguments) argument.

```python test-set=3 order=grades_inferred,grades_provided
from deephaven import parquet

# directory layout may be inferred
grades_inferred = parquet.read(path="/data/examples/ParquetExamples/grades_kv/")

# or provided by user, skipping the check for a _metadata file
grades_provided = parquet.read(
    path="/data/examples/ParquetExamples/grades_kv/",
    file_layout=parquet.ParquetFileLayout.KV_PARTITIONED,
)
```

If the key-value partitioned Parquet directory contains `_common_metadata` and `_metadata` files, Deephaven uses them automatically when it infers the layout. To state the layout explicitly, set the `file_layout` argument to [`parquet.ParquetFileLayout.METADATA_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout.METADATA_PARTITIONED). Reading through the metadata files is the most performant option when they are available.

```python test-set=3
# read through the metadata files
grades_metadata = parquet.read(
    path="/data/examples/ParquetExamples/grades_kv_meta/",
    file_layout=parquet.ParquetFileLayout.METADATA_PARTITIONED,
)
```

#### From S3

Use [`parquet.read`](../../reference/data-import-export/Parquet/readTable.md) to read a key-value partitioned Parquet directory from S3. Supply the `special_instructions` argument with an instance of the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class. To skip the check for a `_metadata` file, set the `file_layout` argument to [`parquet.ParquetFileLayout.KV_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout.KV_PARTITIONED). For performance, run Deephaven in the same AWS region as the bucket, as described in [Read a single Parquet file from S3](#from-s3).

```python test-set=4 order=grades_inferred,grades_provided docker-config=rustfs
from deephaven import parquet
from deephaven.experimental import s3

credentials = s3.Credentials.basic(
    access_key_id="example_username", secret_access_key="example_password"
)

# directory layout may be inferred
grades_inferred = parquet.read(
    path="s3://example-bucket/grades_kv/",
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://rustfs.example.com:9000",
        credentials=credentials,
    ),
)

# or provided
grades_provided = parquet.read(
    path="s3://example-bucket/grades_kv/",
    file_layout=parquet.ParquetFileLayout.KV_PARTITIONED,
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://rustfs.example.com:9000",
        credentials=credentials,
    ),
)
```

S3-hosted key-value partitioned Parquet datasets may also have `_common_metadata` and `_metadata` files. Deephaven uses them automatically when it infers the layout. To state the layout explicitly, set the `file_layout` argument to [`parquet.ParquetFileLayout.METADATA_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout.METADATA_PARTITIONED).

```python test-set=4 docker-config=rustfs
# read through the metadata files
grades_metadata = parquet.read(
    path="s3://example-bucket/grades_kv_meta/",
    file_layout=parquet.ParquetFileLayout.METADATA_PARTITIONED,
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://rustfs.example.com:9000",
        credentials=credentials,
    ),
)
```

### Read a flat partitioned Parquet directory

A flat partitioned Parquet directory is a single directory of Parquet files with no partition subdirectories. Deephaven reads the `.parquet` files in the directory into one table. For a local directory, it skips hidden files whose names start with `.`. A flat layout is simpler to manage than the nested subdirectories of a [key-value partitioned directory](#read-a-key-value-partitioned-parquet-directory). Because a flat directory has no partition columns, filters can't skip its files by partition value the way they can with a key-value partitioned directory.

#### From local storage

Read local flat partitioned Parquet directories into Deephaven tables with [`parquet.read`](../../reference/data-import-export/Parquet/readTable.md). `parquet.read` can infer the directory layout. To skip the check for a `_metadata` file, set the `file_layout` argument to [`parquet.ParquetFileLayout.FLAT_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout.FLAT_PARTITIONED).

```python test-set=5 order=grades_inferred,grades_provided
from deephaven import parquet

# directory layout may be inferred
grades_inferred = parquet.read(path="/data/examples/ParquetExamples/grades_flat/")

# or provided by user, skipping the check for a _metadata file
grades_provided = parquet.read(
    path="/data/examples/ParquetExamples/grades_flat/",
    file_layout=parquet.ParquetFileLayout.FLAT_PARTITIONED,
)
```

If the flat partitioned directory contains `_common_metadata` and `_metadata` files, Deephaven uses them automatically when it infers the layout. To state the layout explicitly, set the `file_layout` argument to [`parquet.ParquetFileLayout.METADATA_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout.METADATA_PARTITIONED).

```python test-set=5
# read through the metadata files
grades_metadata = parquet.read(
    path="/data/examples/ParquetExamples/grades_flat_meta/",
    file_layout=parquet.ParquetFileLayout.METADATA_PARTITIONED,
)
```

#### From S3

Use [`parquet.read`](../../reference/data-import-export/Parquet/readTable.md) to read a flat partitioned Parquet directory from S3. Supply the `special_instructions` argument with an instance of the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class. To skip the check for a `_metadata` file, set the `file_layout` argument to [`parquet.ParquetFileLayout.FLAT_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout.FLAT_PARTITIONED). For performance, run Deephaven in the same AWS region as the bucket, as described in [Read a single Parquet file from S3](#from-s3).

```python test-set=6 order=grades_inferred,grades_provided docker-config=rustfs
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
        endpoint_override="http://rustfs.example.com:9000",
        credentials=credentials,
    ),
)

# or provided
grades_provided = parquet.read(
    path="s3://example-bucket/grades_flat/",
    file_layout=parquet.ParquetFileLayout.FLAT_PARTITIONED,
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://rustfs.example.com:9000",
        credentials=credentials,
    ),
)
```

If the S3-hosted flat partitioned Parquet dataset has `_common_metadata` and `_metadata` files, Deephaven uses them automatically when it infers the layout. To state the layout explicitly, set the `file_layout` argument to [`parquet.ParquetFileLayout.METADATA_PARTITIONED`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout.METADATA_PARTITIONED).

```python test-set=6 docker-config=rustfs
# read through the metadata files
grades_metadata = parquet.read(
    path="s3://example-bucket/grades_flat_meta/",
    file_layout=parquet.ParquetFileLayout.METADATA_PARTITIONED,
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://rustfs.example.com:9000",
        credentials=credentials,
    ),
)
```

## Arguments

[`parquet.read`](../../reference/data-import-export/Parquet/readTable.md) accepts the following arguments. The examples above use only some of them.

- `path` (required): The Parquet file, metadata file (`_metadata` or `_common_metadata`), or directory to read. This is typically a string containing a local path, an S3 URI, or a Google Cloud Storage (`gs://`) URI.
- `col_instructions`: Per-column read settings, such as the Parquet column name to read into each Deephaven column, provided as a list of [`ColumnInstruction`](../../reference/data-import-export/Parquet/ColumnInstruction.md) objects. The default is `None`, which means no specialization for any column.
- `is_legacy_parquet`: `True` or `False` indicating if the Parquet data is in legacy Parquet format. When `True`, Deephaven reads binary columns that have no logical type annotation and no recorded codec as strings rather than as byte arrays. Some older Parquet writers store strings this way. The default is `False`.
- `is_refreshing`: `True` or `False` indicating if the Parquet data represents a refreshing source. When `True`, Deephaven checks a partitioned directory for new Parquet files and adds their rows to the table, so the result is a [refreshing table](../../conceptual/table-types.md). Refreshing reads aren't supported for a single Parquet file or for a directory read through its metadata files. The default is `False`.
- `file_layout`: The Parquet file or directory layout, provided as a [`ParquetFileLayout`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ParquetFileLayout). The default is `None`, which means Deephaven infers the layout.
- `table_definition`: The table definition or schema, provided as a [`TableDefinition`](/core/pydoc/code/deephaven.table.html#deephaven.table.TableDefinition), a dictionary of string-[`DType`](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.DType) pairs, or a list of [`ColumnDefinition`](/core/pydoc/code/deephaven.column.html#deephaven.column.ColumnDefinition) instances. When not provided, Deephaven infers the definition from the Parquet file(s).
- `special_instructions`: Special instructions for reading Parquet files from S3, an S3-compatible store, or Google Cloud Storage (`gs://` URIs), provided as an instance of [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions). The default is `None`.

### Column instructions

The `col_instructions` argument to [`parquet.read`](../../reference/data-import-export/Parquet/readTable.md) takes a list of [`ColumnInstruction`](../../reference/data-import-export/Parquet/ColumnInstruction.md) objects. Each `ColumnInstruction` maps a column in the Parquet data to a column in the Deephaven table.

[`ColumnInstruction`](../../reference/data-import-export/Parquet/ColumnInstruction.md) has the following arguments:

- `column_name`: The name of the Deephaven table column that these instructions apply to. Required.
- `parquet_column_name`: The name of the corresponding column in the Parquet dataset. Required when reading.
- `codec_name`: The fully qualified name of an [`ObjectCodec`](/core/javadoc/io/deephaven/util/codec/ObjectCodec.html) class that serializes the column's values to and from bytes, such as `io.deephaven.util.codec.LocalDateCodec`. Use a codec for types that have no language-agnostic Parquet representation. This is not a compression codec.
- `codec_args`: An implementation-specific argument string passed to the codec named by `codec_name`.
- `unsigned_long_target`: The Deephaven type to read an unsigned 64-bit integer (`UINT_64`) column as, provided as a [`parquet.UnsignedLongTarget`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.UnsignedLongTarget). The default is `None`, which reads such columns as `BigInteger`.

The `use_dictionary` argument applies only when writing. See [Parquet export](./parquet-export.md).

### Special instructions (S3 and GCS)

The `special_instructions` argument to [`parquet.read`](../../reference/data-import-export/Parquet/readTable.md) is relevant when reading from S3, an S3-compatible store, or Google Cloud Storage (`gs://` URIs), and takes an instance of the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class. This class specifies details for connecting to the store.

Deephaven reads an S3 object in fragments, which are byte ranges of the file that it fetches with separate requests. Several of the arguments below tune how Deephaven fetches fragments.

[`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) has the following arguments for reading:

- `region_name`: The AWS region of the S3 bucket that holds the Parquet data. When this is not set, the AWS SDK looks for a region in the following places, in order:
  - The `aws.region` system property.
  - The `AWS_REGION` environment variable.
  - The `{user.home}/.aws/credentials` and `{user.home}/.aws/config` files.
  - The EC2 metadata service, when Deephaven runs in EC2.

  If none of these gives a region, or the region is wrong for the bucket, Deephaven finds the correct region itself. This costs one extra request.

- `credentials`: A [`Credentials`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.Credentials) object for authenticating to the S3 instance. The default is `None`, which uses [`Credentials.resolving`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.Credentials.resolving).
- `endpoint_override`: The endpoint to connect to. Set it when connecting to a non-AWS, S3-compatible API. Connections to AWS typically don't need it. The default is `None`.
- `read_ahead_count`: The number of fragments that Deephaven reads asynchronously ahead of the fragment it is currently reading. The default is 32.
- `fragment_size`: The maximum size of each fragment to read in bytes. The default is 65536 bytes (64 KiB).
- `read_timeout`: The time to wait for a fragment read to complete before timing out. The default is 2 seconds.
- `max_concurrent_requests`: The maximum number of concurrent requests to make to S3. The default is 256.
- `connection_timeout`: The time to wait for a successful S3 connection before timing out. The default is 2 seconds.
- `profile_name`: The AWS profile name used to configure the default region, credentials, and so on.
- `config_file_path`: The path to the AWS configuration file.
- `credentials_file_path`: The path to the AWS credentials file.

The `write_timeout`, `write_part_size`, and `num_concurrent_write_parts` arguments apply only when writing to S3. See the [`S3Instructions` pydoc](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) for details.

The `access_key_id`, `secret_access_key`, and `anonymous_access` arguments are deprecated. Use [`Credentials.basic(access_key_id, secret_access_key)`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.Credentials.basic) or [`Credentials.anonymous()`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.Credentials.anonymous) for the `credentials` argument instead.

## Related documentation

- [Parquet formats](./parquet-formats.md)
- [Parquet export](./parquet-export.md)
