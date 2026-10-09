---
title: Export Deephaven tables to Parquet files
---

The [Deephaven Parquet Python module](/core/pydoc/code/deephaven.parquet.html#module-deephaven.parquet) provides tools to integrate Deephaven with the Parquet file format. This document covers writing Deephaven tables to single Parquet files, key-value partitioned Parquet directories, and flat partitioned Parquet directories. You can write each layout to local storage or to S3.

By default, Deephaven writes Parquet files with `SNAPPY` compression. To choose a different codec or set other write options, see [Optional arguments](#optional-arguments).

> [!NOTE]
> When writing to S3, run Deephaven in the same AWS region as the S3 bucket for the best performance. To improve performance further, store the data in a directory bucket in a single AWS Availability Zone, and run Deephaven in that same Availability Zone. See the [AWS article on S3 Express One Zone directory buckets](https://community.aws/content/2ZDARM0xDoKSPDNbArrzdxbO3ZZ/s3-express-one-zone?lang=en) for more information.
>
> The S3 examples use placeholder credentials and endpoints. Replace them with the values for your S3 instance.

First, create some tables to use in the examples in this guide.

```python test-set=1 order=grades,math_grades,science_grades,history_grades docker-config=rustfs
from deephaven import new_table, merge
from deephaven.column import int_col, string_col

math_grades = new_table(
    [
        string_col("Name", ["Ashley", "Jeff", "Rita", "Zach"]),
        string_col("Class", ["Math", "Math", "Math", "Math"]),
        int_col("Test1", [92, 78, 87, 74]),
        int_col("Test2", [94, 88, 81, 70]),
    ]
)

science_grades = new_table(
    [
        string_col("Name", ["Ashley", "Jeff", "Rita", "Zach"]),
        string_col("Class", ["Science", "Science", "Science", "Science"]),
        int_col("Test1", [87, 90, 99, 80]),
        int_col("Test2", [91, 83, 95, 78]),
    ]
)

history_grades = new_table(
    [
        string_col("Name", ["Ashley", "Jeff", "Rita", "Zach"]),
        string_col("Class", ["History", "History", "History", "History"]),
        int_col("Test1", [82, 87, 84, 76]),
        int_col("Test2", [88, 92, 85, 78]),
    ]
)

grades = merge([math_grades, science_grades, history_grades])

grades_partitioned = grades.partition_by("Class")
```

## Write to a single Parquet file

### To local storage

Write a Deephaven table to a single Parquet file with [`parquet.write`](../../reference/data-import-export/Parquet/writeTable.md). Pass the table as the `table` argument and the destination file path as the `path` argument. The `path` must end with the `.parquet` file extension.

```python test-set=1
from deephaven import parquet

parquet.write(table=grades, path="/data/grades/grades.parquet")
```

To use a different compression codec, set the `compression_codec_name` argument. [Optional arguments](#optional-arguments) lists the available codecs.

```python test-set=1
parquet.write(
    table=grades, path="/data/grades/grades_gzip.parquet", compression_codec_name="GZIP"
)
```

Write `_metadata` and `_common_metadata` files by setting the `generate_metadata_files` argument to `True`. These files hold the schema and other metadata for the Parquet files that the write produces, and Deephaven places them in the destination directory. Readers use them to find the data files and the full schema without listing the directory tree, so they matter most for the [partitioned Parquet directories](#partitioned-parquet-directories) described later in this guide. See [Metadata-partitioned directories](./parquet-formats.md#metadata-partitioned-directories) for what each file contains.

```python test-set=1
parquet.write(
    table=grades, path="/data/grades_meta/grades.parquet", generate_metadata_files=True
)
```

### To S3

Use [`parquet.write`](../../reference/data-import-export/Parquet/writeTable.md) to write Deephaven tables to Parquet files on S3. The `path` should be the URI of the destination file in S3. Supply an instance of the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class to the `special_instructions` argument to specify the details of the connection to the S3 instance. See [Special instructions (S3 only)](#special-instructions-s3-only) for the `S3Instructions` arguments that apply when writing.

```python test-set=1
from deephaven.experimental import s3

credentials = s3.Credentials.basic(
    access_key_id="example_username", secret_access_key="example_password"
)

parquet.write(
    table=grades,
    path="s3://example-bucket/grades.parquet",
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://rustfs.example.com:9000",
        credentials=credentials,
    ),
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

A regular table needs a table definition, which is the table's schema with the partitioning columns marked. A regular table's own definition usually has no partitioning columns, so the write fails unless you pass a definition that has them. Pass it as the `table_definition` argument to [`write_partitioned`](../../reference/data-import-export/Parquet/writePartitioned.md).

One way to build a table definition is as a list of [`ColumnDefinition`](/core/pydoc/code/deephaven.column.html#deephaven.column.ColumnDefinition) objects. Create each one with [`col_def`](/core/pydoc/code/deephaven.column.html#deephaven.column.col_def), passing the column's name and a type from the [`deephaven.dtypes`](/core/pydoc/code/deephaven.dtypes.html) module. To mark a column as a partitioning column, set the `column_type` argument of `col_def` to [`ColumnType.PARTITIONING`](/core/pydoc/code/deephaven.column.html#deephaven.column.ColumnType).

Create a table definition for the `grades` table defined above.

```python test-set=1
from deephaven import dtypes
from deephaven.column import col_def, ColumnType

grades_def = [
    col_def("Name", dtypes.string),
    # Class is declared to be a partitioning column
    col_def("Class", dtypes.string, column_type=ColumnType.PARTITIONING),
    col_def("Test1", dtypes.int32),
    col_def("Test2", dtypes.int32),
]
```

### To local storage

Pass a regular table or a partitioned table to the `table` argument of [`parquet.write_partitioned`](../../reference/data-import-export/Parquet/writePartitioned.md), and set the `destination_dir` argument to the root directory for the partitioned Parquet data. Deephaven creates any missing directories in the path.

```python test-set=1
from deephaven import parquet

# write a regular Deephaven table; table_definition is required
parquet.write_partitioned(
    table=grades, destination_dir="/data/grades_kv/", table_definition=grades_def
)

# or write a partitioned table
parquet.write_partitioned(
    table=grades_partitioned, destination_dir="/data/grades_kv_partitioned/"
)
```

Set the `generate_metadata_files` argument to `True` to write `_metadata` and `_common_metadata` files at the root of the directory, as described in [Write to a single Parquet file](#write-to-a-single-parquet-file).

```python test-set=1
parquet.write_partitioned(
    table=grades_partitioned,
    destination_dir="/data/grades_kv_partitioned_meta/",
    generate_metadata_files=True,
)
```

### To S3

Use [`parquet.write_partitioned`](../../reference/data-import-export/Parquet/writePartitioned.md) to write key-value partitioned Parquet directories to S3. The `destination_dir` should be the URI of the destination directory in S3. Supply an instance of the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class to the `special_instructions` argument to specify the details of the connection to the S3 instance.

```python test-set=1
from deephaven.experimental import s3

credentials = s3.Credentials.basic(
    access_key_id="example_username", secret_access_key="example_password"
)

parquet.write_partitioned(
    table=grades_partitioned,
    destination_dir="s3://example-bucket/partitioned-directory/",
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://rustfs.example.com:9000",
        credentials=credentials,
    ),
)
```

## Write to a flat partitioned Parquet directory

A flat partitioned directory holds one Parquet file per table in a single directory.

### To local storage

Use [`parquet.write`](../../reference/data-import-export/Parquet/writeTable.md) or [`parquet.batch_write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.batch_write) to write Deephaven tables to Parquet files in flat partitioned directories.

Call [`parquet.write`](../../reference/data-import-export/Parquet/writeTable.md) once per table, giving each file its own path in the same directory, as in [Write to a single Parquet file](#write-to-a-single-parquet-file).

```python test-set=1
from deephaven import parquet

parquet.write(math_grades, "/data/grades_flat_1/math.parquet")
parquet.write(science_grades, "/data/grades_flat_1/science.parquet")
parquet.write(history_grades, "/data/grades_flat_1/history.parquet")
```

Use [`parquet.batch_write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.batch_write) to accomplish the same thing by passing multiple tables to the `tables` argument and multiple destination paths to the `paths` argument. If the tables have different definitions, also pass the `table_definition` argument.

```python test-set=1
parquet.batch_write(
    tables=[math_grades, science_grades, history_grades],
    paths=[
        "/data/grades_flat_2/math.parquet",
        "/data/grades_flat_2/science.parquet",
        "/data/grades_flat_2/history.parquet",
    ],
)
```

To write a [Deephaven partitioned table](../../how-to-guides/partitioned-tables.md) to a flat partitioned Parquet directory, get its constituent tables from the [`constituent_tables`](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable.constituent_tables) property and pass them to [`parquet.batch_write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.batch_write).

```python test-set=1
# write each constituent table to Parquet using batch_write
parquet.batch_write(
    tables=grades_partitioned.constituent_tables,
    paths=[
        "/data/grades_flat_3/math.parquet",
        "/data/grades_flat_3/science.parquet",
        "/data/grades_flat_3/history.parquet",
    ],
)
```

### To S3

Use [`parquet.batch_write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.batch_write) to write a list of Deephaven tables to a flat partitioned Parquet directory in S3. The `paths` should be the URIs of the destination files in S3. Supply an instance of the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class to the `special_instructions` argument to specify the details of the connection to the S3 instance.

```python test-set=1
from deephaven.experimental import s3

credentials = s3.Credentials.basic(
    access_key_id="example_username", secret_access_key="example_password"
)

parquet.batch_write(
    tables=[math_grades, science_grades, history_grades],
    paths=[
        "s3://example-bucket/grades-flat/math.parquet",
        "s3://example-bucket/grades-flat/science.parquet",
        "s3://example-bucket/grades-flat/history.parquet",
    ],
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://rustfs.example.com:9000",
        credentials=credentials,
    ),
)
```

## Optional arguments

The [`write`](../../reference/data-import-export/Parquet/writeTable.md), [`write_partitioned`](../../reference/data-import-export/Parquet/writePartitioned.md), and [`batch_write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.batch_write) functions from the [Deephaven Parquet Python module](/core/pydoc/code/deephaven.parquet.html#module-deephaven.parquet) all accept the following optional arguments, which control how Deephaven writes data to Parquet:

- `table_definition`: The table definition or schema, provided as a [`TableDefinition`](/core/pydoc/code/deephaven.table.html#deephaven.table.TableDefinition), a dictionary of string-[`DType`](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.DType) pairs, or a list of [`ColumnDefinition`](/core/pydoc/code/deephaven.column.html#deephaven.column.ColumnDefinition) instances. When not provided, Deephaven uses the column definitions of the table or tables being written.
- `col_instructions`: A list of [`ColumnInstruction`](../../reference/data-import-export/Parquet/ColumnInstruction.md) objects that customize how particular columns are written. See [Column instructions](#column-instructions). The default is `None`.
- `compression_codec_name`: The name of the [compression codec](https://www.javadoc.io/doc/org.apache.parquet/parquet-common/1.18.1/org/apache/parquet/hadoop/metadata/CompressionCodecName.html) to use for the whole file. Codecs trade write speed against compression ratio. The options are:
  - `SNAPPY`: A codec based on Google's [Snappy compression format](https://github.com/google/snappy/blob/main/format_description.txt) that aims for high speed and reasonable compression. This is the default.
  - `UNCOMPRESSED`: No compression.
  - `LZ4_RAW`: A codec based on the [LZ4 block format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md).
  - `LZO`: A codec based on or interoperable with the [LZO compression library](https://www.oberhumer.com/opensource/lzo/).
  - `GZIP`: A codec based on the GZIP format defined by [RFC 1952](https://tools.ietf.org/html/rfc1952). This differs from the related zlib and deflate formats.
  - `ZSTD`: A codec with a high compression ratio, based on the Zstandard format defined by [RFC 8478](https://tools.ietf.org/html/rfc8478).
  - `BROTLI`: A codec based on [Brotli](https://github.com/google/brotli), offering high compression ratios. Deephaven doesn't include a Brotli codec. To use this option, add a Brotli codec (`org.apache.hadoop.io.compress.BrotliCodec`) to the server classpath. Without one, the write fails with a `Failed to find CompressionCodec` error.
  - `LZ4`: **Deprecated.** Use `LZ4_RAW` instead.
- `max_dictionary_keys`: The maximum number of unique keys the writer should add to a dictionary page before switching to non-dictionary encoding. [Dictionary-based encoding](https://en.wikipedia.org/wiki/Dictionary_coder) stores each column's unique values in dictionary pages. The writer applies this limit only to `String` columns and columns of `String` arrays or vectors, and ignores it for a column whose [`use_dictionary`](#column-instructions) hint is `True`. Defaults to 2^20 (1,048,576).
- `max_dictionary_size`: The maximum number of bytes the writer should add to the dictionary before switching to non-dictionary encoding. The writer applies this limit only to `String` columns and columns of `String` arrays or vectors, and ignores it for a column whose [`use_dictionary`](#column-instructions) hint is `True`. Defaults to 2^20 (1,048,576).
- `target_page_size`: The target page size in bytes. Defaults to 65,536 bytes (64 KiB), which you can change with the `Parquet.defaultTargetPageSize` [configuration property](../configuration/configuration-properties.md).
- `generate_metadata_files`: Whether to generate Parquet `_metadata` and `_common_metadata` files. Defaults to `False`.
- `row_group_info`: Sets how the written data is divided into [row groups](./parquet-formats.md), the horizontal slices of rows that make up a Parquet file. The available [`RowGroupInfo`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.RowGroupInfo) options are:
  - `RowGroupInfo.single_group`: All data is within a single row group. This is the default.
  - `RowGroupInfo.max_rows(max_rows)`: Splits the data into row groups of no more than `max_rows` rows each.
  - `RowGroupInfo.max_groups(num_row_groups)`: Splits the data into the requested number of row groups of nearly equal size. If the row count is not evenly divisible by that number, some row groups contain one fewer row.
  - `RowGroupInfo.by_groups(groups, max_rows)`: Writes the rows for each unique combination of values in the `groups` columns as their own row group. `groups` is a list of column names. The rows for each group must be contiguous in the table, or the write raises an error. The optional `max_rows` argument splits any group with more rows than `max_rows` into smaller row groups, as `RowGroupInfo.max_rows` does.
- `index_columns`: The sets of columns to write [data indexes](../data-indexes.md) for, as a sequence of sequences. For example, `[["Col1"], ["Col1", "Col2"]]` writes an index on `Col1` and an index on `Col1` and `Col2`. Deephaven writes each index to its own Parquet file under a `.dh_metadata/indexes` subdirectory of the data file's directory. If the source table lacks a listed index, Deephaven computes it. The default depends on the function:
  - [`write`](../../reference/data-import-export/Parquet/writeTable.md): Writes the indexes on the source table.
  - [`write_partitioned`](../../reference/data-import-export/Parquet/writePartitioned.md) with a regular table: Writes the indexes on the source table, except single-column indexes on a partitioning column.
  - [`batch_write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.batch_write): Writes the indexes on the first table.
  - `write_partitioned` with a partitioned table: Writes no indexes.
- `special_instructions`: Connection details for writing to S3, provided as an [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) instance. See [Special instructions (S3 only)](#special-instructions-s3-only).

[`write_partitioned`](../../reference/data-import-export/Parquet/writePartitioned.md) also accepts a `base_name` argument, which sets the names of the Parquet files it writes in each partition directory.

### Column instructions

The `col_instructions` argument to [`write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write), [`write_partitioned`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write_partitioned), and [`batch_write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.batch_write) must be a list of [`ColumnInstruction`](../../reference/data-import-export/Parquet/ColumnInstruction.md) instances. Each `ColumnInstruction` maps a column in the Deephaven table to a column in the resulting Parquet files, and optionally sets an object codec or dictionary encoding for that column.

[`ColumnInstruction`](../../reference/data-import-export/Parquet/ColumnInstruction.md) has the following arguments that apply when writing:

- `column_name`: The name of the Deephaven column that these instructions apply to. Required when writing.
- `parquet_column_name`: The name of the corresponding column in the Parquet dataset.
- `codec_name`: The fully qualified name of an [`ObjectCodec`](/core/javadoc/io/deephaven/util/codec/ObjectCodec.html) class that serializes the column's values to and from bytes. Use it for types that have no language-agnostic Parquet representation. This is not the compression codec. The `compression_codec_name` argument sets that for the whole file.
- `codec_args`: An implementation-specific argument string passed to the codec named by `codec_name`.
- `use_dictionary`: A hint to keep [dictionary-based encoding](https://en.wikipedia.org/wiki/Dictionary_coder) for this column. The writer already tries dictionary encoding for every `String` column and every column of `String` arrays or vectors. Setting the hint to `True` removes the `max_dictionary_keys` and `max_dictionary_size` limits for the column, so the writer never falls back to non-dictionary encoding. The writer ignores the hint for other column types. The default is `False`.

### Special instructions (S3 only)

The `special_instructions` argument to [`write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write), [`write_partitioned`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write_partitioned), and [`batch_write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.batch_write) is relevant when writing to S3, an S3-compatible store, or Google Cloud Storage (`gs://` URIs), and takes an instance of the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class. This class specifies details for connecting to the store.

The following [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) arguments are relevant when writing. The API documentation lists the rest, including arguments that apply only to reads.

- `region_name`: The region name of the AWS S3 bucket. If you don't set it, the AWS SDK looks for a region in these places, in order:
  - The `aws.region` system property.
  - The `AWS_REGION` environment variable.
  - The `{user.home}/.aws/credentials` and `{user.home}/.aws/config` files.
  - The EC2 metadata service, when running on EC2.

  If none of these gives the bucket's correct region, Deephaven looks up the region with one additional request.

- `credentials`: The [credentials object](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.Credentials) for authenticating to the S3 instance. The default is `Credentials.resolving()`.
- `endpoint_override`: The endpoint to connect to. Connections to AWS rarely need it. Set it when connecting to a non-AWS, S3-compatible API. The default is `None`.
- `connection_timeout`: Time to wait for a successful S3 connection before timing out. The default is 2 seconds.
- `write_timeout`: The amount of time to wait when writing a fragment before timing out. The default is 2 seconds.
- `write_part_size`: The part or chunk size, in bytes, when writing to S3. The default is 10 MiB, and the minimum is 5,242,880 bytes (5 MiB).
- `num_concurrent_write_parts`: The maximum number of parts that can be uploaded concurrently without blocking. The default is 64. This value cannot exceed `max_concurrent_requests`.
- `max_concurrent_requests`: The maximum number of concurrent requests to make to S3, for reads and writes. The default is 256.
- `profile_name`: The AWS profile name used to configure the default region, credentials, and other settings.
- `config_file_path`: The path to the AWS configuration file.
- `credentials_file_path`: The path to the AWS credentials file.

The `access_key_id`, `secret_access_key`, and `anonymous_access` arguments are deprecated. Use [`Credentials.basic(access_key_id, secret_access_key)`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.Credentials.basic) or [`Credentials.anonymous()`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.Credentials.anonymous) for the `credentials` argument instead.

## Related documentation

- [Import Parquet files](./parquet-import.md)
- [Parquet formats](./parquet-formats.md)
