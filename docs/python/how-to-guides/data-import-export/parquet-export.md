---
title: Export Deephaven Tables to Parquet Files
sidebar_label: Export to Parquet
---

The [Deephaven Parquet Python module](/core/pydoc/code/deephaven.parquet.html#module-deephaven.parquet) provides tools to integrate Deephaven with the Parquet file format. This module makes it easy to write Deephaven tables to Parquet files and directories. This document covers writing Deephaven tables to single Parquet files, flat partitioned Parquet directories, and key-value partitioned Parquet directories.

By default, Deephaven tables are written to Parquet files using `SNAPPY` compression when writing the data. This default can be changed with the `compression_codec_name` argument in any of the writing functions discussed here or with the `codec_name` argument in the [`ColumnInstruction`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ColumnInstruction) class. See the [column instructions section](#column-instructions) for more information.

> [!NOTE]
> Much of this document covers writing Parquet files to S3. For the best performance, the Deephaven instance should be running in the same AWS region as the S3 bucket. Additional performance improvements can be made by using directory buckets to localize all data to a single AWS sub-region, and running the Deephaven instance in that same sub-region. See [this article](https://community.aws/content/2ZDARM0xDoKSPDNbArrzdxbO3ZZ/s3-express-one-zone?lang=en) for more information on S3 directory buckets.

> [!NOTE]
> Some of the code in this guide writes data to S3. Take care to replace the S3 authentication details with the correct values for your S3 instance.

First, create some tables that will be used for the examples in this guide.

```python test-set=1 order=grades,math_grades,science_grades,history_grades docker-config=minio
from deephaven import new_table, merge
from deephaven.column import int_col, double_col, string_col

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

Write a Deephaven table to a single Parquet file with [`parquet.write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write). Supply the `table` argument with the Deephaven table to be written, and the `path` argument with the destination file path for the resulting Parquet file. This file path should end with the `.parquet` file extension.

```python test-set=1
from deephaven import parquet

parquet.write(table=grades, path="/data/grades/grades.parquet")
```

Write `_metadata` and `_common_metadata` files by setting the `generate_metadata_files` argument to `True`. Parquet metadata files are useful for reading very large datasets, as they enhance the performance of the read operation significantly. If the data might be read in the future, consider writing metadata files.

```python test-set=1
parquet.write(
    table=grades, path="/data/grades_meta/grades.parquet", generate_metadata_files=True
)
```

### To S3

Similarly, use [`parquet.write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write) to write Deephaven tables to Parquet files on S3. The `path` should be the URI of the destination file in S3. Supply an instance of the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class to the `special_instructions` argument to specify the details of the connection to the S3 instance.

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
        endpoint_override="http://minio.example.com:9000",
        credentials=credentials,
    ),
)
```

## Partitioned Parquet directories

Deephaven supports writing tables to partitioned Parquet directories. A partitioned Parquet directory organizes data into subdirectories based on one or more partitioning columns. This structure allows for more efficient data querying by pruning irrelevant partitions, leading to faster read times than a single Parquet file. Deephaven tables can be written to _flat_ partitioned directories or _key-value_ partitioned directories.

Data can be written to partitioned directories from Deephaven tables or from Deephaven's [partitioned tables](../../how-to-guides/partitioned-tables.md). Partitioned tables have partitioning columns built into the API, so Deephaven can use those partitioning columns to create partitioned directories. Regular Deephaven tables do not have partitioning columns, so the user must provide that information using the `table_definition` argument to any of the writing functions.

Table definitions represent a table's schema. They are constructed from lists of Deephaven [`Column`](/core/pydoc/code/deephaven.column.html#deephaven.column.Column) objects that specify a column's name and type using types from the [`deephaven.dtypes`](/core/pydoc/code/deephaven.dtypes.html) Python module. Additionally, [`Column`](/core/pydoc/code/deephaven.column.html#deephaven.column.Column) objects are used to specify whether a particular column is a partitioning column by setting the `column_type` argument to `ColumnType.PARTITIONING`.

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

## Write to a key-value partitioned Parquet directory

Key-value partitioned Parquet directories extend partitioning by organizing data based on key-value pairs in the directory structure. This allows for highly granular and flexible data access patterns, providing efficient querying for complex datasets. The downside is the added complexity in managing and maintaining the key-value pairs, which can be more intricate than other partitioning methods.

### To local storage

Use [`parquet.write_partitioned`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write_partitioned) to write Deephaven tables to key-value partitioned Parquet directories. Supply a Deephaven table or a [partitioned table](../../how-to-guides/partitioned-tables.md) to the `table` argument, and set the `destination_dir` argument to the destination root directory where the partitioned Parquet data will be stored. Non-existing directories in the provided path will be created.

```python test-set=1
from deephaven import parquet

# write a standard Deephaven table, must specify table_definition
parquet.write_partitioned(
    table=grades, destination_dir="/data/grades_kv/", table_definition=grades_def
)

# or write a partitioned table
parquet.write_partitioned(
    table=grades_partitioned, destination_dir="/data/grades_kv_partitioned/"
)
```

Set the `generate_metadata_files` argument to `True` to write metadata files.

```python test-set=1
parquet.write_partitioned(
    table=grades_partitioned,
    destination_dir="/data/grades_kv_partitioned_meta/",
    generate_metadata_files=True,
)
```

### To S3

[`parquet.write_partitioned`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write_partitioned) is used to write key-value partitioned Parquet directories to S3. The `path` should be the URI of the destination directory in S3. Supply an instance of the [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class to the `special_instructions` argument to specify the details of the connection to the S3 instance.

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
        endpoint_override="http://minio.example.com:9000",
        credentials=credentials,
    ),
)
```

## Write to a flat partitioned Parquet directory

A flat partitioned Parquet directory stores data without nested subdirectories. Each file contains partition information within its filename or as metadata. This approach simplifies directory management compared to hierarchical partitioning but can lead to larger directory listings, which might affect performance with many partitions.

### To local storage

Use [`parquet.write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write) or [`parquet.batch_write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.batch_write) to write Deephaven tables to Parquet files in flat partitioned directories. [`parquet.write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write) requires multiple calls to write multiple tables to the destination, while [`parquet.batch_write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.batch_write) can write multiple tables to multiple paths in a single call.

Supply [`parquet.write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write) with the Deephaven table to be written and the destination file path with the `table` and `path` arguments. The `path` must end with the `.parquet` file extension.

```python test-set=1
from deephaven import parquet

parquet.write(math_grades, "/data/grades_flat_1/math.parquet")
parquet.write(science_grades, "/data/grades_flat_1/science.parquet")
parquet.write(history_grades, "/data/grades_flat_1/history.parquet")
```

Use [`parquet.batch_write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.batch_write) to accomplish the same thing by passing multiple tables to the `tables` argument and multiple destination paths to the `paths` argument. This requires the `table_definition` argument to be specified.

```python test-set=1
parquet.batch_write(
    tables=[math_grades, science_grades, history_grades],
    paths=[
        "/data/grades_flat_2/math.parquet",
        "/data/grades_flat_2/science.parquet",
        "/data/grades_flat_2/history.parquet",
    ],
    table_definition=grades_def,
)
```

To write a [Deephaven partitioned table](../../how-to-guides/partitioned-tables.md) to a flat partitioned Parquet directory, the table must first be broken into a list of constituent tables, then [`parquet.batch_write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.batch_write) can be used to write all of the resulting constituent tables to Parquet. Again, the `table_definition` argument must be specified.

```python test-set=1
from deephaven.pandas import to_pandas

# get keys from table
grades_partitioned_keys = grades_partitioned.keys()

# make keys iterable through pandas
keys = to_pandas(grades_partitioned_keys)["Class"]

# create list of constituent tables from keys
grades_partitioned_list = []
for key in keys:
    grades_partitioned_list.append(grades_partitioned.get_constituent(key))

# write each constituent table to Parquet using batch_write
parquet.batch_write(
    tables=grades_partitioned_list,
    paths=[
        "/data/grades_flat_3/math.parquet",
        "/data/grades_flat_3/science.parquet",
        "/data/grades_flat_3/history.parquet",
    ],
    table_definition=grades_def,
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
        "s3://example-bucket/math.parquet",
        "s3://example-bucket/science.parquet",
        "s3://example-bucket/history.parquet",
    ],
    table_definition=grades_def,
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://minio.example.com:9000",
        credentials=credentials,
    ),
)
```

## Optional arguments

The [`write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write), [`write_partitioned`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write_partitioned), and [`batch_write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.batch_write) functions from the [Deephaven Parquet Python module](/core/pydoc/code/deephaven.parquet.html#module-deephaven.parquet) all accept additional optional arguments used to control the specifics of how data gets written from Deephaven to Parquet. Here are the additional arguments that all three of these functions accept:

- `table_definition`: The table definition or schema, provided as a dictionary of string-[`DType`](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.DType) pairs, or as a list of [`ColumnDefinition`](/core/pydoc/code/deephaven.column.html#deephaven.column.ColumnDefinition) instances. When not provided, the column definitions implied by the table(s) are used.
- `col_instructions`: Instructions for customizations while writing particular columns, provided as a [`ColumnInstruction`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ColumnInstruction) or a list of [`ColumnInstruction`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ColumnInstruction)s. The default is `None`, which means no specialization for any column.
- `compression_codec_name`: The name of the [compression codec](https://www.javadoc.io/doc/org.apache.parquet/parquet-hadoop/1.8.1/org/apache/parquet/hadoop/metadata/CompressionCodecName.html) to use. Defaults to `SNAPPY`.
- `max_dictionary_keys`: The maximum number of unique keys the writer should add to a dictionary page before switching to non-dictionary encoding. This is never evaluated for non-string columns. Defaults to 2^20 (1,048,576).
- `max_dictionary_size`: The maximum number of bytes the writer should add to the dictionary before switching to non-dictionary encoding. This is never evaluated for non-string columns. Defaults to 2^20 (1,048,576).
- `target_page_size`: The target page size in bytes. Defaults to 2^20 bytes (1 MiB).
- `generate_metadata_files`: Whether to generate Parquet `_metadata` and `_common_metadata` files. Defaults to `False`.
- `row_group_info`: Sets the Row Group type used for writing. Available Row Group types are:
  - `RowGroupInfo.single_group()`: All data is within a single Row Group. This is the default `RowGroupInfo` implementation.
  - `RowGroupInfo.max_rows(max_rows)`: Splits into a number of Row Groups, each of which has no more than the requested number of rows.
  - `RowGroupInfo.max_groups(num_row_groups)`: Split evenly into a pre-defined number of Row Groups, each of which contains the same number of rows. If the input table size is not evenly divisible by the number of Row Groups requested, then some Row Groups will contain one fewer row.
  - `RowGroupInfo.by_groups(groups, (Optional) max_rows)`: Splits each unique group into a Row Group. If the table does not have all values for the group(s) contiguously, then an error will be raised. If `max_rows` is set and a given Row Group yields a row count greater than the requested number of rows, then it will be split further using `max_rows(...)`.
- `index_columns`: Sequence of sequences containing the column names for indexes to persist. The write operation will store the index info for the provided columns as sidecar tables. For example, if the input is `[[“Col1”], [“Col1”, “Col2”]]`, the write operation will store the index info for `[“Col1”]` and for `[“Col1”, “Col2”]`. By default, data indexes to write are determined by those of the source table. This argument can be used to narrow the set of indexes to write or to be explicit about the expected set of indexes present on all sources. Indexes that are specified but missing will be computed on demand.

## Column instructions and special instructions

### Column instructions

The `col_instructions` argument to [`write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write), [`write_partitioned`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write_partitioned), and [`batch_write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.batch_write) must be an instance of the [`ColumnInstruction`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ColumnInstruction) class. This class maps specific columns in the Deephaven table to specific columns in the resulting Parquet files, as well as specifying the method of compression used for that column.

[`ColumnInstruction`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.ColumnInstruction) has the following arguments:

- `column_name`: The column name in the Deephaven table to apply these instructions.
- `parquet_column_name`: The name of the corresponding column in the Parquet dataset.
- `codec_name`: The name of the [compression codec](https://www.javadoc.io/doc/org.apache.parquet/parquet-hadoop/1.8.1/org/apache/parquet/hadoop/metadata/CompressionCodecName.html) to use.
- `codec_args`: An implementation-specific string used to map types to/from bytes. It is typically used in cases where there is no obvious language-agnostic representation in Parquet.
- `use_dictionary`: `True` or `False` indicating whether or not to use [dictionary-based encoding](https://en.wikipedia.org/wiki/Dictionary_coder) for string columns.

Of particular interest is the `codec_name` argument. This defines the particular type of compression used for the given column and can have significant implications for the speed of the export. The options are:

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

- [Import Parquet files](./parquet-import.md)
- [Parquet formats](./parquet-formats.md)
