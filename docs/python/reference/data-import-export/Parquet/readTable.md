---
title: read
---

The `read` method will read a single Parquet file, metadata file, or directory with a recognized layout into an in-memory table.

## Syntax

```python syntax
read(
    path: str,
    col_instructions: str = None,
    is_legacy_parquet: bool = False,
    is_refreshing: bool = False,
    file_layout: Optional[ParquetFileLayout] = None,
    table_definition: Union[Dict[str, DType], List[Column], None] = None,
    special_instructions: Optional[s3.S3Instructions] = None
) -> Table
```

## Parameters

<ParamTable>
<Param name="path" type="str">

The file to load into a table. The file should exist and end with the `.parquet` extension.

</Param>
<Param name="col_instructions" type="list[ColumnInstruction]" optional>

One or more optional [`ColumnInstruction`](./ColumnInstruction.md) objects that provide instructions for how to read particular columns in the file.

</Param>
<Param name="is_legacy_parquet" type="bool" optional>

Whether or not the Parquet data is legacy.

</Param>
<Param name="is_refreshing" type="bool" optional>

Whether or not the Parquet data represents a refreshing source.

</Param>
<Param name="file_layout" type="Optional[ParquetFileLayout]" optional>

The Parquet file layout. The default is `None`, which infers the layout.

</Param>
<Param name="table_definition" type="Union[Dict[str, DType], List[Column], None]" optional>

The table definition. The default is `None`, which infers the table definition from the Parquet file(s). If set, `file_layout` must also be set.

</Param>
<Param name="special_instructions" type="Optional[s3.S3Instructions]" optional>

Special instructions for reading Parquet files. Mostly used when reading from nonlocal filesystems, such as AWS S3 buckets. Default is `None`.

</Param>
</ParamTable>

## Returns

A new in-memory table from a Parquet file, metadata file, or directory with a recognized layout.

## Examples

> [!NOTE]
> All but the final example in this document use data mounted in `/data` in Deephaven. For more information on the relation between this location in Deephaven and on your local file system, see [Docker data volumes](../../../conceptual/docker-data-volumes.md).

### Single Parquet file

In this example, `read` is used to load the file `/data/examples/Taxi/parquet/taxi.parquet` into a Deephaven table.

```python
from deephaven.parquet import read

source = read("/data/examples/Taxi/parquet/taxi.parquet")
```

### Compression codec

In this example, `read` is used to load the file `/data/output_GZIP.parquet`, with `GZIP` compression, into a Deephaven table.

> [!CAUTION]
> This file needs to exist for this example to work. To generate this file, see [`write`](./writeTable.md).

```python
from deephaven.parquet import read, write
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("X", ["A", "B", "B", "C", "B", "A", "B", "B", "C"]),
        int_col("Y", [2, 4, 2, 1, 2, 3, 4, 2, 3]),
        int_col("Z", [55, 76, 20, 4, 230, 50, 73, 137, 214]),
    ]
)

write(source, "/data/output_GZIP.parquet", compression_codec_name="GZIP")

source = read("/data/output_GZIP.parquet")
```

### Partitioned datasets

`_metadata` and/or `_common_metadata` files are occasionally present in partitioned datasets. These files can be used to load Parquet data sets more quickly. These files are specific to only certain frameworks and are not required to read the data into a Deephaven table.

- `_common_metadata`: File containing schema information needed to load the whole dataset faster.
- `_metadata`: File containing (1) complete relative pathnames to individual data files, and (2) column statistics, such as min, max, etc., for the individual data files.

> [!WARNING]
> For a directory of Parquet files, all sub-directories are also searched. Only files with a `.parquet` extension or `_common_metadata` and `_metadata` files should be located in these directories. All files ending with `.parquet` need the same schema.

> [!NOTE]
> The following examples use data in [Deephaven's example repository](https://github.com/deephaven/examples). Follow the instructions in [`Launch Deephaven from pre-built images`](../../../getting-started/docker-install.md) to download and manage the example data.

In this example, `read` is used to load the directory `/data/examples/Pems/parquet/pems` into a Deephaven table.

```python order=null
from deephaven.parquet import read

source = read("/data/examples/Pems/parquet/pems")
```

![The above `source` table](../../../assets/reference/data-import-export/readTable3.png)

### Refreshing tables

The following example set demonstrates how to read refreshing Parquet files into Deephaven.

First, we create a Parquet table with [`write`](./writeTable.md).

```python test-set=1 order=null
from deephaven import new_table
from deephaven.column import int_col, double_col, string_col
from deephaven import parquet
from deephaven.parquet import read, write
import os, shutil

# Create new tables
grades1 = new_table(
    [
        string_col("Name", ["Ashton", "Jeffrey", "Samantha", "Zachary"]),
        int_col("Test1", [92, 78, 87, 74]),
        int_col("Test2", [94, 88, 81, 70]),
        int_col("Average", [93, 83, 84, 72]),
        double_col("GPA", [3.9, 2.9, 3.0, 1.8]),
    ]
)
grades2 = new_table(
    [
        string_col("Name", ["Josh", "Martin", "Mariah", "Rick"]),
        int_col("Test1", [67, 92, 87, 54]),
        int_col("Test2", [97, 99, 92, 63]),
        int_col("Average", [82, 96, 93, 59]),
        double_col("GPA", [4.0, 3.2, 3.6, 2.7]),
    ]
)

# Write both tables to parquet files
write(grades1, "/data/grades/part1.parquet")
write(grades2, "/data/grades/part2.parquet")

## Read the tables. is_refreshing must be True, or the result will be static
refreshing_result = read(path="/data/grades/", is_refreshing=True)

static_result = read(path="/data/grades/", is_refreshing=False)
```

![The four above tables, displayed in a 2x2 grid in the Deephaven UI](../../../assets/reference/data-import-export/parquet-isrefreshing-1.png)

Next, we list our current partitions, and then create a new Deephaven table by using the `read` method.

```python test-set=1 order=:log
# List the files in /tmp/grades
print(sorted(os.listdir("/data/grades")))

# Create table from parquet files
grades = parquet.read("/data/grades")
```

![The above four tables, plus the new `grades` table](../../../assets/reference/data-import-export/parquet-isrefreshing-2.png)

Finally, we create a third partition that is a copy of the first.

```python test-set=1 order=refreshing_result,static_result
# Make a 3rd partition by copying the first
shutil.copyfile("/data/grades/part1.parquet", "/data/grades/part3.parquet")
```

![`refreshing_result` displayed above `static_result`](../../../assets/reference/data-import-export/parquet-isrefreshing-3.png)

### Read from a nonlocal filesystem

Deephaven currently supports reading Parquet files from your local filesystem and [S3 storage](https://aws.amazon.com/s3/). The following code block uses special instructions to read a public Parquet dataset from an S3 bucket.

```python docker-config=minio order=drivestats
from deephaven import parquet
from deephaven.experimental import s3
from datetime import timedelta

drivestats = parquet.read(
    "s3://drivestats-parquet/drivestats/year=2023/month=02/2023-02-1.parquet",
    special_instructions=s3.S3Instructions(
        region_name="us-west-004",
        credentials=s3.Credentials.anonymous(),
        endpoint_override="https://s3.us-west-004.backblazeb2.com",
        read_ahead_count=8,
        fragment_size=65536,
        read_timeout=timedelta(seconds=10),
    ),
)
```

When reading from [AWS S3](https://aws.amazon.com/s3/), you must _always_ specify instructions for doing so via [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions). The following input parameters can be used to construct these special instructions:

- `region_name`: This parameter defines the region name of the AWS S3 bucket where the Parquet data exists. If `region_name` is not set, it is picked by the AWS SDK from 'aws.region' system property, "AWS_REGION" environment variable, the `{user.home}/.aws/credentials or {user.home}/.aws/config` files, or from EC2 metadata service, if running in EC2. If no region name is derived from the above chain or the region name derived is incorrect for the bucket accessed, the correct region name will be derived internally, at the cost of one additional request.
- `max_concurrent_requests`: The maximum number of concurrent requests to make to S3. The default is 50.
- `read_ahead_count`: The number of fragments that are asynchronously read ahead of the current fragment as the current fragment is being read. The default is 1.
- `fragment_size`: The maximum size of each fragment to read in bytes. The default is 5 MB.
- `max_cache_size`: The maximum number of fragments to cache in memory while reading. The default is 32.
- `connection_timeout`: The amount of time to wait for a successful S3 connection before timing out. The default is 2 seconds.
- `read_timeout`: The amount of time it takes to time out while reading a fragment. The default is 2 seconds.
- `access_key_id`: The access key for reading files. If set, `secret_access_key` must also be set.
- `secret_access_key`: The secret access key for reading files.
- `anonymous_access`: A boolean indicating to use anonymous credentials. The default is `False`. If the [default credentials](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/s3/Credentials.html#defaultCredentials()) fail, anonymous credentials are used.
- `endpoint_override`: The endpoint to connect to. The default is `None`.

Additionally, the `S3.maxFragmentSize` [configuration property](../../../how-to-guides/configuration/docker-application.md) can be set upon server startup. It sets the buffer size when reading Parquet from S3. The default is 5 MB. The buffer size should be set based on the largest expected fragment.

## Related documentation

- [Import Parquet files](../../../how-to-guides/data-import-export/parquet-import.md)
- [Export Parquet files](../../../how-to-guides/data-import-export/parquet-export.md)
- [`write_table`](./writeTable.md)
- [Docker data volumes](../../../conceptual/docker-data-volumes.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#readTable(java.lang.String))
- [Pydoc](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.read)
