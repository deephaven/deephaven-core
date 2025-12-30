---
title: Import and Export Data
sidebar_label: Data I/O
---

Data I/O is mission-critical for any real-time data analysis platform. Deephaven supports a wide variety of data sources and formats, including [CSV](../../reference/cheat-sheets/csv.md), [Parquet](../../reference/cheat-sheets/parquet.md), [Kafka](../../reference/cheat-sheets/kafka.md), and more. This document covers those formats in Deephaven.

## CSV

Deephaven can [read CSV](../../how-to-guides/data-import-export/csv-import.md) files that exist locally or remotely. This example reads a local CSV file.

```python test-set=1 order=iris
from deephaven import read_csv

iris = read_csv("/data/examples/Iris/csv/iris.csv")
```

It can also [write data to CSV](../../how-to-guides/data-import-export/csv-export.md). The code below writes that same table back to a CSV file.

```python test-set=1 order=null
from deephaven import write_csv

write_csv(iris, "/data/iris_new.csv")
```

Just to show that it's there:

```python test-set=1 order=iris_new
iris_new = read_csv("/data/iris_new.csv")
```

## Parquet

[Apache Parquet](../../reference/cheat-sheets/parquet.md) is a columnar storage format that supports compression to store more data in less space. Deephaven supports reading and writing single, nested, and partitioned Parquet files. Parquet data can be stored locally or in [S3](/core/pydoc/code/deephaven.experimental.s3.html#module-deephaven.experimental.s3).

The example below reads from a local Parquet file.

```python test-set=2 order=crypto_trades
from deephaven import parquet as dhpq

crypto_trades = dhpq.read(
    "/data/examples/CryptoCurrencyHistory/Parquet/CryptoTrades_20210922.parquet"
)
```

That same table can be written back to a Parquet file:

```python test-set=2 order=null
dhpq.write(crypto_trades, "/data/crypto_trades_new.parquet")
```

Just to show that it worked:

```python test-set=2 order=crypto_trades_new
crypto_trades_new = dhpq.read("/data/crypto_trades_new.parquet")
```

The example below reads a Parquet file from S3. This example uses [MinIO](https://min.io/) as a local S3-compatible object store. The `s3` module in Deephaven provides a way to specify how to connect to the S3 instance.

```python test-set=2 docker-config=minio order=grades
from deephaven import parquet
from deephaven.experimental import s3

# pass the S3 URL, as well as instructions on how to talk to the S3 instance
grades = parquet.read(
    path="s3://example-bucket/grades/grades.parquet",
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://minio.example.com:9000",
        access_key_id="example_username",
        secret_access_key="example_password",
    ),
)
```

## Kafka

[Apache Kafka](../../how-to-guides/data-import-export/kafka-stream.md) is a distributed event streaming platform that can be used to publish and subscribe to streams of records. Deephaven can consume and publish to Kafka streams. The code below consumes a stream.

```python test-set=3 docker-config=kafka order=result_append
from deephaven.stream.kafka import consumer as kc
from deephaven import dtypes as dht

result_append = kc.consume(
    {"bootstrap.servers": "redpanda:9092"},
    "test.topic",
    table_type=kc.TableType.append(),
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.simple_spec("Command", dht.string),
)
```

Similarly, this code publishes the data in a Deephaven table to a Kafka stream.

```python test-set=4 docker-config=kafka order=source
from deephaven.stream.kafka import producer as kp
from deephaven import time_table

# create ticking table to publish to Kafka stream
source = time_table("PT1s").update("X = i")

# publish to time-topic
write_topic = kp.produce(
    source,
    {"bootstrap.servers": "redpanda:9092"},
    "time-topic",
    key_spec=kp.KeyValueSpec.IGNORE,
    value_spec=kp.simple_spec("X"),
)
```

## Iceberg

[Apache Iceberg](https://iceberg.apache.org/) is a high-performance, open table format for huge analytic datasets. Deephaven's `deephaven.experimental.iceberg` module allows you to interact with Iceberg tables. For more on Deephaven's Iceberg integration, see the [Iceberg user guide](../../how-to-guides/data-import-export/iceberg.md).

The following example reads data from an existing Iceberg table into a Deephaven table. It uses a custom Docker deployment found [here](../../how-to-guides/data-import-export/iceberg.md#a-deephaven-deployment-for-iceberg).

```python test-set=5 docker-config=iceberg order=deephaven_table
from deephaven.experimental import iceberg

# Configure the Iceberg catalog adapter for a REST catalog.
iceberg_catalog_adapter = iceberg.adapter_s3_rest(
    name="minio-iceberg",
    catalog_uri="http://rest:8181",
    warehouse_location="s3a://warehouse/wh",
    region_name="us-east-1",
    access_key_id="admin",
    secret_access_key="password",
    end_point_override="http://minio:9000",
)

# Load the Iceberg table adapter, assuming 'nyc.taxis' exists.
my_iceberg_table_adapter = iceberg_catalog_adapter.load_table(
    table_identifier="nyc.taxis"
)

# Read the static Iceberg table into Deephaven.
deephaven_table = my_iceberg_table_adapter.table(
    update_mode=iceberg.IcebergUpdateMode.static()
)

# Now 'deephaven_table' can be used like any other Deephaven table.
```

Similarly, this code writes a Deephaven table to an Iceberg table. If the target table does not exist, it will be created.

```python docker-config=iceberg order=null
from deephaven.experimental import iceberg
from deephaven import new_table
from deephaven.column import int_col, string_col

# Configure the Iceberg catalog adapter.
iceberg_catalog_adapter = iceberg.adapter_s3_rest(
    name="minio-iceberg",
    catalog_uri="http://rest:8181",
    warehouse_location="s3a://warehouse/wh",
    region_name="us-east-1",
    access_key_id="admin",
    secret_access_key="password",
    end_point_override="http://minio:9000",
)

# Create a sample Deephaven table.
my_deephaven_table = new_table(
    [
        int_col("ID", [1, 2, 3]),
        string_col("Category", ["A", "B", "A"]),
        int_col("Value", [100, 200, 300]),
    ]
)

# Create or load an Iceberg table adapter.
# If 'crash_course_db.output_table' doesn't exist, it will be created.
iceberg_target_adapter = iceberg_catalog_adapter.create_table(
    table_identifier="crash_course_db.output_table",
    table_definition=my_deephaven_table.definition,
)

# Define writer options.
writer_options = iceberg.TableParquetWriterOptions(
    table_definition=my_deephaven_table.definition
)

# Get a table writer and append data.
iceberg_writer = iceberg_target_adapter.table_writer(writer_options=writer_options)
iceberg_writer.append(iceberg.IcebergWriteInstructions([my_deephaven_table]))
```

## HTML

Deephaven tables can be converted into an HTML representation using the `to_html` function from the `deephaven.html` module. This is useful for displaying tables in web pages or for creating simple HTML reports.

```python
from deephaven import new_table
from deephaven.column import string_col
from deephaven.html import to_html

source_table = new_table(
    [
        string_col("Name", ["Alice", "Bob", "Charlie"]),
        string_col("Value", ["X", "Y", "Z"]),
    ]
)

html_string = to_html(source_table)

# The html_string now contains the HTML representation of the table.
# You can print it or write it to a file:
# print(html_string)
# with open("/data/my_table.html", "w") as f:
#     f.write(html_string)
```

## Pandas DataFrames

Deephaven provides a seamless way to convert tables to [Pandas DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) and vice-versa using the `deephaven.pandas` module. This is particularly useful when you want to leverage Pandas' extensive data manipulation and analysis capabilities or integrate with other Python libraries that operate on DataFrames.

To convert a Deephaven table to a Pandas DataFrame, use the `to_pandas()` function.

```python order=iris,iris_df
from deephaven import read_csv
from deephaven.pandas import to_pandas

# Assuming 'iris' table from the CSV example above
iris = read_csv("/data/examples/Iris/csv/iris.csv")

iris_df = to_pandas(iris)

# iris_df is now a Pandas DataFrame
# print(iris_df.head())
```

**Note:** Converting an entire large table to a Pandas DataFrame will load all data into memory. For very large tables, consider filtering or aggregating the data within Deephaven first before converting to a DataFrame to avoid potential memory issues.

You can also convert a Pandas DataFrame back to a Deephaven table using `to_table()` from the same module.

```python order=sample_df,deephaven_table_from_df
import pandas as pd
from deephaven.pandas import to_table

# Create a sample Pandas DataFrame
data = {"col1": [1, 2], "col2": ["a", "b"]}
sample_df = pd.DataFrame(data)

# Convert the Pandas DataFrame to a Deephaven table
deephaven_table_from_df = to_table(sample_df)

# deephaven_table_from_df is now a Deephaven table
# You can view its contents or perform Deephaven operations on it
# deephaven_table_from_df.show()
```

## Function generated tables

[Function generated tables](../../how-to-guides/function-generated-tables.md) are tables populated by a Python function. The function is reevaluated when source tables change or at a regular interval. The following example re-generates data in a table once per second.

```python test-set=5 order=fgt
from deephaven import empty_table, function_generated_table


def regenerate():
    return empty_table(10).update(
        [
            "Group = randomInt(1, 4)",
            "GroupMean = Group == 1 ? -10.0 : Group == 2 ? 0.0 : Group == 3 ? 10.0 : NULL_DOUBLE",
            "GroupStd = Group == 1 ? 2.5 : Group == 2 ? 0.5 : Group == 3 ? 1.0 : NULL_DOUBLE",
            "X = randomGaussian(GroupMean, GroupStd)",
        ]
    )


fgt = function_generated_table(table_generator=regenerate, refresh_interval_ms=1000)
```

[Function generated tables](../../how-to-guides/function-generated-tables.md), on their own, don't do any data I/O. However, Python functions evaluated at a regular interval to create a ticking table are a powerful tool for data ingestion from external sources like WebSockets, databases, and much more. Check out this [blog post](/blog/2023/10/06/function-generated-tables#what-is-a-function-generated-table) that uses WebSockets to stream data into Deephaven with function generated tables.
