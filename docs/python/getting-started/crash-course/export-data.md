---
title: Import and Export Data
---

Data I/O is mission-critical for any real-time data analysis platform. Deephaven supports a wide variety of data sources and formats, including [CSV](../../reference/cheat-sheets/csv.md), [Parquet](../../reference/cheat-sheets/parquet.md), [Kafka](../../reference/cheat-sheets/kafka.md), and more. This chapter shows how to:

- Read and write CSV and Parquet data.
- Read Parquet files from S3.
- Consume and publish Kafka streams.
- Read and write Iceberg tables.
- Convert tables to HTML.
- Use pandas DataFrames as a bridge to other Python libraries.
- Use function-generated tables to pull in data from other sources.

> [!NOTE]
> The first CSV and Parquet examples read files from Deephaven's [example data](https://github.com/deephaven/examples). A container has those files at `/data/examples` only when you start it from a [deployment that includes example data](../../getting-started/docker-install.md#choose-a-deployment). The `docker run` command in [Get started](./get-started.md) does not include them. To run these examples, start one of those deployments, or download the files into the `data/examples` directory that command mounts, keeping the same paths.

## CSV

Deephaven can [read CSV](../../how-to-guides/data-import-export/csv-import.md) files that exist locally or remotely.

> [!TIP]
> To import a CSV without writing code, use **Upload Table from File** in the Console Options menu (⋮). See [Upload a table from a file](../../how-to-guides/user-interface/upload-table-from-file.md).

This example reads a local CSV file.

```python test-set=1 order=iris
from deephaven import read_csv

iris = read_csv("/data/examples/Iris/csv/iris.csv")
```

Deephaven can also [write data to CSV](../../how-to-guides/data-import-export/csv-export.md). The code below writes that same table back to a CSV file.

```python test-set=1 order=null
from deephaven import write_csv

write_csv(iris, "/data/iris_new.csv")
```

Just to show that it's there, the code below reads the new file back.

```python test-set=1 order=iris_new
iris_new = read_csv("/data/iris_new.csv")
```

## Parquet

[Apache Parquet](https://parquet.apache.org/) is a columnar storage format that supports compression to store more data in less space. Deephaven reads and writes single Parquet files as well as [partitioned Parquet directories](../../how-to-guides/data-import-export/parquet-import.md#partitioned-parquet-directories), where one table's data is split across many Parquet files in a directory. Deephaven reads Parquet data stored locally or in cloud object storage, such as Amazon S3, any S3-compatible object store, and Google Cloud Storage.

The example below reads from a local Parquet file.

```python test-set=2 order=crypto_trades
from deephaven import parquet as dhpq

crypto_trades = dhpq.read(
    "/data/examples/CryptoCurrencyHistory/Parquet/CryptoTrades_20210922.parquet"
)
```

The code below writes that same table back to a Parquet file.

```python test-set=2 order=null
dhpq.write(crypto_trades, "/data/crypto_trades_new.parquet")
```

Just to show that it worked, the code below reads the new file back.

```python test-set=2 order=crypto_trades_new
crypto_trades_new = dhpq.read("/data/crypto_trades_new.parquet")
```

The example below reads a Parquet file from a [RustFS](https://rustfs.com/) server at `rustfs.example.com`. RustFS is an S3-compatible object store. The [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) class from the [`deephaven.experimental.s3`](/core/pydoc/code/deephaven.experimental.s3.html) module specifies how to connect to the S3 instance. To run the example, replace the endpoint, file path, and credentials with those of your own S3 instance. See [Read Parquet from S3](../../how-to-guides/data-import-export/parquet-import.md#from-s3) for more.

```python test-set=7 docker-config=rustfs order=grades
from deephaven import parquet as dhpq
from deephaven.experimental import s3

# pass the S3 URL, as well as instructions on how to talk to the S3 instance
grades = dhpq.read(
    path="s3://example-bucket/grades/grades.parquet",
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        endpoint_override="http://rustfs.example.com:9000",
        credentials=s3.Credentials.basic(
            access_key_id="example_username",
            secret_access_key="example_password",
        ),
    ),
)
```

## Kafka

[Apache Kafka](https://kafka.apache.org/) is a distributed event streaming platform for publishing and subscribing to streams of records. Deephaven can consume data from Kafka streams and publish data to them. See the [Kafka guide](../../how-to-guides/data-import-export/kafka-stream.md) for more.

The examples below connect to a Kafka broker at `redpanda:9092`. Redpanda is a Kafka-compatible broker. To run the examples, start Deephaven alongside Redpanda as described in [Launching Kafka with Deephaven](../../how-to-guides/data-import-export/kafka-stream.md#launching-kafka-with-deephaven). Run the `docker compose` commands below from the directory that holds the `docker-compose.yml` file from that guide.

The code below consumes a stream.

```python test-set=3 docker-config=kafka ticking-table order=null
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

The `result_append` table starts empty. Rows appear as records arrive on `test.topic`. To send some, run `docker compose exec redpanda rpk topic produce test.topic` in a terminal and type a few lines. `rpk` is Redpanda's command-line client.

Similarly, this code publishes the data in a Deephaven table to a Kafka stream.

```python test-set=4 docker-config=kafka ticking-table order=null
from deephaven.stream.kafka import producer as kp
from deephaven import time_table

# create ticking table to publish to Kafka stream
source = time_table("PT1s").update("X = `Value ` + i")

# publish to time-topic
write_topic = kp.produce(
    source,
    {"bootstrap.servers": "redpanda:9092"},
    "time-topic",
    key_spec=kp.KeyValueSpec.IGNORE,
    value_spec=kp.simple_spec("X"),
)
```

To see the records arrive, run `docker compose exec redpanda rpk topic consume time-topic` in a terminal.

## Iceberg

[Apache Iceberg](https://iceberg.apache.org/) is a high-performance, open table format for huge analytic datasets. Deephaven's [`deephaven.experimental.iceberg`](/core/pydoc/code/deephaven.experimental.iceberg.html#module-deephaven.experimental.iceberg) module reads and writes Iceberg tables. For more on Deephaven's Iceberg integration, see the [Iceberg user guide](../../how-to-guides/data-import-export/iceberg.md).

Both Iceberg examples need the custom Docker deployment described in [A Deephaven deployment for Iceberg](../../how-to-guides/data-import-export/iceberg.md#a-deephaven-deployment-for-iceberg). That deployment runs its object store at `http://minio:9000`, so before you run the first example, change the `"s3.endpoint"` value in the adapter call from `http://rustfs:9000` to `http://minio:9000`.

An Iceberg catalog adapter connects Deephaven to an Iceberg catalog. The following example creates one with [`iceberg.adapter`](../../reference/iceberg/adapter.md) and uses it to read an existing Iceberg table into a Deephaven table.

```python test-set=5 docker-config=iceberg order=deephaven_table
from deephaven.experimental import iceberg

# Configure the Iceberg catalog adapter for a REST catalog.
iceberg_catalog_adapter = iceberg.adapter(
    name="rustfs-iceberg",
    properties={
        "type": "rest",
        "uri": "http://rest:8181",
        "warehouse": "s3a://warehouse/wh",
        "client.region": "us-east-1",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.endpoint": "http://rustfs:9000",
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    },
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

Using the same `iceberg_catalog_adapter`, this code creates a new Iceberg table and writes a Deephaven table to it. [`create_table`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergCatalogAdapter.create_table) fails if the table already exists. To write to an existing table, use [`load_table`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergCatalogAdapter.load_table) instead.

```python test-set=5 order=null
from deephaven import new_table
from deephaven.column import int_col, string_col

# Create a sample Deephaven table.
my_deephaven_table = new_table(
    [
        int_col("ID", [1, 2, 3]),
        string_col("Category", ["A", "B", "A"]),
        int_col("Value", [100, 200, 300]),
    ]
)

# Create a new Iceberg table.
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

The [`to_html`](../../reference/data-import-export/HTML/to-html.md) function from the [`deephaven.html`](/core/pydoc/code/deephaven.html.html) module converts a Deephaven table into an HTML string. This is useful for displaying small tables in web pages or for creating simple HTML reports. `to_html` builds the whole string in memory, so avoid converting large tables.

```python order=:log,source_table
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
print(html_string)
with open("/data/my_table.html", "w") as f:
    f.write(html_string)
```

## pandas DataFrames

The [Python integrations](./py-integrations.md#pandas) chapter showed how [`to_pandas`](../../reference/pandas/to-pandas.md) and [`to_table`](../../reference/pandas/to-table.md) from the [`deephaven.pandas`](/core/pydoc/code/deephaven.pandas.html) module convert between Deephaven tables and [pandas DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html). For I/O, that makes pandas a bridge. Convert a table with `to_pandas` to leverage pandas' extensive data manipulation and analysis capabilities, or to hand it to any other Python library that works on DataFrames. Convert a DataFrame with `to_table` to bring a library's results back into Deephaven.

> [!NOTE]
> Converting a table to a pandas DataFrame copies the entire table into memory. For large tables, [filter](../../how-to-guides/use-filters.md) or [aggregate](../../how-to-guides/combined-aggregations.md) the data in Deephaven first.

## Function-generated tables

[Function-generated tables](../../how-to-guides/function-generated-tables.md) are a powerful tool for ingesting data from external sources such as WebSockets, databases, and much more. A Python function populates a function-generated table. Deephaven reruns the function at a regular interval, or whenever one of the tables you pass in the `source_tables` argument of [`function_generated_table`](../../reference/table-operations/create/function_generated_table.md) changes. The table does no data I/O on its own, but your function can read from an external source each time it runs.

The following example regenerates data in a table once per second. To keep the example self-contained, the function generates random data instead of reading from an external source.

```python test-set=6 ticking-table order=null
from deephaven import empty_table, function_generated_table


def regenerate():
    return empty_table(10).update(
        [
            "Group = randomInt(1, 4)",
            "GroupMean = Group == 1 ? -10.0 : Group == 2 ? 0.0 : 10.0",
            "GroupStd = Group == 1 ? 2.5 : Group == 2 ? 0.5 : 1.0",
            "X = randomGaussian(GroupMean, GroupStd)",
        ]
    )


fgt = function_generated_table(table_generator=regenerate, refresh_interval_ms=1000)
```

For an example that polls a web API for weather data, see the [function-generated tables blog post](/blog/2023/10/06/function-generated-tables#weather-api-example).
