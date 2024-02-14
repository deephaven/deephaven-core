
# Deephaven Python Integration Package

Deephaven Python Integration Package is created by Deephaven Data Labs. It allows Python developers, including data
scientists, to access data, run queries, and execute Python scripts directly inside Deephaven data servers to achieve
maximum performance. By taking advantage of the unique streaming table capability of Deephaven and its many data ingestion
facilities (Kafka, Parquet, CSV, SQL, etc.), Python developers can quickly put together a real-time data processing pipeline
that is high performing and easy to consume.

If you use a Windows operating system, WSL is **not** required to run Deephaven via pip.

## Install

Because this package depends on the Deephaven server, it comes preinstalled with Deephaven Docker images and is made 
available at runtime in the Python console in the Deephaven Web UI.

## Quick start

```python
from deephaven import read_csv
from deephaven.stream.kafka.consumer import kafka_consumer, TableType
from deephaven.plot import Figure, PlotStyle
csv_table = read_csv("data1.csv")
kafka_table = kafka_consumer.consume({'bootstrap.servers': 'redpanda:29092'}, topic='realtime_feed', table_type=TableType.Append)
joined_table = kafka_table.join(csv_table, on=["key_col_1", "key_col_2"], joins=["data_col1"])
plot = Figure() \
    .axes(plot_style = PlotStyle.STACKED_BAR )\
    .plot_cat(series_name="Categories1", t=joined_table, category="Key_col_1", y = "data_col1") \
    .show()
```

## Related documentation

* https://deephaven.io/

## API Reference

[Start here](https://deephaven.io/core/pydoc/)
