# **Stream vs. Append in the Deephaven-Kafka Ingestor**

This notebook will demonstrate the value of Deephaven stream tables for aggregation use cases.
It will also familiarize you with some of the options related to Deephaven's Kafka integration.
\
\
When ingesting a stream, you can choose whether you want to create:

1. A table that adds new data at the end of the table (_`table_type` = **append**_); or
2. A table that simply receives the new events, passes them to downstream nodes, and then flushes them (_`table_type` = **stream**_).

That decision can impact memory usage. The code below illustrates the difference, while showing that either method can provide _**identical aggregation results**_.\
\
\
\
Start by importing some requisite packages. There is documentation on [installing Python packages](https://deephaven.io/core/docs/how-to-guides/install-python-packages/),
[aggBy](https://deephaven.io/core/docs/reference/table-operations/group-and-aggregate/aggBy/), [emptyTable](https://deephaven.io/core/docs/how-to-guides/empty-table/#related-documentation), and [merge](https://deephaven.io/core/docs/how-to-guides/merge-tables/#merge-tables).

```python
from deephaven import ConsumeKafka as ck, Aggregation as agg, as_list
from deephaven.TableTools import emptyTable, merge
```

\
\
\
The function below establishes a [Kafka importer](https://deephaven.io/core/docs/how-to-guides/kafka-stream/) that sources data from a crypto trade recorder we wrote using [the XChange library](https://github.com/knowm/XChange).
The feed started on September 10th, 2021. A month later it had ~ 110 million events. (People like trading crypto apparently.)

[Kafka's docs](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html) describe the parameters and concepts.
This demo will demonstrate the impact of choices related to `offsets` and `table_type`.

```python
def get_trades(*, offsets, table_type):
    return ck.consumeToTable(
        {  'bootstrap.servers' : 'demo-kafka.c.deephaven-oss.internal:9092',
          'schema.registry.url' : 'http://demo-kafka.c.deephaven-oss.internal:8081' },
        'io.deephaven.crypto.kafka.TradesTopic',
        key = ck.IGNORE,
        value = ck.avro('io.deephaven.crypto.kafka.TradesTopic-io.deephaven.crypto.Trade'),
        offsets=offsets,
        table_type=table_type)
```

\
\
\
In this demo, imagine you want to start your Kafka feed "1 million events ago" (instead of "now" or "at the beginning" -- i.e. 09/10/2021 in this case). To do so, you need to find the Kafka offset equivalent to "1 million events ago".

Create a Deephaven table that listens to current records (-- i.e. crypto trades happening now).

```python
latest_offset = get_trades(offsets=ck.ALL_PARTITIONS_SEEK_TO_END, table_type='stream')
```

\
\
\
You can do a simple [lastBy()](https://deephaven.io/core/docs/reference/table-operations/group-and-aggregate/lastBy/) to refine the view to only the single last record.

```python
latest_offset = latest_offset.view("KafkaOffset").lastBy()
```

\
\
\
Since you are targeting a Kafka offset of "1 mm events ago", create a static table.
[Snapshotting](https://deephaven.io/core/docs/how-to-guides/reduce-update-frequency/#create-a-static-snapshot) to an empty table does the trick.

```python
latest_offset = emptyTable(0).snapshot(latest_offset, True)
```

\
\
\
With the static table, you can now set a variable by pulling a record from the table using [view](https://deephaven.io/core/docs/how-to-guides/use-select-view-update/) and get().

```python
size_offset = 1_000_000
target_offset_table = latest_offset.view("Offset=max(0, KafkaOffset-size_offset)")
offset_variable = target_offset_table.getColumn("Offset").get(0)
```

\
\
\
For fun, print the value to the console.

```python
print(offset_variable)
```

\
\
\
The goal is to show that **_aggregations will be identical_** whether the Kafka-sourced table is an appending or streaming one.
Define a [table aggregation function](https://deephaven.io/core/docs/reference/table-operations/group-and-aggregate/aggBy/).

```python
def trades_agg(table):
    agg_list = as_list([
        agg.AggCount("Trade_Count"),
        agg.AggSum("Total_Size = Size"),
    ])
    return table.aggBy(agg_list, "Exchange", "Instrument").\
        sort("Exchange", "Instrument").\
        formatColumnWhere("Instrument", "Instrument.startsWith(`BTC`)", "IVORY")
```

\
\
\
Create a new appending table, starting at the `offset_variable` you defined above, as well as two other derived tables:

1. An aggregation table using the function you just made, and
2. A simple count of rows of the source table.

You will see that the count is growing. It should reach > 1mm records quickly.

```python
trades_append = get_trades(offsets={ 0: offset_variable }, table_type='append')
agg_append = trades_agg(trades_append)
row_count_append = trades_append.countBy("RowCount").updateView("Table_Type = `append`")
```

\
\
\
Touch the table tab called `agg_append` to see the Trade\_Count and Total\_Size by Exchange and Instrument (over the last 1 mm-ish trades).
\
\
\
For comparison, repeat the exercise, changing only the `table_type` parameter of the Kafka integration to be _**stream**_ (instead of _**append**_).

Note the `dropStream()` syntax.

```python
trades_stream = get_trades(offsets={ 0: offset_variable }, table_type='stream')
agg_stream = trades_agg(trades_stream)
row_count_stream = trades_stream.dropStream()\
.countBy("RowCount").updateView("Table_Type = `stream`")
```

\
\
\
The `row_count_stream` table will show large numbers as it first ingests the 1mm records,
then settle at noisy small numbers as crypto trades take place in the world, in real-time.

Go to the table `agg_stream` and it should look similar to what you just viewed in `agg_append`.
\
\
\
However, stream tables have the ingested records in memory only for a split second.

Merging the two row_count tables makes the difference obvious.

```python
row_count_merge = merge(row_count_append, row_count_stream)
```

\
\
\
You can confirm the two aggregation tables are identical by [joining them](https://deephaven.io/core/docs/reference/table-operations/join/natural-join/) and taking the difference.

```python
diff_table = agg_append.naturalJoin(agg_stream, "Exchange, Instrument",\
    "Trade_Count_Agg = Trade_Count, Total_Size_Agg = Total_Size")\
    .view("Exchange", "Instrument", "Diff_Trade_Count = Trade_Count - Trade_Count_Agg",
    "Diff_Total_Size = Total_Size - Total_Size_Agg")
```

\
\
\
You can manually close each table, and then still access it from the button at top-right called "**Panels**".

If you want to delete a table, you can:

```python
my_table = None
```

\
\
\
[Deephaven documentation](https://deephaven.io/core/docs/) has many more examples.

```python
print("Go to https://deephaven.io/core/docs/tutorials/quickstart/ to download pre-built Docker images.")
```
