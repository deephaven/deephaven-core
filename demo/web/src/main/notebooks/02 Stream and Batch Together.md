# Unified Abstraction for Streams and Tables

\
Deephaven empowers you to work with updating and dynamic data - in real-time. However, frequently you will want to simultaneously manipulate or incorporate batch data into your applications and analytics.

This matters:  
You can use the same methods, operations, and API calls for static tables as you do for streaming updates. If you're interested, here is [an article](https://deephaven.io/core/docs/conceptual/table-update-model/) the "how",

Below you’ll do calculations and aggregations on stream and batch data using identical methods and functions. Then you'll merge and join the two, without any need to track which is which.
\
\
First, hook up a Kafka stream. (This is the same script from the first notebook.) Our [how-to guide](https://deephaven.io/core/docs/how-to-guides/kafka-stream/) provides detail on the integration.

```python
from deephaven import ConsumeKafka as ck

def get_trades_stream():
    return ck.consumeToTable(
        { 'bootstrap.servers' : 'demo-kafka.c.deephaven-oss.internal:9092',
          'schema.registry.url' : 'http://demo-kafka.c.deephaven-oss.internal:8081' },
        'io.deephaven.crypto.kafka.TradesTopic',
        key = ck.IGNORE,
        value = ck.avro('io.deephaven.crypto.kafka.TradesTopic-io.deephaven.crypto.Trade'),
        offsets=ck.ALL_PARTITIONS_SEEK_TO_END,
        table_type='append')

trades_stream = get_trades_stream()
```

\
\
\
You can select columns and reverse the table to make it nicer and more exciting to look at.

```python
trades_stream_view = trades_stream.view("KafkaTimestamp", "Instrument", "Exchange", "Price", "Size").reverse()
```

\
\
\
[Apache Parquet](https://parquet.apache.org/) is a popular columnar storage format.  Deephaven has [a rich Parquet integration](https://deephaven.io/core/docs/how-to-guides/parquet-partitioned/) that takes advantage of Parquet's drectory features and codecs to support sophisticated use cases.
\
\
The simple script below reads in a 10 billion row, one column table.  
Feel free to scroll around at 

```python
from deephaven import ParquetTools as pt
t_parquet = pt.readTable("/data/large/misc/10b-x.snappy.parquet").coalesce().restrictSortTo()
# Allowing users to sort 10 bb rows in the UI is not best practice.
```
\
\
\
You can see the row count by hovering on the column header or running this script.
```python
t_parquet_row_count = t_parquet.countBy("Row_Count")
```
\
\
\
Let's return to our crypto data.
Read in a CSV of batch crypto data sourced on 09/22/2021.

```python
from deephaven import read_csv
trades_batch_view = read_csv("/data/large/crypto/CryptoTrades_20210922.csv")
```

\
\
\
You can easily examine the table schemas, noting the columns are identically named and typed.

```python
schema_stream = trades_stream_view.getMeta()
schema_batch  = trades_batch_view.getMeta()
```

\
\
\
That was the first example of using the same table operation (or API call) on different tables -- one with streaming, updating data, the other with static.
\
\
The following scripts will demonstrate much the same with two examples:

1. A table decoration, specifically a new column with a date-time transformation. You could add math, logic, or other manipulations [this way](https://deephaven.io/core/docs/how-to-guides/use-select-view-update/).
2. A table aggregation via a Python function call.
   \
   In both cases, the same method is applied to the static and updating tables.

```python
# the table decoration
from deephaven.DateTimeUtils import formatDate

add_column_streaming = trades_stream_view.updateView("Date = formatDate(KafkaTimestamp, TZ_NY)")
add_column_batch     = trades_batch_view .updateView("Date = formatDate(Timestamp, TZ_NY)")

# the table aggregation
from deephaven import Aggregation as agg, as_list

agg_list = as_list([
    agg.AggFirst("Price"),
    agg.AggAvg("Avg_Price = Price"),
])

agg_streaming = add_column_streaming.aggBy(
    agg_list, "Date", "Exchange", "Instrument"
)

agg_batch = add_column_batch.aggBy(
    agg_list, "Date", "Exchange", "Instrument"
)
```

\
\
\
You can [merge tables](https://deephaven.io/core/docs/how-to-guides/merge-tables/#merge-tables) without worrying about whether the tables are real-time and updating or not.

The two tables simply need to have the same schema. You already inspected that above.
Below you'll merge the two raw tables (with each other), then the two aggregations.

```python
from deephaven.TableTools import merge
merge_trade_views = merge(add_column_streaming, add_column_streaming)

merge_aggs = merge(agg_streaming, agg_batch)\
  .formatColumnWhere("Date", "Date = currentDateNy()", "IVORY")
```

\
\
\
You’ll likely want to bring data together from different tables with joins, not just by merging tables.

You can use Deephaven for **time-series and relational** joins on static tables, real-time updating tables, and derived tables of either type. Here is information on [how to choose and use joins](https://deephaven.io/core/docs/how-to-guides/joins-overview/).
\
\
Please note that you do not have to think about whether a named_table happens to be updating (stream-like) or static (batch-like). The methods are common between the two.

```python
join_stream_batch = agg_streaming.renameColumns("Price_Streaming = Price", "Avg_Price_Streaming = Avg_Price")\
  .naturalJoin(agg_batch, "Exchange, Instrument", "Date_Batch = Date, Price_Batch = Price, Avg_Price_Batch = Avg_Price")\
  .formatColumns("Date = `IVORY`")\
  .formatColumns("Date_Batch = `SKYBLUE`")\
  .updateView("Avg_Price_Change = Avg_Price_Streaming - Avg_Price_Batch")
```

\
\
\
[The next notebook](03%20Kafka%20Stream%20vs%20Append.md) demonstrates the value of different Kafka integrations.

```python
print("When is streaming smarter than appending?  Find out next.")
```
