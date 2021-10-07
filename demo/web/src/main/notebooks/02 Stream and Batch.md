### Stream and table are one thing!

Deephaven empowers you to work with updating and dynamic data -- in real-time. However, frequently you will want to simultaneously manipulate or incorporate batch, static, or historical data.

This matters:  
You can use the same methods, operations, and API calls for batch, static tables as you do for real-time, streaming updates. If interested, see article on [the "how"](https://deephaven.io/core/docs/conceptual/table-update-model/).

Below you’ll do calculations and aggregations on stream and batch data using identical methods and functions. Then you'll merge and join batch and streaming data, without any need to track which is which.

First hook up a Kafka stream. (This is the same script from Notebook 1.) Here is our guide, [How to Connect to Kafka](https://deephaven.io/core/docs/how-to-guides/kafka-stream/).

```python
from deephaven import KafkaTools as kt

def get_trades_stream():
    return kt.consumeToTable(
        { 'bootstrap.servers' : 'demo-kafka.c.deephaven-oss.internal:9092',
          'schema.registry.url' : 'http://demo-kafka.c.deephaven-oss.internal:8081' },
        'io.deephaven.crypto.kafka.TradesTopic',
        key = kt.IGNORE,
        value = kt.avro('io.deephaven.crypto.kafka.TradesTopic-io.deephaven.crypto.Trade'),
        offsets=kt.ALL_PARTITIONS_DONT_SEEK,
        table_type='append')

trades_stream = get_trades_stream()
```

You can select columns and reverse the table to make it nicer and more exciting to look at.

```python
trades_stream_view = trades_stream.view("KafkaTimestamp", "Instrument", "Exchange", "Price", "Size").reverse()
```

Now read in a CSV of batch data sourced on 09/22/2021.

```python
# TODO: this is a placeholder for real csv() upload.)
# from deephaven import readCsv
trades_batch_view = trades_stream_view.tail(1000)
```

You can easily examine the table schemas, noting the columns are identically named and typed.

```python
schema_stream = trades_stream_view.getMeta()
schema_batch  = trades_batch_view.getMeta()
```

That was the first example of using the same table operation (or API call) on different tables -- one with streaming, updating data, the other with static.

The following scripts will demonstrate much the same with two examples:
Decorating each of the two tables with a date-time transformation. You could add math, logic, or other manipulations [this way](https://deephaven.io/core/docs/how-to-guides/use-select-view-update/).
Doing aggregations by defining a Python function on a table object, then calling that function using each of the two tables -- updating and batch.

Again the methods are identical.

```python
# methods and operations are identical between the updating table and the static one
from deephaven.DBTimeUtils import formatDate
add_column_streaming = trades_stream_view.updateView("Date = formatDate(KafkaTimestamp, TZ_NY)")
add_column_batch     = trades_batch_view .updateView("Date = formatDate(KafkaTimestamp, TZ_NY)")

# another example:  do aggs on streams and batch
from deephaven import ComboAggregateFactory as caf
agg_streaming = add_column_streaming.by(caf.AggCombo(caf.AggFirst("Price"), caf.AggAvg("Avg_Price = Price")), "Date", "Exchange", "Instrument")
agg_batch     = add_column_batch    .by(caf.AggCombo(caf.AggFirst("Price"), caf.AggAvg("Avg_Price = Price")), "Date", "Exchange", "Instrument")
```

You can easily [merge tables](https://deephaven.io/core/docs/how-to-guides/merge-tables/#merge-tables) without worrying about whether the tables are real-time and updating or not.

The two tables simply need to have the same schema. You already inspected that above.
The scripts below, respectively, merge the raw tables together, then the two aggregated views.

```python
from deephaven.TableTools import merge
merge_trade_views = merge(add_column_streaming, add_column_streaming)

merge_aggs = merge(agg_streaming, agg_batch)\
  .formatColumnWhere("Date", "Date = currentDateNy()", "IVORY")
```

You’ll likely want to bring data together from different tables with joins, not just by merging tables.

You can use Deephaven for **time-series and relational** joins on static tables, real-time updating tables, and derived tables that inherit both characteristics.

Here is more information on [how to choose and use joins](https://deephaven.io/core/docs/how-to-guides/joins-overview/)

Please note that you do not have to think about whether a named_table happens to be updating (stream-like) or static (batch-like). The methods are common between the two.

```python
join_stream_batch = agg_streaming.renameColumns("Price_Streaming = Price", "Avg_Price_Streaming = Avg_Price")\
  .naturalJoin(agg_batch, "Exchange, Instrument", "Date_Batch = Date, Price_Batch = Price, Avg_Price_Batch = Avg_Price")\
  .formatColumns("Date = `IVORY`")\
  .formatColumns("Date_Batch = `SKYBLUE`")\
  .updateView("Avg_Price_Change = Avg_Price_Streaming - Avg_Price_Batch")
```
