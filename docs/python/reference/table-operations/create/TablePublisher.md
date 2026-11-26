---
title: TablePublisher
---

The [`table_publisher`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.table_publisher) function creates a [`TablePublisher`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher). A `TablePublisher` produces a [blink table](../../../conceptual/table-types.md#specialization-3-blink) from the tables passed to its [`add`](#methods) method.

## Syntax

```python syntax
table_publisher(
    name: str,
    col_defs: TableDefinitionLike,
    on_flush_callback: Callable[[TablePublisher], None] | None = None,
    on_shutdown_callback: Callable[[], None] | None = None,
    update_graph: UpdateGraph | None = None,
    chunk_size: int = 2048
) -> tuple[Table, TablePublisher]
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the publisher, used for logging.

</Param>
<Param name="col_defs" type="TableDefinitionLike">

The blink table's column definitions. See [`TableDefinitionLike`](./TableDefinitionLike.md) for the accepted forms.

</Param>
<Param name="on_flush_callback" type="Callable" optional>

If present, the on-flush callback runs once at the beginning of each [update graph cycle](../../../conceptual/dag.md#update-graph-cycles). It receives this `TablePublisher`, so it can call [`add`](#methods) to publish any data it has been batching. The callback blocks the update cycle until it returns, so it should avoid extraneous work. The default is `None`.

</Param>
<Param name="on_shutdown_callback" type="Callable" optional>

An on-shutdown callback function. The publisher calls it one time when the caller should stop adding new data and release any related resources — for example, because the caller called [`publish_failure`](#methods), the blink table is no longer reachable, or an internal failure occurred. The default is `None`.

</Param>
<Param name="update_graph" type="UpdateGraph" optional>

The [update graph](../../../conceptual/dag.md) that the resulting table belongs to. The default is `None`, which uses the update graph of the current [execution context](../../../conceptual/execution-context.md).

</Param>
<Param name="chunk_size" type="int" optional>

The maximum number of rows that `add` copies from the added table in each batch, or chunk. Must be positive. The default is 2048 rows.

</Param>
</ParamTable>

## Returns

A tuple containing the resulting blink [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table) and the [`TablePublisher`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher) used to populate it.

## Methods

`TablePublisher` supports the following methods and properties:

- [`add(table)`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher.add) - Adds a snapshot of the data from `table` into the blink table. All of the data from one call is consistent with a single point in time, appears in a single update cycle of the blink table, and is not interleaved with data from any other call to `add`, concurrent or not.
  - `table` must contain every column in the blink table's definition, in any order.
  - `add` ignores columns in `table` that are not in the blink table's definition.
- [`is_alive`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher.is_alive) - A property that is `True` while the publisher is alive. Once it is `False`, it remains `False`, and the caller should stop adding data.
- [`publish_failure(failure)`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher.publish_failure) - Indicates that data publication has failed. The publisher notifies the blink table's listeners of the failure. Listeners receive a Java `RuntimeException` whose message is `str(failure)`, not the Python exception itself. The on-shutdown callback runs if it has not already run, `is_alive` becomes `False`, and later calls to `add` return without publishing. These effects can take place asynchronously, on a later update cycle.

## Examples

The following example creates a `TablePublisher` whose blink table has three columns. The `add_to_table` function uses [`new_table`](./newTable.md) to build a three-row table and passes it to [`my_publisher.add`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher.add).

The two calls to `add_to_table` below run in the same command. The script session holds the update graph's exclusive lock while a command runs, so both batches land in the same [update cycle](../../../conceptual/dag.md#update-graph-cycles) and all six rows appear together. For details, see [Data appears on the next update cycle](../../../how-to-guides/table-publisher.md#data-appears-on-the-next-update-cycle).

```python ticking-table test-set=1 order=null
from deephaven.stream.table_publisher import table_publisher
from deephaven import dtypes as dht
from deephaven import new_table
from deephaven.column import string_col, int_col, float_col

my_blink_table, my_publisher = table_publisher(
    "My table",
    {"Name": dht.string, "Age": dht.int32, "Height": dht.float32},
)


def add_to_table():
    my_publisher.add(
        new_table(
            [
                string_col("Name", ["Foo", "Bar", "Baz"]),
                int_col("Age", [42, 22, 32]),
                float_col("Height", [5.5, 6.0, 6.5]),
            ]
        )
    )


add_to_table()
add_to_table()
```

![The table `my_blink_table` created by the above query](../../../assets/reference/table-operations/my_blink_table.png)

Because `my_blink_table` is a [blink table](../../../conceptual/table-types.md#specialization-3-blink), it shows only the rows added during the most recent update cycle. On the next update cycle, those six rows are removed. Calling `add_to_table` again adds three new rows, which `my_blink_table` shows for one update cycle.

```python ticking-table test-set=1 order=null
add_to_table()
```

![The blink table updates to only show new rows created in the current update cycle](../../../assets/reference/table-operations/my_blink_table2.png)

## Related documentation

- [Write data to an in-memory, real-time table](../../../how-to-guides/table-publisher.md)
- [`DynamicTableWriter`](./DynamicTableWriter.md)
- [Execution Context](../../../conceptual/execution-context.md)
- [Blink tables](../../../conceptual/table-types.md#specialization-3-blink)
- [Directed acyclic graph (DAG)](../../../conceptual/dag.md)
- [`new_table`](./newTable.md)
- [`TableDefinitionLike`](./TableDefinitionLike.md)
- [Javadoc](/core/javadoc/io/deephaven/stream/TablePublisher.html)
- [Pydoc](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher)
