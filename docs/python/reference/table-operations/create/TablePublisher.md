---
title: TablePublisher
---

The `table_publisher` method creates a `TablePublisher`. A `TablePublisher` produces a [blink table](../../../conceptual/table-types.md#specialization-3-blink) from tables that are added to it via the [`add`](#methods) method.

## Syntax

```python syntax
table_publisher(
    name: str,
    col_defs: dict[str, DType],
    on_flush_callback: Callable[[TablePublisher], None] = None
    on_shutdown_callback: Callable[[], None] = None,
    update_graph: UpdateGraph = None,
    chunk_size: int = 2048
) -> TablePublisher
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the blink table.

</Param>
<Param name="col_defs" type="dict">

The blink table's column definitions.

</Param>
<Param name="on_flush_callback" type="Callable" optional>

The on-flush callback, if present, is called once at the beginning of each update graph cycle. It allows publishers to add any data they may have been batching. This blocks the update cycle from proceeding, so implements should take care not to do extraneous work. The default is `None`.

</Param>
<Param name="on_shutdown_callback" type="Callable" optional>

An on-shutdown callback method. It will be called once when the caller should stop adding new data and release any related resources. The default is `None`.

</Param>
<Param name="update_graph" type="UpdateGraph" optional>

The [update graph](../../../conceptual/dag.md) that the resulting table will belong to. The default is `None`, which uses the update graph of the current [execution context](../../../conceptual/execution-context.md).

</Param>
<Param name="chunk_size" type="int" optional>

The size at which chunks of data will be filled from the source table during an add. The default is 2048 bytes.

</Param>
</ParamTable>

## Returns

A [`TablePublisher`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher).

## Methods

`TablePublisher` supports the following methods:

- [`add(table)`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher.add) - Adds a snapshot of the data from `table` into the blink table. The table _must_ contain a superset of the columns from the blink table's definition. The columns can be in any order. Columns from `table` that are not in the blink table's definition are ignored.
- [`is_alive()`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher.is_alive) - Checks if the table is alive. Returns `True` if the table is alive, and `False` otherwise.
- [`publish_failure(failure)`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher.publish_failure) - Indicates that data publication has failed.

## Examples

The following example creates a `TablePublisher` with three columns. It adds a table with three rows using `add` and [`new_table`](./newTable.md). `add_to_table` calls `my_publisher.add`, which adds three new rows to `my_blink_table`. Because `my_blink_table` is a blink table, each subsequent call to `add_to_table` will reset the state of the table.

```python test-set=1 order=my_blink_table
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

Subsequent calls to `add_to_table` in later update cycles will show only the rows created in that update cycle.

```python test-set=1
add_to_table()
```

![The blink table updates to only show new rows created in the current update cycle](../../../assets/reference/table-operations/my_blink_table2.png)

## Related documentation

- [How to create and use TablePublisher](../../../how-to-guides/table-publisher.md)
- [DynamicTableWriter](./DynamicTableWriter.md)
- [Execution Context](../../../conceptual/execution-context.md)
- [Javadoc](/core/javadoc/io/deephaven/stream/TablePublisher.html)
- [Pydoc](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher)
