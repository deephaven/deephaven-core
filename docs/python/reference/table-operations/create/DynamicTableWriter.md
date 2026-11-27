---
title: DynamicTableWriter
---

[`DynamicTableWriter`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.DynamicTableWriter) creates a writer and a new real-time, in-memory table that the writer appends rows to.

> [!NOTE]
> For most cases, Deephaven recommends [`table_publisher`](./TablePublisher.md) for writing data to a live table. It has a more flexible and more performant API and produces a [blink table](../../../conceptual/table-types.md#specialization-3-blink). `DynamicTableWriter` produces an [append-only table](../../../conceptual/table-types.md#specialization-1-append-only) and suits simple cases that add a few rows at a time.

## Syntax

```python syntax
DynamicTableWriter(col_defs: dict[str, DType]) -> DynamicTableWriter
```

## Parameters

<ParamTable>
<Param name="col_defs" type="dict[str, DType]">

A dictionary that maps each column name to its Deephaven data type (for example, `deephaven.dtypes.int32`).

</Param>
</ParamTable>

## Returns

A `DynamicTableWriter`.

## Methods

`DynamicTableWriter` supports the following methods:

- [`close`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.DynamicTableWriter.close) - Closes the `DynamicTableWriter`.
- [`write_row(*values)`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.DynamicTableWriter.write_row) - Writes a row of values to the table. Each value must be convertible to its column's type. Numeric values are converted even when the conversion loses precision or overflows.

`DynamicTableWriter` implements the context manager protocol, so a `with` statement can manage it and call `close` on exit.

A `DynamicTableWriter` is not thread-safe. Call `write_row` from one thread at a time, or synchronize the calls yourself.

## Properties

- `table` - The table that the `DynamicTableWriter` writes to.

## Examples

The following two examples create the same table, with an `int32` column `Numbers` and a `string` column `Words`. They differ only in which thread writes the rows.

```python ticking-table order=null reset
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

import time

column_definitions = {"Numbers": dht.int32, "Words": dht.string}

table_writer = DynamicTableWriter(column_definitions)

result = table_writer.table

# The write_row method adds a row to the table
table_writer.write_row(1, "Testing")
time.sleep(3)
table_writer.write_row(2, "Dynamic")
time.sleep(3)
table_writer.write_row(3, "Table")
time.sleep(3)
table_writer.write_row(4, "Writer")
```

![The above `result` table](../../../assets/reference/create/DynamicTableWriter_ref1.png)

The example above writes data to `result` from the thread that runs the script. The script session holds the exclusive [update graph lock](../../../conceptual/query-engine/engine-locking.md#query-engine-locks) while the script runs, so no rows appear in `result` until the script finishes. Then all four rows appear at once. For details, see [Data appears on the next update cycle](../../../how-to-guides/table-publisher.md#data-appears-on-the-next-update-cycle).

The example below writes data to the table from a separate thread. The script finishes right away, and each row appears in `result` the next time the table refreshes during an [update graph cycle](../../../conceptual/table-update-model.md), usually the next cycle after the thread writes it.

```python ticking-table order=null reset
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

import threading
import time

column_definitions = {"Numbers": dht.int32, "Words": dht.string}

table_writer = DynamicTableWriter(column_definitions)

result = table_writer.table


# Define a function to write data to a table
def thread_func():
    strings = ["Testing", "Dynamic", "Table", "Writer"]
    for i in range(4):
        # The write_row method adds a row to the table
        table_writer.write_row(i + 1, strings[i])
        time.sleep(3)


# Run the thread that writes to the table
thread = threading.Thread(target=thread_func)
thread.start()
```

<LoopedVideo src='../../../assets/reference/create/DynamicTableWriter_ref2.mp4' />

## Related documentation

- [How to write data to a real-time in-memory table](../../../how-to-guides/table-publisher.md#dynamictablewriter)
- [Incremental update model](../../../conceptual/table-update-model.md)
- [Multithreading: Synchronization, locks, and snapshots](../../../conceptual/query-engine/engine-locking.md)
- [`table_publisher`](./TablePublisher.md)
- [Table types](../../../conceptual/table-types.md)
- [Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.DynamicTableWriter)
