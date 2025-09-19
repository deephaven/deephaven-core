---
title: DynamicTableWriter
---

`DynamicTableWriter` creates a [`DynamicTableWriter`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.DynamicTableWriter) for writing data to a real-time, in-memory table.

## Syntax

```python syntax
DynamicTableWriter(col_defs: dict) -> DynamicTableWriter
```

## Parameters

<ParamTable>
<Param name="col_defs" type="dict">

The column definitions. Each column name should have a 1-to-1 correspondence with its data type.

</Param>
</ParamTable>

## Returns

A [`DynamicTableWriter`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.DynamicTableWriter).

## Methods

[`DynamicTableWriter`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.DynamicTableWriter) supports the following methods:

- [`close()`](/core/pydoc/code/deephaven.html#deephaven.DynamicTableWriter.close) - Closes the [`DynamicTableWriter`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.DynamicTableWriter).
- [`write_row(values...)`](/core/pydoc/code/deephaven.html#deephaven.DynamicTableWriter.write_row) - Writes a row of values to the table.

## Properties

- `.table` - The table that the [`DynamicTableWriter`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.DynamicTableWriter) will write to.

## Examples

In this example, [`DynamicTableWriter`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.DynamicTableWriter) is used to create a table with two columns:

- The first contains the row number.
- The second contains a string.

```python order=result
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

The example above writes data to `result` from the main thread. As a result, the Deephaven web interface will not display the `result` table until the script finishes execution.

The example below uses a dedicated thread to write data to the table. The Deephaven web interface immediately updates to display all `result` table changes.

```python order=result
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

import threading, time

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

- [How to use write data to a real-time in-memory table](../../../how-to-guides/table-publisher.md#dynamictablewriter)
- [Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.DynamicTableWriter)
