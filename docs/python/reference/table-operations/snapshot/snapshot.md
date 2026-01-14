---
title: snapshot
slug: ./snapshot
---

`snapshot` produces an static, in-memory copy of a source table.

## Syntax

```
result = source.snapshot() -> Table
```

## Returns

A static copy of a source table.

## Examples

In the following example, the `source` table updates every second with new data. After some time, a snapshot is taken.

```python skip-test
from deephaven import time_table

source = time_table("PT1S")

# Some time later...
result = source.snapshot()
```

![A user navigates between the ticking `source` table and the `result` snapshot](../../../assets/reference/table-operations/snapshot.gif)

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [How to capture the history of ticking tables](../../../how-to-guides/capture-table-history.md)
- [How to reduce the update frequency of ticking tables](../../../how-to-guides/performance/reduce-update-frequency.md)
- [`time_table`](../create/timeTable.md)
- [`update`](../select/update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html#snapshot())
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.snapshot)
