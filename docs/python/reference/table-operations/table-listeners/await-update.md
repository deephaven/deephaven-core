---
title: await_update
---

The `await_update` method instructs Deephaven to wait for updates to the Table specified in the query.

## Syntax

```
table.await_update(timeout: int = None) -> bool
```

## Parameters

<ParamTable>
<Param name="timeout" type="int" optional>

The maximum time to wait, in milliseconds. The default is `None`, meaning the operation will never time out.

</Param>
</ParamTable>

## Returns

`True` when the table is updated, or `False` if the timeout is reached.

## Examples

In this example, the result of `await_update` is called and printed twice. The first time, 0 milliseconds have elapsed, and the method returns `false` since the table has not been updated. `await_update` is called again immediately afterward with a wait time of 1000 milliseconds. By that time, the source table has been updated, so the method returns `true`.

```python order=source,result
from deephaven import time_table

source = time_table("PT1S")

print(source.await_update(0))
print(source.await_update(1000))

result = source.update(formulas=("renamedTimestamp = Timestamp"))
```

## Related documentation

- [Create a time table](../../../how-to-guides/time-table.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#awaitUpdate())
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.await_update)
