---
title: awaitUpdate
---

The `awaitUpdate` method instructs Deephaven to wait for updates to the table specified in the query.

## Syntax

```
table.awaitUpdate()
table.awaitUpdate(timeout)
```

## Parameters

<ParamTable>
<Param name="timeout" type="long">

The maximum time to wait, in milliseconds.

</Param>
</ParamTable>

## Returns

A boolean value - `false` if the timeout elapses without notification, `true` otherwise.

## Examples

In this example, the result of `awaitUpdate` is called and printed twice. The first time, 1 milliseconds have elapsed, and the method returns `false` since the table has not been updated. `awaitUpdate` is called again immediately afterward with a wait time of 1000 milliseconds. By that time, the source table has been updated, so the method returns `true`.

```groovy order=source,result
source = timeTable("PT00:00:01")

println(source.awaitUpdate(1))
println(source.awaitUpdate(1000))

result = source.update(formulas=("renamedTimestamp = Timestamp"))
```

## Related documentation

- [Create a time table](../../../how-to-guides/time-table.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#awaitUpdate())
