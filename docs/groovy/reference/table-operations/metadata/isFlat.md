---
title: isFlat
---

The `isFlat` method returns a boolean value that is `true` if the table is flat, or `false` if it is not.

## Syntax

```groovy syntax
source.isFlat()
```

## Parameters

This method takes no arguments.

## Returns

A boolean value that is `true` if the table is flat or `false` if it is not.

## Example

In this example we create a time table and a `result` table that has an aggregation. Next, we call `isFlat` twice to see which of the tables is flat.

```groovy order=:log
import static io.deephaven.api.agg.Aggregation.AggFirst

tt = timeTable("PT1S").update("X = ii")

result = tt.aggBy([AggFirst("Timestamp")], "X")

println tt.isFlat()
println result.isFlat()
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#isFlat())
