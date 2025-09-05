---
title: flatten
---

The `flatten` method returns a new copy of the source table with a flat row set - i.e., from 0 to number of rows - 1.

## Syntax

```python syntax
table.flatten() -> Table
```

## Parameters

This method takes no arguments.

## Returns

A Table with a flat row set.

## Example

The following example creates a non-flat time table, and then flattens it.

```groovy order=tt,result
import static io.deephaven.api.agg.Aggregation.AggFirst

tt = timeTable("PT1S").update("X = ii").aggBy(AggFirst("X"), "Timestamp")

println tt.isFlat()

result = tt.flatten()

println result.isFlat()
```

## Related documentation

- [Java](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#flatten())
