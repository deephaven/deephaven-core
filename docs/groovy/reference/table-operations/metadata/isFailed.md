---
title: isFailed
---

The `isFailed` method returns a boolean value that is `true` if the table is in a failure state or `false` if the table is healthy.

## Syntax

```groovy syntax
source.isFailed()
```

## Parameters

This method takes no arguments.

## Returns

A boolean value that is `true` if the table is in a failure state, or `false` if the table is healthy.

## Example

In this example, we will create two tables: a regular time table, and a time table that will fail after two rows. Then, we use Java's `Timer` class to define a method that will print the status of the tables after 5 seconds.

```groovy test-set=1 should-fail
import java.util.Timer

tt = timeTable("PT1S")
fail_tt = tt.update("X=ii", "Y=ii > 1 ? null : `a` + Long.toString(X)", "BooBoo = Y.toLowerCase()")

def getStatus = new Timer()
getStatus.runAfter(5000) {
    println tt.isFailed()
    println fail_tt.isFailed()
}
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#isFailed())
