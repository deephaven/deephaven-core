---
title: Built-in query language functions
sidebar_label: Built-in functions
---

Like [constants](./built-in-constants.md) and [variables](./built-in-variables.md), there are many built-in functions that can be called from the query language with no additional imports or setup. In most cases, users will prefer these built-in functions over user-defined functions in query strings because:

- They are coded with an eye for performance and correctness.
- Ease of use - built-in functions don't require any additional coding.

## Built-in functions

Built-in functions range from simple mathematical functions to date-time arithmetic, string manipulation, and more.

The following query calculates the absolute value of a column using the built-in [`abs`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#abs(long)) function:

```groovy order=source
source = emptyTable(10).update("X = -ii", "AbsX = abs(X)")
```

The following query parses a column of strings containing integers into Java primitive `int` values using [`parseInt`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Parse.html#parseInt(java.lang.String)):

```groovy order=result,source
source = emptyTable(10).update("StringColumn = Integer.toString(randomInt(0, 9))")
result = source.update("IntegerColumn = parseInt(StringColumn)")
```

The following query uses the built-in [`and`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Logic.html#and(boolean...)) function to evaluate whether or not three different columns are all `true`:

```groovy order=source,result
source = newTable(
    booleanCol("A", true, true, true),
    booleanCol("B", true, true, false),
    booleanCol("C", true, false, true),
)
result = source.update("IsOk = and(A, B, C)")
```

Built-in functions gracefully handle null values as input. For example, the following example calls the built-in [`sin`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) function on a column that contains null values:

```groovy order=source
source = emptyTable(40).update(
    "X = (ii % 3 != 0) ? 0.1 * ii : NULL_DOUBLE", "SinX = sin(X)"
)
```

This page does not provide a comprehensive list of all built-ins. For the complete list of functions built into the query language, see [Auto-imported functions](../reference/query-language/formulas/auto-imported-functions.md).

## Related documentation

- [Query string overview](./query-string-overview.md)
- [Built-in constants](./built-in-constants.md)
- [Built-in variables](./built-in-variables.md)
- [Ternary conditional operator](./ternary-if-how-to.md)
- [Java objects in query strings](./java-classes.md)
- [Auto-imported functions](../reference/query-language/formulas/auto-imported-functions.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
