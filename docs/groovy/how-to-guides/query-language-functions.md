---
title: Use Deephaven's built-in query language functions
sidebar_label: Built-in query language functions
---

Deephaven's query language has many functions that are automatically imported for use. These functions can be used in any [`update`](../reference/table-operations/select/update.md) (or similar) operation without any imports needed. Since these functions are Java functions, they are fast and don't require any data type casting.

This guide shows a few examples of using these automatically imported functions. The [auto import query language functions reference](../reference/query-language/query-library/query-language-function-reference.md) shows all of the Java classes whose functions are automatically imported for use.

## Example: `abs`

This example shows how to convert a column of integers into a column of the absolute values of the integers using `abs`.

```groovy order=result,source
source = newTable(
    intCol("IntegerColumn", 1, 2, -2, -1)
)
result = source.update("Abs = abs(IntegerColumn)")
```

## Example: `parseInt`

This example shows how to convert a column of numeric strings into integers using `parseInt`.

```groovy order=result,source
source = newTable(
    stringCol("StringColumn", "1", "2", "-2", "-1")
)
result = source.update("IntegerColumn = parseInt(StringColumn)")
```

## Example: `parseInstant`

This example shows how to convert a column of [date-time](../reference/query-language/types/date-time.md) strings to [Instant](../reference/query-language/types/date-time.md) objects using `parseInstant`.

```groovy order=result,source
source = newTable(
    stringCol("DateTimeStrings", "2020-01-01T00:00:00 ET", "2020-01-02T00:00:00 ET", "2020-01-03T00:00:00 ET")
)
result = source.update("DateTimes = parseInstant(DateTimeStrings)")
```

## Example: `and`

This example shows how to compute the logical AND of a group of columns of booleans using `and`.

```groovy order=result,source
source = newTable(
    booleanCol("A", true, true, true),
    booleanCol("B", true, true, false),
    booleanCol("C", true, false, true)
)
result = source.update("IsOk = and(A, B, C)")
```

## Example: `absAvg` and `avg`

This example shows how to compute the average of the absolute value of integers in a column using `absAverage`.

```groovy order=result,source
source = newTable(
    intCol("A", -1, 2, 3),
    intCol("B", 1, -2, 3),
    intCol("C", 1, 2, -3),
)
result = source.update("AbsAvg = absAvg(A, B, C)", "Avg = avg(A, B, C)")
```

## Related documentation

- [Auto import query language functions reference](../reference/query-language/query-library/query-language-function-reference.md)
