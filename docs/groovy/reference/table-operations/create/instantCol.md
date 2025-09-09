---
title: instantCol
---

The `instantCol` method creates a column containing date-time values.

> [!NOTE]
> This method is commonly used with [`newTable`](./newTable.md) to create tables.

## Syntax

```
instantCol(name, data...)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The name of the new column.

</Param>
<Param name="data" type="Instant...">

The column values.

</Param>
</ParamTable>

## Returns

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html).

## Example

The following examples use [`newTable`](./newTable.md) to create a table with a single column of date-times named `DateTimes`.

```groovy
firstTime = parseInstant("2021-07-04T08:00:00 ET")
secondTime = parseInstant("2021-09-06T12:30:00 ET")
thirdTime = parseInstant("2021-12-25T21:15:00 ET")

result = newTable(
    instantCol("DateTimes", firstTime, secondTime, thirdTime)
)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [`newTable`](./newTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#instantCol(java.lang.String,java.time.Instant...))
