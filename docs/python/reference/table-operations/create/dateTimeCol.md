---
title: datetime_col
---

The `datetime_col` function creates a column of Java [`Instant`](../../query-language/types/date-time.md#instant) values.

> [!NOTE]
> Pass the column that `datetime_col` returns to [`new_table`](./newTable.md) to create a table.

## Syntax

```python syntax
datetime_col(name: str, data: Sequence) -> InputColumn
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the new column.

</Param>
<Param name="data" type="Sequence">

The column values, as a list, tuple, NumPy array, or other sequence. Each value is a Java `Instant`, `None` (stored as a null value), or a value that `datetime_col` converts to an `Instant`: `int` nanoseconds since the Epoch, a date-time string that includes a time zone (such as `"2021-07-04T08:00:00 ET"`), `datetime.datetime`, `numpy.datetime64`, or `pandas.Timestamp`.

</Param>
</ParamTable>

> [!CAUTION]
> A naive `datetime.datetime` value, which has no time zone, can be read as UTC or in the server's local time zone, depending on how `datetime_col` converts the sequence. For example, it is read as UTC in a list of `datetime.datetime` values, but in the server's local time zone when the list also contains a string or when the values are in an object-dtype NumPy array. Pass `datetime.datetime` values that include a time zone.

## Returns

An [`InputColumn`](./inputColumn.md).

## Examples

The following examples use [`new_table`](./newTable.md) to create a table with a single column of date-times named `DateTimes`.

This example creates the values as Java `Instant`s with [`to_j_instant`](../../time/datetime/to_j_instant.md):

```python order=result
from deephaven.time import to_j_instant
from deephaven import new_table
from deephaven.column import datetime_col

first_time = to_j_instant("2021-07-04T08:00:00 ET")
second_time = to_j_instant("2021-09-06T12:30:00 ET")
third_time = to_j_instant("2021-12-25T21:15:00 ET")

result = new_table([datetime_col("DateTimes", [first_time, second_time, third_time])])
```

This example passes date-time strings, which `datetime_col` converts to Java `Instant`s:

```python order=result
from deephaven import new_table
from deephaven.column import datetime_col

result = new_table(
    [
        datetime_col(
            "DateTimes",
            [
                "2021-07-04T08:00:00 ET",
                "2021-09-06T12:30:00 ET",
                "2021-12-25T21:15:00 ET",
            ],
        )
    ]
)
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [Date-time types](../../query-language/types/date-time.md)
- [`InputColumn`](./inputColumn.md)
- [`new_table`](./newTable.md)
- [`to_j_instant`](../../time/datetime/to_j_instant.md)
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.datetime_col)
