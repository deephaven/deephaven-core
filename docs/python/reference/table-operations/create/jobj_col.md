---
title: jobj_col
---

The `jobj_col` function creates a column containing Java objects. The column's type is `java.lang.Object`, and the function converts Python `str`, `int` (within the 64-bit range), `float`, `bool`, and `None` values to Java values. To store other Python objects, such as dictionaries or lists, use [`pyobj_col`](./pyobj_col.md).

> [!NOTE]
> Pass the column that `jobj_col` returns to [`new_table`](./newTable.md) to create a table.

> [!CAUTION]
> When all values in a column share one type, give the column that type instead of `java.lang.Object` so that later operations can process it efficiently. Use the matching typed function when one exists, such as [`int_col`](./intCol.md) for integers or [`string_col`](./stringCol.md) for strings. Otherwise, create an [`InputColumn`](./inputColumn.md) whose `data_type` is that type, such as `deephaven.dtypes.LocalDate`. `LocalDate`, `LocalTime`, and `BigDecimal` columns need Java objects as input data, because jpy does not convert Python `datetime.date`, `datetime.time`, or `decimal.Decimal` values to these types. Create `LocalDate` and `LocalTime` values with [`to_j_local_date`](../../time/datetime/to_j_local_date.md) and [`to_j_local_time`](../../time/datetime/to_j_local_time.md). See [Python data types](../../python/deephaven-python-types.md) for the available types.

## Syntax

```python syntax
jobj_col(name: str, data: Sequence) -> InputColumn
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the new column.

</Param>
<Param name="data" type="Sequence">

The column values. Values can be Java objects, such as those created with [`jpy`](../../../how-to-guides/use-jpy.md), or Python `str`, `int`, `float`, and `bool` values, which the function converts to Java objects. `None` becomes a null value. NumPy `float64` values convert like `float` values, because `numpy.float64` is a subclass of `float`. Other Python values, such as dictionaries, lists, tuples, `datetime.datetime` values, and NumPy integer and Boolean scalars, raise a `DHError`. A Python `int` outside the 64-bit range also raises a `DHError`.

</Param>
</ParamTable>

## Returns

An [`InputColumn`](./inputColumn.md).

## Examples

The following example uses [`new_table`](./newTable.md) to create a table with a single column, `Values`, that holds Java objects. `jobj_col` converts the Python values in the list to Java objects, as the `ClassName` column shows. Each Python integer becomes the smallest Java integer type that holds it, so `1` becomes a `java.lang.Byte`, and each Python float becomes a `java.lang.Double`.

```python order=result
from deephaven import new_table
from deephaven.column import jobj_col

result = new_table([jobj_col("Values", ["a", 1, -5.5])]).update(
    "ClassName = Values.getClass().getSimpleName()"
)
```

The following example mixes Java objects created with [`jpy`](../../../how-to-guides/use-jpy.md) and a Python string. The Java objects keep their own classes, and `jobj_col` converts the string to a `java.lang.String`.

```python order=result
from deephaven import new_table
from deephaven.column import jobj_col
import jpy

ArrayList = jpy.get_type("java.util.ArrayList")
LocalDate = jpy.get_type("java.time.LocalDate")

result = new_table(
    [jobj_col("Values", [LocalDate.of(2024, 1, 1), ArrayList(), "a"])]
).update("ClassName = Values.getClass().getSimpleName()")
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to use jpy](../../../how-to-guides/use-jpy.md)
- [`InputColumn`](./inputColumn.md)
- [`int_col`](./intCol.md)
- [`new_table`](./newTable.md)
- [`pyobj_col`](./pyobj_col.md)
- [Python data types](../../python/deephaven-python-types.md)
- [`string_col`](./stringCol.md)
- [`to_j_local_date`](../../time/datetime/to_j_local_date.md)
- [`to_j_local_time`](../../time/datetime/to_j_local_time.md)
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.jobj_col)
