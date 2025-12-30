---
title: to_pandas
---

The `to_pandas` method converts a Deephaven table to a `pandas.DataFrame`.

> [!CAUTION]
> `to_pandas` clones an _entire table_ into memory. For large tables, consider limiting the number of rows and columns in the table before using this method.

## Syntax

```python syntax
to_pandas(
  table: Table,
  cols: list[str] = None,
  dtype_backend: str = 'numpy_nullable',
  conv_null: bool = True
) -> pandas.DataFrame
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The source table to convert.

</Param>
<Param name="cols" type="list[str]" optional>

The source table columns to convert. If not specified, all columns are converted.

</Param>
<Param name="dtype_backend" type="str" optional>

Which `dtype_backend` to use. The default is "numpy_nullable".

- `"numpy_nullable"` uses nullable NumPy data types as the backend.
- `None` uses non-nullable NumPy data types as the backend.
- `"PyArrow"` uses PyArrow data types as the backend.

Both “numpy_nullable” and “pyarrow” automatically convert Deephaven nulls to pandas NA and enable pandas extension types. Extension types are needed to support types beyond NumPy’s type system. Extension types support operations such as properly mapping Java Strings to Python strings.

</Param>
<Param name="conv_null" type="bool" optional>

When `dtype_backend` is set to `None`, `conv_null` determines whether to check for Deephaven nulls in the data and automatically replace them with `pd.NA`. The default is True.

</Param>
</ParamTable>

## Returns

A `pandas.DataFrame`.

## Example

The following example uses [`new_table`](../table-operations/create/newTable.md) to create a table, then converts it to a `pandas.DataFrame` with `to_pandas`.

```python order=result,source
from deephaven.pandas import to_pandas
from deephaven import new_table
from deephaven.column import int_col

source = new_table([int_col("Num1", [1, 2, 3, 4]), int_col("Num2", [5, 6, 7, 8])])

result = to_pandas(source)
```

## Related documentation

- [Create a new table](../../how-to-guides/new-and-empty-table.md#new_table)
- [`new_table`](../table-operations/create/newTable.md)
- [Pydoc](/core/pydoc/code/deephaven.pandas.html#deephaven.pandas.to_pandas)
