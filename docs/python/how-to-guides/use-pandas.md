---
title: Use pandas in Deephaven queries
sidebar_label: pandas
---

This guide covers the intersection of [Pandas](https://pandas.pydata.org/) and Deephaven in queries. Pandas is a popular Python library for data analysis and manipulation that centers around [DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html), similar to how Deephaven centers around tables. Deephaven's Pandas integration is used to convert between tables and DataFrames.

## `deephaven.pandas`

> [!IMPORTANT]
> Converting between Deephaven tables and Pandas DataFrames copies the entire objects into memory. Be cautious when converting large datasets.

The [`deephaven.pandas`](/core/pydoc/code/deephaven.pandas.html#module-deephaven.pandas) Python module provides only two functions:

- [`to_pandas`](../reference/pandas/to-pandas.md): Converts a Deephaven table to a Pandas DataFrame.
- [`to_table`](../reference/pandas/to-table.md): Converts a Pandas DataFrame to a Deephaven table.

The following example creates a table, then converts it to a DataFrame and back.

```python test-set=1 order=source,result
from deephaven import column as dhcol
from deephaven import pandas as dhpd
from deephaven import new_table

source = new_table(
    [
        dhcol.string_col("Strings", ["Hello", "ABC", "123"]),
        dhcol.int_col("Ints", [1, 100, 10000]),
        dhcol.bool_col("Booleans", [True, False, True]),
        dhcol.double_col("Doubles", [-3.14, 2.73, 9999.999]),
        dhcol.char_col("Chars", "qrs"),
    ]
)

df = dhpd.to_pandas(source)

result = dhpd.to_table(df)
```

The resultant data types in the Pandas DataFrame and table are correct:

```python test-set=1 order=result_meta,:log
result_meta = result.meta_table
print(df.dtypes)
```

[`to_pandas`](../reference/pandas/to-pandas.md) and [`to_table`](../reference/pandas/to-table.md) can convert only a subset of available columns:

```python test-set=1 order=result_onecol
df_onecol = dhpd.to_pandas(source, cols=["Strings", "Doubles", "Chars"])
result_onecol = dhpd.to_table(df, cols=["Booleans", "Doubles"])
```

[`to_pandas`](../reference/pandas/to-pandas.md), by default, converts a table to a Pandas DataFrame that is backed by NumPy arrays with no nullable dtypes. Instead, NumPy nullable and PyArrow backends can be used.

```python test-set=1 order=null
df_numpy_nullable = dhpd.to_pandas(source, dtype_backend="numpy_nullable")
df_pyarrow = dhpd.to_pandas(source, dtype_backend="pyarrow")
```

If `dtype_backend` is `None`, `conv_null` can be set to `False`, which will _not_ convert null values to `pandas.NA`.

```python test-set=1 order=null
df_no_na = dhpd.to_pandas(source, dtype_backend=None, conv_null=False)
```

> [!WARNING]
> `conv_null=False` will result in an error if `dtype_backend` is not `None`.

Pandas DataFrames sometimes contain generic `Object` columns that don't directly translate to Deephaven column types. For instance, the following DataFrame has an `Object` column. By default, Deephaven calls `pandas.DataFrame.convert_dtypes()` prior to conversion. This can be turned off by setting `infer_objects=False`.

```python order=infer_meta,no_infer_meta,result_infer,result_no_infer,df
from deephaven.pandas import to_table
import pandas as pd

df = pd.DataFrame({"A": [1, 2, 3], "B": [1, 2.1, 3], "C": [1, pd.NA, 3]})

result_infer = to_table(df)
result_no_infer = to_table(df, infer_objects=False)

infer_meta = result_infer.meta_table
no_infer_meta = result_no_infer.meta_table
```

## Related documentation

- [Select data in tables](./use-select-view-update.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`merge`](../reference/table-operations/merge/merge.md)
- [`to_pandas`](../reference/pandas/to-pandas.md)
- [`to_table`](../reference/pandas/to-table.md)
- [`update`](../reference/table-operations/select/update.md)
