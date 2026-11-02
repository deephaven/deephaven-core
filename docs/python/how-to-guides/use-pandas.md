---
title: Convert between Deephaven tables and pandas DataFrames
---

[pandas](https://pandas.pydata.org/) is a popular Python library for data analysis and manipulation. It centers around [DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html), much as Deephaven centers around tables. Converting lets you take a [snapshot](../reference/table-operations/snapshot/snapshot.md) of a live Deephaven table into pandas and its ecosystem of analysis tools, then bring the results back as a table that the rest of your queries can use.

This guide shows how to convert between Deephaven tables and pandas DataFrames. It also shows how to:

- Choose which columns to convert.
- Choose which pandas data types (dtypes) the DataFrame columns use.
- Control how Deephaven [nulls](../reference/query-language/types/nulls.md) convert, in both directions.
- Infer types for `object` columns.

The [`deephaven.pandas`](/core/pydoc/code/deephaven.pandas.html#module-deephaven.pandas) Python module provides only two functions:

- [`to_pandas`](../reference/pandas/to-pandas.md): Converts a Deephaven table to a pandas DataFrame.
- [`to_table`](../reference/pandas/to-table.md): Converts a pandas DataFrame to a Deephaven table.

> [!IMPORTANT]
> Converting between Deephaven tables and pandas DataFrames copies the data into memory. Be cautious when converting large datasets, and convert only the columns you need (see [Convert a subset of columns](#convert-a-subset-of-columns)). If the table is [refreshing](../conceptual/table-types.md#standard-streaming-tables), [`to_pandas`](../reference/pandas/to-pandas.md) converts a [snapshot](../reference/table-operations/snapshot/snapshot.md) of its current contents, and the DataFrame does not change when the table updates.

## Convert a table to a DataFrame and back

The following example uses [`new_table`](../reference/table-operations/create/newTable.md) to create a table, then converts it to a DataFrame with [`to_pandas`](../reference/pandas/to-pandas.md) and back to a table with [`to_table`](../reference/pandas/to-table.md).

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

Each column keeps a matching data type through the round trip: the [`meta_table`](../reference/table-operations/metadata/meta_table.md) of `result` shows the same column types as the [`meta_table`](../reference/table-operations/metadata/meta_table.md) of `source`. The log shows each column's dtype in the intermediate DataFrame:

```python test-set=1 order=source_meta,result_meta,:log
source_meta = source.meta_table
result_meta = result.meta_table
print(df.dtypes)
```

Deephaven `char` columns become `UInt16` columns that hold character codes, and [`to_table`](../reference/pandas/to-table.md) turns them back into `char` columns.

## Convert a subset of columns

Both [`to_pandas`](../reference/pandas/to-pandas.md) and [`to_table`](../reference/pandas/to-table.md) accept a `cols` argument to convert only some of the columns. The following example uses `source` and `df` from [Convert a table to a DataFrame and back](#convert-a-table-to-a-dataframe-and-back):

```python test-set=1 order=df_subset,result_subset
df_subset = dhpd.to_pandas(source, cols=["Strings", "Doubles", "Chars"])
result_subset = dhpd.to_table(df, cols=["Booleans", "Doubles"])
```

## Choose a dtype backend

A dtype backend is the kind of array that stores each DataFrame column. The `dtype_backend` argument of [`to_pandas`](../reference/pandas/to-pandas.md) selects it:

- `"numpy_nullable"` (the default) uses pandas nullable dtypes, which can hold a missing value, [`pandas.NA`](https://pandas.pydata.org/docs/user_guide/missing_data.html). Numeric, `char`, Boolean, and string columns get them, for example `Int32` and `boolean`. Deephaven [null values](../reference/query-language/types/nulls.md) in those columns become `pandas.NA`. [Nulls](../reference/query-language/types/nulls.md) in [date-time](../reference/query-language/types/date-time.md) columns become `NaT`, the pandas missing value for date-times.
- `"pyarrow"` uses [PyArrow](./pyarrow.md)-backed dtypes for numeric, `char`, Boolean, string, and [date-time](../reference/query-language/types/date-time.md) columns, and Deephaven [null values](../reference/query-language/types/nulls.md) in them become `pandas.NA`. This option requires pandas 2.0.0 or later.
- `None` builds each column from a [NumPy](./use-numpy.md) array. Conversion of Deephaven [null values](../reference/query-language/types/nulls.md) to `pandas.NA` is still on by default, so numeric, `char`, and Boolean columns get pandas nullable dtypes, just as they do with `"numpy_nullable"`. To get plain [NumPy](./use-numpy.md) dtypes, turn null conversion off, as described in [Keep null sentinel values](#keep-null-sentinel-values).

The following example converts the `source` table from [Convert a table to a DataFrame and back](#convert-a-table-to-a-dataframe-and-back) with the `"pyarrow"` and `None` backends and prints the dtypes of each DataFrame. Compare them with the default `"numpy_nullable"` dtypes printed in that section:

```python test-set=1 order=:log
df_pyarrow = dhpd.to_pandas(source, dtype_backend="pyarrow")
df_numpy = dhpd.to_pandas(source, dtype_backend=None)

print("pyarrow backend:")
print(df_pyarrow.dtypes)
print("None backend:")
print(df_numpy.dtypes)
```

The `pyarrow` DataFrame uses `[pyarrow]` dtypes for every column. The `None` DataFrame has the same nullable dtypes as the default for `Ints`, `Booleans`, `Doubles`, and `Chars`, because null conversion is still on. Only `Strings` differs: it is built from a [NumPy](./use-numpy.md) array instead of getting the pandas nullable `string` dtype.

### Keep null sentinel values

Deephaven stores a [null](../reference/query-language/types/nulls.md) in a primitive column as a reserved value from that type's range, called a sentinel value, such as [`NULL_INT`](../reference/query-language/types/nulls.md) for `int`.

The `conv_null` argument of [`to_pandas`](../reference/pandas/to-pandas.md) controls whether Deephaven [null values](../reference/query-language/types/nulls.md) become `pandas.NA`. It defaults to `True`. You can set it to `False` only with `dtype_backend=None`. With any other backend, `conv_null=False` raises a `DHError`.

With `conv_null=False`, each column gets a plain [NumPy](./use-numpy.md) dtype:

- Numeric columns get dtypes such as `int32` and `float64`, and `char` columns get `uint16`. Sentinel values stay in the DataFrame as ordinary numbers instead of becoming `pandas.NA`.
- Boolean columns get the [NumPy](./use-numpy.md) `bool` dtype, and a null Boolean value reads as `True`.

The following example converts the `source` table from [Convert a table to a DataFrame and back](#convert-a-table-to-a-dataframe-and-back) with `conv_null=False` and prints the resulting dtypes:

```python test-set=1 order=:log
df_no_na = dhpd.to_pandas(source, dtype_backend=None, conv_null=False)

print(df_no_na.dtypes)
```

The following example shows how one null `int` value converts with the default `conv_null` setting and with `conv_null=False`:

```python test-set=1 order=:log
from deephaven.constants import NULL_INT

with_null = new_table([dhcol.int_col("Ints", [1, NULL_INT])])

print("conv_null=True (default):")
print(dhpd.to_pandas(with_null, dtype_backend=None))
print("conv_null=False:")
print(dhpd.to_pandas(with_null, dtype_backend=None, conv_null=False))
```

With the default, the null is `<NA>` in an `Int32` column. With `conv_null=False`, it is `-2147483648`, the value of [`NULL_INT`](../reference/query-language/types/nulls.md), in an `int32` column.

## Convert missing values back to Deephaven nulls

In the other direction, [`to_table`](../reference/pandas/to-table.md) converts `pandas.NA` in nullable integer, Boolean, string, and [PyArrow](./pyarrow.md) columns to Deephaven [nulls](../reference/query-language/types/nulls.md). Missing values in a [NumPy](./use-numpy.md) `float64` column, or in a nullable `Float32` or `Float64` column, become [`NaN`](../reference/query-language/types/NaNs.md) instead. See [Null, infinity, and NaN values](./null-inf-nan.md) for how Deephaven treats each.

The following example converts a DataFrame with a nullable `Int32` column and a plain `float64` column, each with one missing value. It then uses [`update`](../reference/table-operations/select/update.md) to check the missing values with the built-in [`isNull`](../reference/query-language/types/nulls.md) and [`isNaN`](../reference/query-language/types/NaNs.md) functions:

```python order=t_nulls
from deephaven import pandas as dhpd
import pandas as pd

df_nulls = pd.DataFrame(
    {
        "NullableInt": pd.Series([1, None], dtype="Int32"),
        "PlainFloat": pd.Series([1.0, float("nan")]),
    }
)

t_nulls = dhpd.to_table(df_nulls).update(
    [
        "IntIsNull = isNull(NullableInt)",
        "FloatIsNull = isNull(PlainFloat)",
        "FloatIsNaN = isNaN(PlainFloat)",
    ]
)
```

In the second row, `IntIsNull` is `true`: the missing `Int32` value became a Deephaven [null](../reference/query-language/types/nulls.md). `FloatIsNull` is `false` and `FloatIsNaN` is `true`: the missing `float64` value stayed [`NaN`](../reference/query-language/types/NaNs.md).

## Infer types for `object` columns

DataFrames sometimes contain generic `object`-dtype columns that don't directly translate to Deephaven column types. By default, [`to_table`](../reference/pandas/to-table.md) calls the pandas [`convert_dtypes`](https://pandas.pydata.org/docs/reference/api/pandas.Series.convert_dtypes.html) method on each `object`-dtype column to infer a better type before conversion. Set `infer_objects=False` to skip this step.

In the following example, columns `A` and `B` have ordinary numeric dtypes, and column `C` is an `object`-dtype column because it holds `pandas.NA`. Inference affects only `C`:

```python order=infer_meta,no_infer_meta,result_infer,result_no_infer,df_mixed
from deephaven import pandas as dhpd
import pandas as pd

df_mixed = pd.DataFrame({"A": [1, 2, 3], "B": [1, 2.1, 3], "C": [1, pd.NA, 3]})

result_infer = dhpd.to_table(df_mixed)
result_no_infer = dhpd.to_table(df_mixed, infer_objects=False)

infer_meta = result_infer.meta_table
no_infer_meta = result_no_infer.meta_table
```

In `infer_meta`, column `C` is a `long` column, and its missing value becomes a Deephaven [null](../reference/query-language/types/nulls.md). In `no_infer_meta`, column `C` is a generic Python object ([`PyObject`](../reference/python/deephaven-python-types.md)) column.

## Related documentation

- [Table types](../conceptual/table-types.md)
- [Deephaven and PyArrow](./pyarrow.md)
- [Null, infinity, and NaN values](./null-inf-nan.md)
- [Use NumPy in Deephaven queries](./use-numpy.md)
- [`bool_col`](../reference/table-operations/create/boolCol.md)
- [`char_col`](../reference/table-operations/create/charCol.md)
- [`double_col`](../reference/table-operations/create/doubleCol.md)
- [`int_col`](../reference/table-operations/create/intCol.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [Nulls](../reference/query-language/types/nulls.md)
- [`string_col`](../reference/table-operations/create/stringCol.md)
- [`to_pandas`](../reference/pandas/to-pandas.md)
- [`to_table`](../reference/pandas/to-table.md)
- [`update`](../reference/table-operations/select/update.md)
- [Pydoc](/core/pydoc/code/deephaven.pandas.html)
