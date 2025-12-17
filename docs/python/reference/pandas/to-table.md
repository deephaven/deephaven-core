---
title: to_table
---

The `to_table` method creates a new table from a `pandas.DataFrame`.

## Syntax

```python syntax
to_table(
    df: pandas.DataFrame,
    cols: list[str] = None,
    infer_objects: bool = True
) -> Table
```

## Parameters

<ParamTable>
<Param name="df" type="pandas.DataFrame">

The `pandas.DataFrame` instance.

</Param>
<Param name="cols" type="list[str]" optional>

The columns to convert. If not specified, all columns are converted.

</Param>
<Param name="infer_objects" type="bool" optional>

Whether to infer the best possible types for columns of the generic `Object` type in the DataFrame before creating the table. When `True`, Pandas `convert_dtypes()` method is called before creating the table. Any conversion will make a copy of the data. The default value is `True`.

</Param>
</ParamTable>

## Returns

A Deephaven Table.

## Examples

The following example uses pandas to create a DataFrame, then converts it to a Deephaven Table with `to_table`.

```python order=result,df
from deephaven.pandas import to_table
import pandas as pd

d = {"col1": [1, 2], "col2": [3, 4]}
df = pd.DataFrame(data=d)

result = to_table(df)
```

The following example uses the `cols` parameter to convert only the specified columns.

```python order=result,df
from deephaven.pandas import to_table
import pandas as pd

d = {"col1": [1, 2], "col2": [3, 4]}
df = pd.DataFrame(data=d)

result = to_table(df, ["col1"])
```

The following example creates a DataFrame with a generic `Object` type column. It then converts it to a table twice: once with `infer_objects=True` and once with `infer_objects=False`. The metadata for each resulting table is shown to demonstrate the difference in column types.

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

- [Create a new table](../../how-to-guides/new-and-empty-table.md#new_table)
- [`new_table`](../table-operations/create/newTable.md)
- [Pydoc](/core/pydoc/code/deephaven.pandas.html#deephaven.pandas.to_table)
