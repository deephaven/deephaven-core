---
title: Strings
---

String values can be represented in Deephaven query strings by using backticks `` ` ``.

## Syntax

```
`string`
```

## Usage

### Filter

The following example shows a query string used to filter data. This query string returns items in the `Value` column that are equal to the string `` `C` ``.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col

source = new_table([string_col("Value", ["A", "B", "C", "D", "E"])])

result = source.where(filters=["Value = `C`"])
```

## Related documentation

- [Built-in query language constants](../../../how-to-guides/built-in-constants.md)
- [Built-in query language variables](../../../how-to-guides/built-in-variables.md)
- [Built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [Formulas in query strings](../../../how-to-guides/formulas.md)
- [Filters in query strings](../../../how-to-guides/filters.md)
- [Operators in query strings](../../../how-to-guides/operators.md)
- [Python variables in query strings](../../../how-to-guides/python-variables.md)
- [Python functions in query strings](../../../how-to-guides/python-functions.md)
- [Python classes in query strings](../../../how-to-guides/python-classes.md)
- [`where`](../../table-operations/filter/where.md)
- [`update`](../../table-operations/select/update.md)
- [Pydoc](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.string)
