---
title: Objects
---

The Deephaven Query Language has native support for objects within query strings.

<!-- TODO: https://github.com/deephaven/deephaven-core/issues/1388 and https://github.com/deephaven/deephaven-core/issues/1389
are blocking examples of objects within a table. For now this doc will stick with just objects
within a query string. -->

## Example

The following example shows how to create an object and use one of its methods within a query string.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col


class MyStringClass:
    def __init__(self, strn):
        self.strn = strn

    def my_string(self):
        return self.strn


obj = MyStringClass("A")

source = new_table([string_col("Strings", ["A", "B", "C"])])

result = source.update(formulas=["Match = Strings_[i] == obj.my_string()"])
```

## Related Documentation

- [Built-in query language constants](../../../how-to-guides/built-in-constants.md)
- [Built-in query language variables](../../../how-to-guides/built-in-variables.md)
- [Built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [`update`](../../table-operations/select/update.md)
