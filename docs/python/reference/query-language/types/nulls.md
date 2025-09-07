---
title: Nulls
---

Null values indicate that a value does not exist in the data. Within Deephaven, each data type represents nulls differently and special null values are assigned. Complex data types, such as Objects, are stored as standard `null` or `None` references. Primitive types, such as doubles or integers, are stored as a single `null` value from the type’s range.

## Example

The following script displays Deephaven null values:

```python
from deephaven.constants import (
    NULL_BYTE,
    NULL_SHORT,
    NULL_INT,
    NULL_LONG,
    NULL_FLOAT,
    NULL_DOUBLE,
)

nulls = {
    "byte": NULL_BYTE,
    "short": NULL_SHORT,
    "int": NULL_INT,
    "long": NULL_LONG,
    "float": NULL_FLOAT,
    "double": NULL_DOUBLE,
}
print(nulls)
```

Null values can be detected using the `isNull` filter.

```python order=source,result
from deephaven import new_table
from deephaven.column import int_col, double_col
from deephaven.constants import NULL_INT, NULL_DOUBLE

source = new_table(
    [double_col("Doubles", [NULL_DOUBLE, 0.0]), int_col("Integers", [NULL_INT, 0])]
)

result = source.update(
    formulas=["NullDoubles = isNull(Doubles)", "NullIntegers = isNull(Integers)"]
)
```

## Related Documentation

- [Built-in query language constants](../../../how-to-guides/built-in-constants.md)
- [Built-in query language variables](../../../how-to-guides/built-in-variables.md)
- [Built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [How to handle null, infinity, and not-a-number values](../../../how-to-guides/null-inf-nan.md)
