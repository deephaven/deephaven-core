---
title: Nulls
---

Null values indicate that a value does not exist in the data. Within Deephaven, each data type represents nulls differently and special null values are assigned. Complex data types, such as Objects, are stored as standard `null` or `None` references. Primitive types, such as doubles or integers, are stored as a single `null` value from the type’s range.

## Example

The following script displays Deephaven null values:

```groovy
nulls = [byte: NULL_BYTE, short: NULL_SHORT, int: NULL_INT, long: NULL_LONG, float: NULL_FLOAT, double: NULL_DOUBLE]
println(nulls)
```

Null values can be detected using the `isNull` filter.

```groovy order=source,result
source = newTable(
    doubleCol("Doubles", NULL_DOUBLE, 0.0),
    intCol("Integers", NULL_INT, 0)
)

result = source.update(
    "NullDoubles = isNull(Doubles)",
    "NullIntegers = isNull(Integers)"
)
```

## Related Documentation

- [How to handle null, infinity, and not-a-number values](../../../how-to-guides/null-inf-nan.md)
- [How to use Deephaven's built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [Query language functions](../query-library/query-language-function-reference.md)
