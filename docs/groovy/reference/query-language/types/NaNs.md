---
title: NaNs
---

NaN, or not-a-number, values indicate non-numeric results that come from computations on a dataset.

## Example

Deephaven supports constants for NaN values.

```groovy
result = newTable(
    floatCol("Floats", Float.NaN),
    doubleCol("Doubles", Double.NaN)
)
```

NaNs typically come from computations on datasets. The following example shows how NaNs might end up in a table.

```groovy test-set=1
source = newTable(
    floatCol("NaNFloat", (float)-1.0),
    doubleCol("NaNDouble", -1.0),

).update(
    "NaNFloat = java.lang.Math.sqrt(NaNFloat)",
    "NaNDouble = java.lang.Math.sqrt(NaNDouble)"
)
```

NaN values can be detected using the [`isNaN`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#isNaN(byte)) filter.

```groovy test-set=1
result = source.update(
    "NaNFloatNaN = isNaN(NaNFloat)",
    "NaNDoubleNaN = isNaN(NaNDouble)",
)
```

## Related Documentation

- [How to handle null, infinity, and not-a-number values](../../../how-to-guides/handle-null-inf-nan.md)
- [How to use Deephaven's built-in query language functions](../../../how-to-guides/query-language-functions.md)
- [Query language functions](../query-library/query-language-function-reference.md)
- [`isNaN`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#isNaN(byte))
