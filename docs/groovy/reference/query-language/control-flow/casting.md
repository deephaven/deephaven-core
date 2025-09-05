---
title: Casting
---

Casting is one of the operators used to construct [formulas](../../../how-to-guides/formulas-how-to.md).

## Usage

`(type)` casting - Casts from one numeric primitive type handled by Java to another.

- byte
- short
- int
- long
- float
- double

> [!NOTE]
> Deephaven [column expressions](../../../how-to-guides/use-select-view-update.md) are [Java](https://docs.oracle.com/javase/specs/jls/se7/html/jls-5.html#jls-5.5) expressions with some extra features, the rules of casting and numbers are consistent with Java.

Each number type has an allotted number of bytes used to store information. Depending on your data needs, consider the data type used in your application.

| Type   | Bytes | Description                   | Example                                    | Example                                   |
| ------ | ----- | ----------------------------- | ------------------------------------------ | ----------------------------------------- |
| byte   | 1     | signed whole numbers          | -123                                       | 123                                       |
| short  | 2     | signed whole numbers          | -30,000                                    | 30,000                                    |
| int    | 4     | signed whole numbers          | -2,634,123                                 | 2,634,123                                 |
| long   | 8     | signed whole numbers          | -8,293,193,496                             | 8,293,193,496                             |
| float  | 4     | signed floating point numbers | -8,293,193,496.2948293                     | 8,293,193,496.2948293                     |
| double | 8     | signed floating point numbers | -64,123,542,927,328,293,193,496.2948293231 | 64,123,542,927,328,293,193,496.2948293231 |

## Example

### Widening conversion

When operations are applied on a type of number that widen the type, the casting will automatically change.

In the following example, column `A` is assigned an integer row element in the `source` table. When operations are applied to that number that require more precision than integer, type allows the new columns to be casted to doubles.

```groovy order=source,result
source = emptyTable(10).update(
                "A = (long) i",
                "B = A * sqrt(2)",
                "C = A / 2"
            )

result = source.meta()
```

### Manually casting

When writing queries, one might need to narrow the casting of the number type. The following example takes a number and reduces the bytes used to store that information. Since the bytes are truncated when narrowing the casting, spurious numbers will result if the number requires more bytes to hold the data.

The table below shows the minimum and maximum values for each data type.

> [!CAUTION]
> The boundary point of each number type might be assigned unexpected values, such as [null](../types/nulls.md) or infinity. If the data is near these boundaries, use a type that allows for more storage.

```groovy order=numbersMax,numbersMin,numbersMinMeta,numbersMaxMeta
numbersMax = newTable(
    doubleCol("MaxNumbers",(2 - 1 / (2**52)) * (2**1023) ,(2 - 1 / (2**23)) * (2**127), (2**63) - 1, (2**31)-1, (2**15) - 1, (2**7) - 1),
).view(
    "DoubleMax = (double) MaxNumbers",
    "FloatMax = (float) MaxNumbers",
    "LongMax = (long) MaxNumbers",
    "IntMax = (int) MaxNumbers",
    "ShortMax = (short) MaxNumbers",
    "ByteMax = (byte) MaxNumbers",
).formatColumns("DoubleMax=Decimal(`0.0E0`)", "FloatMax=Decimal(`0.0E0`)", "LongMax=Decimal(`0.0E0`)")

numbersMin = newTable(
    doubleCol("MinNumbers",1 / (2**1074), 1 / (2**149), -(2**63)+513, -(2**31)+2, -1*(2**15)+1, -(2**7)+1)
).view(
    "DoubleMin = (double) MinNumbers",
    "FloatMin = (float) MinNumbers",
    "LongMin = (long) MinNumbers",
    "IntMin = (int) MinNumbers",
    "ShortMin = (short) MinNumbers ",
    "ByteMin = (byte) MinNumbers ",
).formatColumns("DoubleMin=Decimal(`0.0E0`)", "FloatMin=Decimal(`0.0E0`)", "LongMin=Decimal(`0.0E0`)")

numbersMinMeta = numbersMin.meta().view("Name", "DataType")
numbersMaxMeta = numbersMax.meta().view("Name", "DataType")
```

## Related documentation

- [Access metadata](../../../how-to-guides/metadata.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [How to use filters](../../../how-to-guides/use-filters.md)
- [How to use select, view, and update](../../../how-to-guides/use-select-view-update.md)
- [How to use Deephaven's built-in query language functions](../../../how-to-guides/query-language-functions.md)
- [Query language functions](../query-library/query-language-function-reference.md)
- [Formulas](../../../how-to-guides/formulas-how-to.md)
- [`meta`](../../table-operations/metadata/meta.md)
- [`update`](../../table-operations/select/update.md)

<!-- TODO: [514](https://github.com/deephaven/deephaven.io/issues/514)link to "Filters" and "Select" generally in docs #514-->
