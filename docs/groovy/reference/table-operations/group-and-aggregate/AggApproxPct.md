---
title: AggApproxPct
---

`AggApproxPct` returns an approximate percentile aggregation for the supplied percentile and column name pairs.

<!--TODO: (https://github.com/deephaven/deephaven.io/issues/2554) add code examples-->

## Syntax

```
AggApproxPct(percentile, pairs...)
AggApproxPct(percentile, compression, pairs...)
AggApproxPct(inputColumn, percentileOutputs...)
AggApproxPct(inputColumn, compression, percentileOutputs...)
```

## Parameters

<ParamTable>
<Param name="percentile" type="double">

The percentile value to use for all component aggregations.

</Param>
<Param name="pairs" type="String...">

The source/result column name pairs.

</Param>
<Param name="compression" type="double">

[T-Digest compression](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpecTDigest.html#compression()) factor. This value must be greater than 1 and should be less than 1,000 in most cases.

</Param>
<Param name="percentileOutputs" type="PercentileOutput...">

The percentile/output column name pairs for the component aggregations.

</Param>
</ParamTable>

## Returns

An [approximate percentile](/core/javadoc/io/deephaven/api/agg/spec/AggSpecApproximatePercentile.html) aggregation for the supplied percentile and column names.

## Examples

In this example, `AggApproxPct` returns the approximate percentage value of `Number` as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggApproxPct

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggApproxPct(0.50, "Number")], "X")
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggApproxPct(double,java.lang.String...))
