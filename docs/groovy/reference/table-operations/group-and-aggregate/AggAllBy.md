---
title: aggAllBy
---

`aggAllBy` creates a new table containing grouping columns and grouped data. The resulting grouped data is defined by the aggregation specified.

> [!NOTE]
> Because `aggAllBy` applies the aggregation to all the columns of the table, it will ignore any column names specified for the aggregation.

## Syntax

```
aggAllBy(spec)
aggAllBy(spec, groupByColumns...)
```

## Parameters

<ParamTable>
<Param name="spec" type="AggSpec">

The aggregation specifications (`AggSpecs`) to apply to all columns of the source table.
Note that since `aggAllBy` automatically applies aggregations accross all columns, this method requires an `AggSpec` as a parameter instead of an `Aggregation`.

AggSpec options are as follows:

- [`AggSpec.absSum()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#absSum())
- [`AggSpec.aggregation(pair)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#aggregation(io.deephaven.api.Pair))
- [`AggSpec.aggregation(pairs...)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#aggregation(io.deephaven.api.Pair...))
- [`AggSpec.approximatePercentile(percentile)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#approximatePercentile(double))
- [`AggSpec.approximatePercentile(percentile,compression)`](/core/javadoc/io/deephaven/api/agg/spec/AggSpecApproximatePercentile.html)
- [`AggSpec.avg()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#avg())
- [`AggSpec.countDistinct()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#countDistinct())
- [`AggSpec.countDistinct(countNulls)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#countDistinct(boolean))
- [`AggSpec.distinct()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#distinct())
- [`AggSpec.distinct(includeNulls)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#distinct(boolean))
- [`AggSpec.first()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#first())
- [`AggSpec.formula(formula,paramToken)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#formula(java.lang.String,java.lang.String))
- [`AggSpec.freeze()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#freeze())
- [`AggSpec.group()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#group())
- [`AggSpec.last()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#last())
- [`AggSpec.max()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#max())
- [`AggSpec.median()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#median())
- [`AggSpec.median(averageEvenlyDivided)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#median(boolean))
- [`AggSpec.min()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#min())
- [`AggSpec.percentile(percentile)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#percentile(double))
- [`AggSpec.percentile(percentile,averageEvenlyDivided)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#percentile(double,boolean))
- [`AggSpec.sortedFirst(columns...)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#sortedFirst(java.lang.String...))
- [`AggSpec.sortedFirst(columns)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#sortedFirst(java.util.Collection))
- [`AggSpec.sortedLast(columns...)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#sortedLast(java.lang.String...))
- [`AggSpec.sortedLast(columns)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#sortedLast(java.util.Collection))
- [`AggSpec.std()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#std())
- [`AggSpec.sum()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#sum())
- [`AggSpec.tDigest()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#tDigest())
- [`AggSpec.tDigest(compression)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#tDigest(double))
- [`AggSpec.unique()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#unique())
- [`AggSpec.unique(includeNulls,nonUniqueSentinel)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#unique(boolean,java.lang.Object))
- [`AggSpec.var()`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#var())
- [`AggSpec.wavg(weightColumn)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#wavg(java.lang.String))
- [`AggSpec.wsum(weightColumn)`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#wsum(java.lang.String))

A comprehensive list of AggSpec options and further information can be found [here](/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html).

</Param>
<Param name="groupByColumns" type="String...">

The group-by column names.

</Param>
<Param name="groupByColumns" type="ColumnName...">

The group-by column names.

</Param>
<Param name="groupByColumns" type="Collection<String>">

The group-by column names.

</Param>
</ParamTable>

## Returns

A new table containing grouping columns and grouped data.

## Examples

In this example, `aggAllBy` returns the minimum `"Number"` values, grouped by `X`.

```groovy order=source,result
import io.deephaven.qst.table.*
import io.deephaven.api.agg.spec.*

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "A", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "M", "N", "N", "M", "O", "P", "N", "M", "N", "M", "N", "N", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 55, 130, 230, 50, 76, 137, 214, 55, 76, 55, 130, 230, 50, 76, 137, 214),
)

result = source.aggAllBy(AggSpec.min(), "X")
```

In this example, `aggAllBy` returns the 99th percentile of each column.

```groovy order=source,result
import io.deephaven.qst.table.*
import io.deephaven.api.agg.spec.*

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "A", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "M", "N", "N", "M", "O", "P", "N", "M", "N", "M", "N", "N", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 55, 130, 230, 50, 76, 137, 214, 55, 76, 55, 130, 230, 50, 76, 137, 214),
)

result = source.aggAllBy(AggSpec.percentile(0.99))
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`aggBy`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/qst/table/TableBase.html#aggAllBy(io.deephaven.api.agg.spec.AggSpec,io.deephaven.api.ColumnName...))
