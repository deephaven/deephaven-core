---
title: rollup
---

The Deephaven `rollup` method creates a rollup table from a source table with zero or more aggregations and zero or more grouping columns to create a hierarchy.

## Syntax

```python syntax
result = source.rollup(
    aggs: list[Aggregation],
    by: list[str] = None,
    include_constituents: bool = False,
) -> RollupTable
```

## Parameters

<ParamTable>
<Param name="aggs" type="list[Aggregation]">

A list of aggregations. If `None`, no aggregations are performed.

The following aggregations are supported:

- [`abs_sum`](../group-and-aggregate/AggAbsSum.md)
- [`avg`](../group-and-aggregate/AggAvg.md)
- [`count_`](../group-and-aggregate/AggCount.md)
- [`count_distinct`](../group-and-aggregate/AggCountDistinct.md)
- [`count_where`](../group-and-aggregate/AggCountWhere.md)
- [`first`](../group-and-aggregate/AggFirst.md)
- [`last`](../group-and-aggregate/AggLast.md)
- [`max_`](../group-and-aggregate/AggMax.md)
- [`min_`](../group-and-aggregate/AggMin.md)
- [`sorted_first`](../group-and-aggregate/AggSortedFirst.md)
- [`sorted_last`](../group-and-aggregate/AggSortedLast.md)
- [`std`](../group-and-aggregate/AggStd.md)
- [`sum`](../group-and-aggregate/AggSum.md)
- [`unique`](../group-and-aggregate/AggUnique.md)
- [`var`](../group-and-aggregate/AggVar.md)
- [`weighted_avg`](../group-and-aggregate/AggWAvg.md)
- [`weighted_sum`](../group-and-aggregate/AggWSum.md)

</Param>
<Param name="by" type="list[str]">

Zero or more column names to group on and create a hierarchy from. If `None`, no hierarchy is created.

</Param>
<Param name="include_constituents" optional type="bool">

Whether or not to include constituent rows at the leaf level. Default is False.

</Param>
</ParamTable>

## Methods

### Instance

- `with_filters(filters...)` - Create a new rollup table that applies a set of filters to the `groupByColumns` of the rollup table.
- `with_update_view(columns...)` - Create a new rollup table that applies a set of `update_view` operations to the `groupByColumns` of the rollup table.
- `node_operation_recorder(nodeType)` - Get a [`recorder`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.RollupNodeOperationsRecorder) for per-node operations to apply during snapshots of the requested [`NodeType`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.NodeType.html).
- `with_node_operations(recorders...)` - Create a new rollup table that applies the [`recorded`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.RollupNodeOperationsRecorder) operations to nodes when gathering snapshots.

## Returns

A rollup table.

## Examples

The following example creates two rollup tables from a source table of insurance expense data. The first performs no aggregations, but creates a hierarchy from the `region` and `age` columns. The second performs two aggregations: the aggregated average of the `bmi` and `expenses` columns are calculated, then the same `by` columns are given as the first. The optional argument `include_constituents` is set to `True` so that members of the lowest-level nodes (individual cells) can be expanded.

```python order=insurance,insurance_rollup
from deephaven import read_csv, agg

insurance = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)

agg_list = [agg.avg(cols=["bmi", "expenses"])]
by_list = ["region", "age"]

insurance_rollup = insurance.rollup(
    aggs=agg_list, by=by_list, include_constituents=False
)
```

Similar to the previous example, this example creates a rollup table from a source table of insurance expense data. However, this time we are filtering on the source table before applying the rollup using `with_filters`. Both group and constituent columns can be used in the filter, while aggregation columns cannot.

```python order=insurance,insurance_rollup
from deephaven import read_csv, agg

insurance = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)

agg_list = [agg.avg(cols=["bmi", "expenses"])]
by_list = ["region", "age"]
filter_list = ["age > 30"]

insurance_rollup = insurance.rollup(
    aggs=agg_list, by=by_list, include_constituents=False
).with_filters(filter_list)
```

The following example creates a rollup table from real-time source data. The source data updates 10,000 times per second. The `result` rollup table can be expanded by the `N` column to show unique values of `M` for each `N`. The aggregated average and sum are calculated for the `Value` and `Weight`, respectively.

```python ticking-table order=null
from deephaven import empty_table, time_table
from deephaven import agg

agg_list = [agg.avg(cols=["Value"]), agg.sum_(cols=["Weight"])]
by_list = ["N", "M"]

rows = empty_table(1_000_000).update_view(["Group = i", "N = i % 347", "M = i % 29"])
changes = (
    time_table("PT0.0001S")
    .view(
        [
            "Group = i % 1_000_000",
            "LastModified = Timestamp",
            "Value = (i * Math.sin(i)) % 6977",
            "Weight = (i * Math.sin(i)) % 7151",
        ]
    )
    .last_by("Group")
)

source = rows.join(changes, "Group")

result = source.rollup(aggs=agg_list, by=by_list)
```

![The above `result` rollup table](../../../assets/how-to/rollup-table-realtime.gif)

## Formula Aggregations in Rollups

When a rollup includes a formula aggregation, care should be taken with the function being applied. On each tick, the formula is evaluated for every changed row in the output table. Since the aggregated rows include numerous source rows, the input vectors for a formula aggregation can become very large — encompassing the entire source table at the root level. If the formula is inefficient when handling large input vectors, it may negatively impact the rollup's performance.

By default, the formula aggregation operates on a group of all the values as they appeared in the source table. In this example, the `Value` column contains the same vector that is used as input to the formula:

```python order=simple_sum,source
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven import agg

source = new_table(
    [
        string_col(
            "Key", ["Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"]
        ),
        int_col("Value", [10, 10, 10, 20, 20, 30, 30]),
    ]
)
simple_sum = source.rollup(
    aggs=[agg.group(cols=["Value"]), agg.formula(formula="Sum = sum(Value)")],
    by=["Key"],
)
```

To calculate the sum for the root row, every row in the source table is read. The Deephaven engine provides detailed update information for rows in the table (i.e., which rows are added, removed, modified, or shifted). Even though a vector contains many values, it is contained within a single row; therefore, the Deephaven engine does not provide detailed update information for a vector. Every time the table ticks, the formula is completely re-evaluated.

### Formula Reaggregation

Formula reaggregation can be used to limit the size of input vectors while evaluating changes to a rollup. When writing your query, be mindful of the requirement that your formula must be applicable to each level of the rollup and produce the same output type.

```python order=reaggregated_sum,source
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven import agg

source = new_table(
    [
        string_col(
            "Key", ["Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"]
        ),
        int_col("Value", [10, 10, 10, 20, 20, 30, 30]),
    ]
)
reaggregated_sum = source.update_view(formulas=["Sum=Value"]).rollup(
    aggs=[agg.formula(formula="Sum = sum(Sum)", reaggregating=True)], by=["Key"]
)
```

`reaggregated_sum` and `simple_sum` produce the same results but operate differently. `simple_sum` reads the source table twice: first to calculate individual sums and then to compute the overall total. In contrast, `reaggregated_sum` reads the source table only once to gather the individual sums and then uses those intermediate sums to get the total.

If a new row with the key `Delta` is added to the source, `simple_sum` will read all eight rows again to recalculate the sums. However, `reaggregated_sum` will only recalculate the sum for `Delta` and then read the intermediate sums for `Alpha`, `Bravo`, `Charlie`, and `Delta`, not all rows. As the number of keys and the size of the data grow, this difference can significantly impact performance.

In the previous example, the `Sum` column evaluated the [`sum(IntVector)`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sum(io.deephaven.vector.IntVector)) function at each level of the rollup and produced a `long`. Since the original table contains an `int` column, the lowest-level rollup provides an `IntVector` to `sum`, while subsequent levels use a `LongVector`.

Similarly, the original table has a column called `Value`, but after aggregation, the result is labeled as `Sum`. To resolve this discrepancy, the `updateView` method is used before the rollup to rename the `Value` column to `Sum`. If the rename was omitted and the original data was used directly, it would lead to inconsistencies in the results at different rollup levels.

If we ran the same example without the rename:

```python syntax
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven import agg

source = new_table(
    [
        string_col(
            "Key", ["Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"]
        ),
        int_col("Value", [10, 10, 10, 20, 20, 30, 30]),
    ]
)
reaggregated_sum = source.rollup(
    aggs=[agg.formula(formula="Sum = sum(Value)", reaggregating=True)], by=["Key"]
)
```

We instead get an Exception message indicating that the formula cannot be applied properly, because the `Value` column does not exist in the second level of the rollup:

```text
Error running script: io.deephaven.engine.table.impl.select.FormulaCompilationException: Formula compilation error for: sum(Value)
...
Full expression           : sum(Value)
Expression having trouble : 
Exception type            : io.deephaven.engine.table.impl.lang.QueryLanguageParser$ParserResolutionFailure
Exception message         : Cannot find variable or class Value
```

### Formula Depth and Keys

Formula aggregations may include the constant `__FORMULA_DEPTH__` or `__FORMULA_KEYS__` columns. The `__FORMULA_DEPTH__` column is the depth of the formula aggregation in the rollup tree. The root node of the rollup has a depth of 0, the next level is 1, and so on. The `__FORMULA_KEYS__` column is an [`ObjectVector`](https://docs.deephaven.io/core/javadoc/io/deephaven/vector/ObjectVector.html) containing the keys of the rows at the current level of the rollup. The following formulas demonstrate the values of depth and keys:

```python order=depth_and_keys,source
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven import agg

source = new_table(
    [
        string_col(
            "Key", ["Alpha", "Alpha", "Alpha", "Bravo", "Charlie", "Charlie", "Charlie"]
        ),
        string_col(
            "Key2",
            [
                "Apple",
                "Banana",
                "Banana",
                "Coconut",
                "Coconut",
                "Coconut",
                "Dragonfruit",
            ],
        ),
        int_col("Value", [10, 20, 15, 20, 15, 30, 35]),
    ]
)
depth_and_keys = source.rollup(
    aggs=[
        agg.formula(formula="Depth = __FORMULA_DEPTH__"),
        agg.formula(formula="Keys = __FORMULA_KEYS__"),
    ],
    by=["Key", "Key2"],
)
```

These variables can be used to implement distinct aggregations at each level of the rollup. For example:

```python order=first_then_sum,source
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven import agg

source = new_table(
    [
        string_col(
            "Key", ["Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"]
        ),
        int_col("Value", [10, 10, 10, 20, 20, 30, 30]),
    ]
)
first_then_sum = source.rollup(
    aggs=[
        agg.formula(
            formula="Value = __FORMULA_DEPTH__ == 0 ? sum(Value) : first(Value)"
        )
    ],
    by=["Key"],
)
```

In this case, for each value of `Key`, the aggregation returns the first value. For the root level, the aggregation returns the sum of all values. When combined with a reaggregating formula, even more interesting semantics are possible. For example, rather than summing all of the values, we can sum the values from the prior level:

```python order=first_then_sum,source
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven import agg

source = new_table(
    [
        string_col(
            "Key", ["Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"]
        ),
        int_col("Value", [10, 10, 10, 20, 20, 30, 30]),
    ]
)
first_then_sum = source.rollup(
    aggs=[
        agg.formula(
            formula="Value = __FORMULA_DEPTH__ == 0 ? sum(Value) : first(Value)",
            reaggregating=True,
        )
    ],
    by=["Key"],
)
```

Another simple example of reaggregation is a capped sum. In this example, the sums below the root level are capped at 40:

```python order=capped_sum,source
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven import agg

source = new_table(
    [
        string_col(
            "Key", ["Alpha", "Alpha", "Alpha", "Bravo", "Bravo", "Charlie", "Charlie"]
        ),
        int_col("Value", [10, 20, 15, 20, 15, 25, 35]),
    ]
)
capped_sum = source.rollup(
    aggs=[
        agg.formula(
            formula="Value = __FORMULA_DEPTH__ == 0 ? sum(Value) : min(sum(Value), 40)",
            reaggregating=True,
        )
    ],
    by=["Key"],
)
```

In this example, the `__FORMULA_KEYS__` column is similarly used to cap at the `Key` column (using `__FORMULA__DEPTH__ == 1` would be equivalent in this case):

```python order=capped_sum,source
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven import agg

source = new_table(
    [
        string_col(
            "Key", ["Alpha", "Alpha", "Alpha", "Bravo", "Charlie", "Charlie", "Charlie"]
        ),
        string_col(
            "Key2",
            [
                "Apple",
                "Banana",
                "Banana",
                "Coconut",
                "Coconut",
                "Coconut",
                "Dragonfruit",
            ],
        ),
        int_col("Value", [10, 20, 15, 20, 15, 30, 35]),
    ]
)
capped_sum = source.rollup(
    aggs=[
        agg.formula(
            formula="Value = __FORMULA_KEYS__.get(__FORMULA_KEYS__.size() - 1) == `Key` ?  min(sum(Value), 40) : sum(Value)",
            reaggregating=True,
        )
    ],
    by=["Key", "Key2"],
)
```

## Related documentation

- [`avg`](../group-and-aggregate/AggAvg.md)
- [`sum`](../group-and-aggregate/AggSum.md)
- [`empty_table`](./emptyTable.md)
- [`join`](../join/join.md)
- [`time_table`](./timeTable.md)
- [`tree`](./tree.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.rollup)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.html)
