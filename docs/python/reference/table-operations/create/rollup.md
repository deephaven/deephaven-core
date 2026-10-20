---
title: rollup
---

The Deephaven `rollup` method creates a [rollup table](../../../how-to-guides/rollup-table.md) from a source table with zero or more [aggregations](../group-and-aggregate/aggBy.md) and zero or more grouping columns to create a hierarchy.

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

A list of aggregations. If `None` or empty, no aggregations are performed.

The following aggregations are supported:

- [`abs_sum`](../group-and-aggregate/AggAbsSum.md)
- [`avg`](../group-and-aggregate/AggAvg.md)
- [`count_`](../group-and-aggregate/AggCount.md)
- [`count_distinct`](../group-and-aggregate/AggCountDistinct.md)
- [`count_where`](../group-and-aggregate/AggCountWhere.md)
- [`distinct`](../group-and-aggregate/AggDistinct.md)
- [`first`](../group-and-aggregate/AggFirst.md)
- [`formula`](../group-and-aggregate/AggFormula.md): supported without `formula_param`. The `formula_param` form is not supported.
- [`group`](../group-and-aggregate/AggGroup.md)
- [`last`](../group-and-aggregate/AggLast.md)
- [`max_`](../group-and-aggregate/AggMax.md)
- [`min_`](../group-and-aggregate/AggMin.md)
- [`sorted_first`](../group-and-aggregate/AggSortedFirst.md)
- [`sorted_last`](../group-and-aggregate/AggSortedLast.md)
- [`std`](../group-and-aggregate/AggStd.md)
- [`sum_`](../group-and-aggregate/AggSum.md)
- [`unique`](../group-and-aggregate/AggUnique.md)
- [`var`](../group-and-aggregate/AggVar.md)
- [`weighted_avg`](../group-and-aggregate/AggWAvg.md)
- [`weighted_sum`](../group-and-aggregate/AggWSum.md)

A rollup of a [blink table](../../../conceptual/table-types.md#specialization-3-blink) does not support `first`, `last`, `sorted_first`, `sorted_last`, `group`, or `formula`.

</Param>
<Param name="by" optional type="list[str]">

Zero or more column names to group on and create a hierarchy from. If `None`, no hierarchy is created.

</Param>
<Param name="include_constituents" optional type="bool">

Whether or not to include constituent rows at the leaf level. Default is `False`. A [blink table](../../../conceptual/table-types.md#specialization-3-blink) source does not support `include_constituents=True`.

</Param>
</ParamTable>

## Methods

### Instance

- [`with_filters(filters)`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.RollupTable.with_filters) - Create a new rollup table that applies a set of filters. If every filter uses only group-by columns, Deephaven filters the lowest aggregated level and reaggregates. Otherwise, it applies the filters to the source table and rebuilds the rollup. Filters may use group-by and constituent columns but may not use columns that are inputs to an aggregation, and may not use column arrays.
- `with_update_view(formulas)` - Create a new rollup table with additional columns, computed by [`update_view`](../select/update-view.md), at the aggregated node level.
- `node_operation_recorder(node_type)` - Get a [`recorder`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.RollupNodeOperationsRecorder) for per-node operations to apply during snapshots of the requested [`NodeType`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.NodeType).
- `with_node_operations(recorders)` - Create a new rollup table that applies the [`recorded`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.RollupNodeOperationsRecorder) operations to nodes when gathering snapshots.

### Read rollup attributes

- `aggs` - The aggregations performed in the `rollup` operation.
- `by` - The group-by columns used for the `rollup` operation.
- `include_constituents` - Whether the constituent rows at the lowest level are included.

## Returns

A rollup table.

## Examples

The following example creates a rollup table from a source table of insurance expense data. The aggregated average of the `bmi` and `expenses` columns is calculated, then the table is rolled up by the `region` and `age` columns.

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

Similar to the previous example, this example creates a rollup table from a source table of insurance expense data and then filters it with [`with_filters`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.RollupTable.with_filters). The filter uses only the group-by column `age`, so Deephaven applies it to the lowest aggregated level and reaggregates. A filter can use group-by and constituent columns, but not columns that are inputs to an aggregation, and not column arrays.

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

## Use formula aggregations in rollups

A [formula aggregation](../group-and-aggregate/AggFormula.md) evaluates its formula for every changed row of the output table on each tick. Its input [vectors](../../../conceptual/arrays-vs-vectors.md) can be large: at the root level, they hold every row of the source table. A formula that is slow on large vectors slows the rollup.

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

The sum for the root row reads every row of the source table. The engine reports row-level changes (added, removed, modified, and shifted rows), but not changes inside a vector, because a vector sits within a single row. When the table ticks, the engine therefore re-evaluates the whole formula.

### Reaggregate formulas

Formula reaggregation limits the size of input vectors while the engine evaluates changes to a rollup. The formula must apply at every level of the rollup and return the same type at each level.

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

If a new row with the key `Delta` is added to the source, `simple_sum` reads all eight rows again to recalculate the sums. `reaggregated_sum` recalculates only the sum for `Delta`, then reads the intermediate sums for `Alpha`, `Bravo`, `Charlie`, and `Delta`, not all rows. As the number of keys and the size of the data grow, this difference can significantly affect performance.

In the previous example, the `Sum` column is the output of the [`sum(IntVector)`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sum(io.deephaven.vector.IntVector)) function, which returns a `long`. At the lowest level of the rollup, `sum` receives an [`IntVector`](https://docs.deephaven.io/core/javadoc/io/deephaven/vector/IntVector.html) from the `int` source column. At each level above, it receives a [`LongVector`](https://docs.deephaven.io/core/javadoc/io/deephaven/vector/LongVector.html) of the results below.

At the lowest level, a formula reads source columns. At every level above, a reaggregating formula can read only the previous level's result columns. The formula must therefore read `Sum`, the column it also writes. The [`update_view`](../select/update-view.md) call adds a `Sum` column that copies `Value` for this purpose.

Without the [`update_view`](../select/update-view.md) call, the formula reads `Value`:

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

This raises an exception, because the `Value` column does not exist at the levels above the lowest level of the rollup:

```text
Error running script: io.deephaven.engine.table.impl.select.FormulaCompilationException: Formula compilation error for: sum(Value)
...
Full expression           : sum(Value)
Expression having trouble : 
Exception type            : io.deephaven.engine.table.impl.lang.QueryLanguageParser$ParserResolutionFailure
Exception message         : Cannot find variable or class Value
```

### Use formula depth and keys

Formula aggregations may include the constant `__FORMULA_DEPTH__` or `__FORMULA_KEYS__` columns. The `__FORMULA_DEPTH__` column is the depth of the formula aggregation in the rollup tree. The root node of the rollup has a depth of 0, the next level is 1, and so on. The `__FORMULA_KEYS__` column is an [`ObjectVector`](https://docs.deephaven.io/core/javadoc/io/deephaven/vector/ObjectVector.html) of `String` containing the names of the group-by columns in effect at the current level of the rollup: empty at the root, `["Key"]` at depth 1, and `["Key", "Key2"]` at depth 2 in the examples below. The following formulas demonstrate the values of depth and keys:

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

At the root, `Depth` is 0 and `Keys` is empty. At each node for a `Key` value, `Depth` is 1 and `Keys` is `["Key"]`. At each node for a `Key2` value within a `Key`, `Depth` is 2 and `Keys` is `["Key", "Key2"]`.

These variables can implement distinct aggregations at each level of the rollup. For example:

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

In this case, for each value of `Key`, the aggregation returns the first value. For the root level, the aggregation returns the sum of all values. A reaggregating formula adds further semantics. For example, rather than summing all of the values, the formula can sum the values from the prior level:

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

Another example of reaggregation is a capped sum, in which the sums below the root level are capped at 40:

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

Alpha sums to 45 and is capped at 40, Bravo sums to 35, Charlie sums to 60 and is capped at 40, and the root row sums the three capped values: 115.

The following capped sum uses `__FORMULA_KEYS__` instead of `__FORMULA_DEPTH__` to cap the sums at the `Key` column. In this case, `__FORMULA_DEPTH__ == 1` would be equivalent:

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

- [Rollup table](../../../how-to-guides/rollup-table.md)
- [`avg`](../group-and-aggregate/AggAvg.md)
- [`sum_`](../group-and-aggregate/AggSum.md)
- [`formula`](../group-and-aggregate/AggFormula.md)
- [`group`](../group-and-aggregate/AggGroup.md)
- [`update_view`](../select/update-view.md)
- [`new_table`](./newTable.md)
- [`empty_table`](./emptyTable.md)
- [`join`](../join/join.md)
- [`time_table`](./timeTable.md)
- [`tree`](./tree.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.rollup)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.html)
