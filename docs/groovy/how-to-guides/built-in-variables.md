---
title: Built-in query language variables
---

Deephaven provides three built-in query language variables that refer to rows in a table:

- `i` is a 32-bit `int` row position.
- `ii` is a 64-bit `long` row position.
- `k` is a 64-bit `long` [row key](../conceptual/table-update-model.md#describing-table-updates). A row key is the engine's internal address for a row, not its position.

Combined with the [underscore operator](./work-with-arrays.md#using-the-underscore-operator), `i` and `ii` also read values from the previous and next rows.

On [refreshing tables](../conceptual/table-types.md), the engine restricts where you can use these variables. [Refreshing table restrictions](#refreshing-table-restrictions) lists the safe cases, and [Alternatives for refreshing tables](#alternatives-for-refreshing-tables) shows what to use instead.

## Use the variables in a query

The following code block creates a table with [`emptyTable`](../reference/table-operations/create/emptyTable.md) and shows how to use `i`, `ii`, and `k` in a query:

```groovy test-set=1 order=source
source = emptyTable(10).update(
    "RowPosition32Bit = i", "RowPosition64Bit = ii", "RowKey = k"
)
```

An `int` holds values only up to 2,147,483,647, so a formula that uses `i` throws an error on any row position past that limit. Use `ii` unless you need to pass an `int` to another function.

In `source`, [row keys](../conceptual/table-update-model.md#describing-table-updates) happen to equal row positions, so all three columns match. A [filter](./filters.md) shows the difference: [`where`](../reference/table-operations/filter/where.md) keeps each row's key from `source`, but positions start again at 0.

```groovy test-set=1 order=filtered
filtered = source.where("RowPosition64Bit % 2 == 0").update("Position = ii", "Key = k")
```

> [!WARNING]
> Because `k` is a [row key](../conceptual/table-update-model.md#describing-table-updates), not a row position, use it only in limited circumstances, such as debugging.

### Read values from other rows

To read values from other rows, combine `i` or `ii` with the [underscore operator](./work-with-arrays.md#using-the-underscore-operator). `Column_` is the column array variable for the column `Column`. `Column_[ii-1]` reads the value of `Column` in the previous row, and `Column_[ii+1]` reads it in the next row. An offset before the first row or after the last row returns null.

The following code block uses [`update`](../reference/table-operations/select/update.md) to add the previous and next values of `RowPosition64Bit` to the `source` table:

```groovy test-set=1 order=prevNext
prevNext = source.update(
    "PrevRow = RowPosition64Bit_[ii-1]", "NextRow = RowPosition64Bit_[ii+1]"
)
```

## Refreshing table restrictions

The engine checks every formula that the following operations run on a [refreshing table](../conceptual/table-types.md):

- [`update`](../reference/table-operations/select/update.md)
- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)
- [`updateView`](../reference/table-operations/select/update-view.md)
- [`lazyUpdate`](../reference/table-operations/select/lazy-update.md)

If the formula uses `i`, `ii`, `k`, or a [column array variable](./work-with-arrays.md#using-the-underscore-operator) such as `Column_` in a way the table below marks unsafe, the operation throws an `IllegalArgumentException`.

The engine does not check [filters](./filters.md) passed to [`where`](../reference/table-operations/filter/where.md). In a [`where`](../reference/table-operations/filter/where.md) filter on a [refreshing table](../conceptual/table-types.md), use [column arrays](./work-with-arrays.md#using-the-underscore-operator) only as constant offsets. Don't rely on `i`, `ii`, or `k` there either, because their results go stale as rows change.

The following table shows which usages are safe on [static](../conceptual/table-types.md#static-tables), [append-only](../conceptual/table-types.md#specialization-1-append-only), [add-only](../conceptual/table-types.md#specialization-2-add-only), and [blink](../conceptual/table-types.md#specialization-3-blink) tables:

| Usage                                                    | Safe on                                                                                                                                                                                                                                                          | Throws error on                                                                                                                                                                                                                    |
| -------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `i`, `ii`                                                | [static](../conceptual/table-types.md#static-tables), [append-only](../conceptual/table-types.md#specialization-1-append-only), [blink](../conceptual/table-types.md#specialization-3-blink)                                                                     | other [refreshing tables](../conceptual/table-types.md), including [add-only](../conceptual/table-types.md#specialization-2-add-only) tables that are not [append-only](../conceptual/table-types.md#specialization-1-append-only) |
| `k`                                                      | [static](../conceptual/table-types.md#static-tables), [append-only](../conceptual/table-types.md#specialization-1-append-only), [add-only](../conceptual/table-types.md#specialization-2-add-only), [blink](../conceptual/table-types.md#specialization-3-blink) | other [refreshing tables](../conceptual/table-types.md)                                                                                                                                                                            |
| Simple constant offset (`Column_[i-1]`, `Column_[ii+1]`) | all tables                                                                                                                                                                                                                                                       | —                                                                                                                                                                                                                                  |
| Other column array usage (`Column_[(i)-1]`, `Column_`)   | [static](../conceptual/table-types.md#static-tables), [blink](../conceptual/table-types.md#specialization-3-blink)                                                                                                                                               | other [refreshing tables](../conceptual/table-types.md), including [append-only](../conceptual/table-types.md#specialization-1-append-only)                                                                                        |

> [!NOTE]
> The engine treats an array index as a constant offset only in these two forms:
>
> - `i` or `ii` alone, such as `Column_[ii]`.
> - `i` or `ii` followed by `+` or `-` and a whole-number literal, such as `Column_[ii-1]` or `Column_[i+2]`.
>
> Any other index, such as `Column_[(i)-1]`, `Column_[1+i]`, or `Column_[ii-1L]`, falls under "Other column array usage" in the table above.

Unsafe usage throws an error like this:

```text
IllegalArgumentException: Formula '<formula>' uses i, ii, k, or column array variables,
and is not safe to refresh. Note that some usages, such as on an append-only table are safe.
To allow unsafe refreshing formulas, set the system property
io.deephaven.engine.table.impl.select.AbstractFormulaColumn.allowUnsafeRefreshingFormulas.
```

> [!WARNING]
> Turning off this check lets these formulas run on any [refreshing table](../conceptual/table-types.md), and their results can be wrong after rows are added, removed, modified, or shifted.
>
> To turn it off anyway, set `AbstractFormulaColumn.allowUnsafeRefreshingFormulas` to `true`, for example with the JVM argument `-DAbstractFormulaColumn.allowUnsafeRefreshingFormulas=true`. The fully qualified name in the error message has no effect.

## Alternatives for refreshing tables

Constant offsets such as `Column_[ii-1]` already work on [refreshing tables](../conceptual/table-types.md) when written in the [exact form the engine recognizes](#refreshing-table-restrictions). For other positional access on a [refreshing table](../conceptual/table-types.md), such as the previous value within each group or a whole-column `Column_` reference, use one of these approaches:

- [`updateBy`](#update-by) computes row-based windows within each group and updates only the windows that a change affects.
- The [group, update, and ungroup](#group-update-and-ungroup) pattern works on any table, but on a [refreshing table](../conceptual/table-types.md) it causes tick expansion. With tick expansion, a change to one input row reports every row in its group as modified.
- An [as-of join](#as-of-joins) works when a column such as a timestamp or sequence number reliably orders the rows.

Which approach is fastest depends on your data, so benchmark each one before you choose.

If you want to work with each group as its own table, you can also [partition](#partition-the-table) the table first and apply any of these approaches to each partition.

The examples in this section use a [keyed input table](./input-tables.md#create-a-keyed-input-table) as the refreshing source. You can modify and remove rows in a [keyed input table](./input-tables.md#create-a-keyed-input-table), so it is neither [append-only](../conceptual/table-types.md#specialization-1-append-only) nor [add-only](../conceptual/table-types.md#specialization-2-add-only). The engine therefore rejects formulas that use `i`, `ii`, or `k` on it in any of the operations listed under [Refreshing table restrictions](#refreshing-table-restrictions).

Each example computes `PrevValue`, the previous `Value` in the same `Group`.

```groovy test-set=2 order=refreshingSource
import io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable

sourceData = emptyTable(9).update(
    "Key = i", "Group = i % 3", "Timestamp = (long)i", "Value = i * 10"
)

// A keyed input table is a refreshing table whose rows can be modified and removed
refreshingSource = KeyedArrayBackedInputTable.make(sourceData, "Key")
```

Because `refreshingSource` is neither [append-only](../conceptual/table-types.md#specialization-1-append-only) nor [blink](../conceptual/table-types.md#specialization-3-blink), a formula that uses `ii` on it throws an error:

```groovy syntax
// Throws IllegalArgumentException: Formula 'ii' uses i, ii, k, or column array variables, and is not safe to refresh. ...
position = refreshingSource.update("Position = ii")
```

### Update by

[`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) computes row-based windows within each group and updates them incrementally as rows are added, modified, or removed. It does not need an ordering column, and it avoids the tick expansion of [group, update, and ungroup](#group-update-and-ungroup).

The following example uses [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md) with a reverse window of `2` and a forward window of `-1`. Together, these settings define a window that contains only the previous row in the group:

```groovy test-set=2 order=updateByResult
updateByResult = refreshingSource.updateBy(RollingFormula(2, -1, "PrevValue = first(Value)"), "Group")
```

The first row of each group has no previous row, so its window is empty and [`first`](../reference/query-language/query-library/auto-imported/basic.md) returns null.

### Group, update, and ungroup

[Group](../reference/table-operations/group-and-aggregate/groupBy.md) the data, perform the [update](../reference/table-operations/select/update.md) operation within each group, then [ungroup](../reference/table-operations/group-and-aggregate/ungroup.md). Because the formula works on each group's array, it references values within the group without relying on absolute row positions.

```groovy test-set=2 order=result
// Group by the grouping column and compute previous values using array operations
// Prepend null to match array sizes for ungroup
grouped = refreshingSource.groupBy("Group").update(
    "PrevValue = concat(new int[]{NULL_INT}, Value.subVector(0, Value.size() - 1).toArray())"
)

// Ungroup to work with individual rows
result = grouped.ungroup()
```

After [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md), each `Value` cell holds the array of that group's values. The formula drops the last element with [`subVector`](./work-with-arrays.md#slice-arrays) and prepends [`NULL_INT`](./built-in-constants.md#null-constants), so each element of `PrevValue` is the previous element of `Value`. [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) then expands every array column back into rows, including `Value` and `PrevValue`. The first row of each group gets a null `PrevValue`.

Because [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) expands each group in place, `result` lists the rows group by group rather than in the source table's order.

> [!NOTE]
> This is where the tick expansion described above comes from: a change to one row modifies that row's group in the grouped table, so [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) reports every row in the group as modified.

### As-of joins

If a column such as a timestamp or sequence number reliably orders the rows, use an [as-of join](../reference/table-operations/join/aj.md) on that column instead of row position.

```groovy test-set=2 order=ajResult
// Join to get the previous value within each group based on timestamp
ajResult = refreshingSource.aj(refreshingSource, "Group, Timestamp > Timestamp", "PrevValue = Value")
```

The [`aj`](../reference/table-operations/join/aj.md) matches `Group` exactly. `Timestamp > Timestamp` then picks the row in the same group with the largest `Timestamp` that is less than the current row's. That row's `Value` becomes `PrevValue`, the previous `Value` in the group. The first row of each group has no earlier row, so its `PrevValue` is null.

### Partition the table

Use this approach if your data is already a [partitioned table](./partitioned-tables.md), or if you want each group as its own table. In the following example, [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md) creates one constituent table, or partition, for each `Group` value.

[Partitioning](../reference/table-operations/group-and-aggregate/partitionBy.md) does not lift the [refreshing table restrictions](#refreshing-table-restrictions), because the engine checks each partition as its own table:

- `i` and `ii` work in a partition that is [static](../conceptual/table-types.md#static-tables), [append-only](../conceptual/table-types.md#specialization-1-append-only), or [blink](../conceptual/table-types.md#specialization-3-blink).
- In any other partition, use [`updateBy`](#update-by), the [group, update, and ungroup](#group-update-and-ungroup) pattern, or an [as-of join](#as-of-joins), as the following example does.

In the example:

- [`transform`](../reference/table-operations/partitioned-tables/transform.md) applies an [`aj`](../reference/table-operations/join/aj.md) to each partition. Each partition holds a single `Group` value, so the [`aj`](../reference/table-operations/join/aj.md) matches only on `Timestamp`.
- The `prevValue` closure opens an [execution context](../conceptual/execution-context.md) because the table operations inside it need one, and [`transform`](../reference/table-operations/partitioned-tables/transform.md) doesn't pass the console's execution context to the closure.
- [`merge`](../reference/table-operations/partitioned-tables/merge.md) combines the results into one table.

```groovy test-set=2 order=partitionedResult
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

// Partition by a grouping column to create multiple tables
partitioned = refreshingSource.partitionBy("Group")

defaultCtx = ExecutionContext.getContext()

// Find the previous value within one partition
prevValue = { t ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return t.aj(t, "Timestamp > Timestamp", "PrevValue = Value")
    }
}

partitionedResult = partitioned.transform(prevValue).merge()
```

## Related documentation

- [Table types](../conceptual/table-types.md)
- [Table update model](../conceptual/table-update-model.md)
- [Execution context](../conceptual/execution-context.md)
- [Built-in constants](./built-in-constants.md)
- [Built-in functions](./built-in-functions.md)
- [Query string overview](./query-string-overview.md)
- [Filters](./filters.md)
- [Formulas](./formulas.md)
- [Work with arrays](./work-with-arrays.md)
- [Group and ungroup data](./grouping-data.md)
- [Rolling aggregations](./rolling-aggregations.md)
- [Inexact, time-series, and range joins](./joins-timeseries-range.md)
- [Input tables](./input-tables.md)
- [Partitioned tables](./partitioned-tables.md)
- [Special variables and constants](../reference/query-language/variables/special-variables.md)
- [`aj`](../reference/table-operations/join/aj.md)
- [`concat`](../reference/query-language/query-library/auto-imported/basic.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`first`](../reference/query-language/query-library/auto-imported/basic.md)
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`InputTable`](../reference/table-operations/create/InputTable.md)
- [`lazyUpdate`](../reference/table-operations/select/lazy-update.md)
- [`merge`](../reference/table-operations/partitioned-tables/merge.md)
- [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md)
- [`select`](../reference/table-operations/select/select.md)
- [`transform`](../reference/table-operations/partitioned-tables/transform.md)
- [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md)
- [`update`](../reference/table-operations/select/update.md)
- [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md)
- [`updateView`](../reference/table-operations/select/update-view.md)
- [`view`](../reference/table-operations/select/view.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`ExecutionContext` Javadoc](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html)
- [`KeyedArrayBackedInputTable` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedArrayBackedInputTable.html)
- [`SafeCloseable` Javadoc](/core/javadoc/io/deephaven/util/SafeCloseable.html)
