---
title: Select and create columns
---

You will often want to select, create, or modify columns in tables. There are five table operations available for this:

- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)
- [`update`](../reference/table-operations/select/update.md)
- [`updateView`](../reference/table-operations/select/update-view.md)
- [`lazyUpdate`](../reference/table-operations/select/lazy-update.md)

All five operations allow you to address column selection and projection on real-time (updating), static, and snapshotted tables.

In all cases, the syntax follows a mostly similar pattern. All five of the scripts below produce the same resultant table, but how and when the engine manifests the results is different in each case.

```groovy order=source,tableSelect,tableView,tableUpdate,tableUpdateView,tableLazyUpdate
source = newTable(intCol("Column1", 1, 1, 2, 3, 4)).update("Column2 = i*2")

tableSelect = source.select("Column1", "Column2", "NewColumnA = Column1 + Column2^2"
)
tableView = source.view("Column1", "Column2", "NewColumnA = Column1 + Column2^2")

tableUpdate = source.update("NewColumnA = Column1 + Column2^2")

tableUpdateView = source.updateView("NewColumnA = Column1 + Column2^2")

tableLazyUpdate = source.lazyUpdate("NewColumnA = Column1 + Column2^2")
```

Within these selection and update operations you can use [query strings](../how-to-guides/query-string-overview.md) to transform data; do math; use common [operators](../how-to-guides/operators.md), [literals](../how-to-guides/string-char-literals.md), [objects](../reference/query-language/types/objects.md), and [special variables](../reference/query-language/variables/special-variables.md); [cast data types](../reference/query-language/control-flow/casting.md); [parse and manipulate strings](../how-to-guides/strings.md); [handle arrays](../how-to-guides/work-with-arrays.md); [use built-in functions](../how-to-guides/built-in-functions.md); [operate on time](../conceptual/time-in-deephaven.md); [bin data](/core/docs/reference/community-questions/bin-times-specific-time); [introduce ternaries](../how-to-guides/ternary-if-how-to.md); and address other vital use cases by generating a new column, manipulating an existing column, or combining and decorating multiple columns from a table. You can also use Java methods, Groovy, and user-defined and 3rd-party library functions within these table operations.

For example, below is an inelegant, but demonstrative script of using [query strings](../how-to-guides/strings.md#java-strings-in-queries) within an [`update`](../reference/table-operations/select/update.md) operation, as representative of the other four methods. The projections below are just just a taste of what you can do with these operations and query strings.

```groovy order=source,result
// Create a sample table
source = newTable(
        stringCol("X", "A", "B", "C", "D", "E", "F", "G"),
        intCol("Y", 1, -2, 3, -4, 5, -6, 7),
        intCol("Z", 2, 3, 1, 2, 3, 1, 2),
)

// Basic example formula
f = { a, b ->
    return a + b
}

// Demonstrate some projections
result = source.update(
        "ColSpecialChar = i",
        "ColOperator = Y*Z/3",
        "ColCast = (int)ColOperator",
        "ColBuilInFcn = absAvg(Y, Z)",
        "ColFormula = (int)f(Y, Z)",
        "ColTernary = ColCast <= 1 ? pow(ColFormula,2) : pow(ColFormula,3)",
        "ColStringStuff = X + `_hello`",
)
```

## Choose the right column selection method

Your decision about which of the five methods to use largely hinges, case by case, on how you answer the following two questions.

### "Do you want all columns in the original table or only the ones you itemized in your script?"

- [`select`](../reference/table-operations/select/select.md) and [`view`](../reference/table-operations/select/view.md) return only the columns you itemize.
- [`update`](../reference/table-operations/select/update.md), [`updateView`](../reference/table-operations/select/update-view.md), and [`lazyUpdate`](../reference/table-operations/select/lazy-update.md) return all of the original columns of the table, as well as the columns you itemize.

Notice below how `tSelect` and `tView` return only `Column2`, whereas the others return the original `Column1`, as well as the new `Column2`.

```groovy order=source,tSelect,tView,tUpdate,tUpdateView,tLazyUpdate
source = emptyTable(5).update("Column1 = i")

// select() and view() have identical syntax
// These create a table only with Column2
tSelect = source.select("Column2 = Column1^2")
tView = source.view("Column2 = Column1^2")

// update(), updateView(), and lazyUpdate() have identical syntax
// These create a table with the original set of columns (i.e., Column1) plus Column2
tUpdate = source.update("Column2 = Column1^2")
tUpdateView = source.updateView("Column2 = Column1^2")
tLazyUpdate = source.lazyUpdate("Column2 = Column1^2")
```

### "Do you want to write new columns to memory, or calculate them on demand?"

In this context, _on demand_ implies that the new columns are not stored in memory. Rather, a formula is stored that is used to recalculate each cell every time it is accessed by a downstream node in the DAG, client, or other consumer.

- Both [`view`](../reference/table-operations/select/view.md) and [`updateView`](../reference/table-operations/select/update-view.md) handle column calculation in this way. (As noted previously, `updateView` returns the table plus additionally articulated columns, whereas `view` returns only the selected columns.)
- [`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md) calculate columns incrementally, writing the results to memory.
- [`view`](../reference/table-operations/select/view.md), [`updateView`](../reference/table-operations/select/update-view.md), and [`lazyUpdate`](../reference/table-operations/select/lazy-update.md) will calculate new columns' cells only on-demand.

It is recommended you use [`updateView`](../reference/table-operations/select/update-view.md) or [`view`](../reference/table-operations/select/view.md) when your use case suggests one, some, or all of the following are true:

1. the column formula is fast to compute,
2. only a small portion of the set of rows is being accessed,
3. cells are accessed very few times, or
4. memory usage must be minimized.

For other cases, consider using `select` or `update`. ([`lazyUpdate`](../reference/table-operations/select/lazy-update.md) is more rarely used than the other four.)

> [!CAUTION]
> When using [`view`](../reference/table-operations/select/view.md) or [`updateView`](../reference/table-operations/select/update-view.md), non-deterministic methods (e.g., random numbers, current time, or mutable structures) produce _unstable_ results. Downstream operations on these results produce _undefined_ behavior. Non-deterministic methods should use [`select`](../reference/table-operations/select/select.md) or [`update`](../reference/table-operations/select/update.md) instead.

Extreme cases #1-4 from the note above are easy to determine. The challenge in determinging the fitness of `updateView` or `view` (over their `update` or `select` counterparts) comes when use case conditions are less obvious. Here are some good rules of thumb for when to prefer the `updateView` or `view` over the alternatives:

- The table is the last node in the DAG and it isn't obvious how the calculated column will be used.
- The table updates in real-time, the new column will therefore be ticking, and clients/consumers thereof will be sporadic.
- The table is big, the column is intermediate and only used once in downstream calculations.
- The only consumers of the new column are humans using GUIs (and therefore restricted to the relatively small scale of viewports in their UI).

> [!NOTE]
> Further reading
> An example exists in a [blog post](/blog/2024/02/12/group-ungroup#compute-data-immediately-or-as-needed) that shows the performance differences in suporting just a UI versus a downstream DAG calculation for a table with 135 million rows. It articulates the performance of respective selection method choices.

## When to use `lazyUpdate`

The [`lazyUpdate`](../reference/table-operations/select/lazy-update.md) method creates a new table containing a new cached formula column for each argument.

Similar to `updateView`, with `lazyUpdate` column formulas are computed on-demand, deferring computation and memory until it is required.

When performing a `lazyUpdate`, cell values are stored in memory in a cache. Because results are cached (memoized) for the set of input values, the same input values will never be computed twice. Existing results are referenced without additional memory allocation. This improves performance when the number of distinct inputs are low relative to the number of rows in the table.

> [!NOTE]
> The syntax for the `lazyUpdate`, [`updateView`](../reference/table-operations/select/update-view.md), and [`update`](../reference/table-operations/select/update.md) methods is identical, as is the resulting table.
>
> `lazyUpdate` is recommended for small sets of unique input values. In this case, `lazyUpdate` uses less memory than `update` and requires less computation than `updateView`. However, if there are many unique input values, `update` will be more efficient because `lazyUpdate` stores the formula inputs and result in a map, whereas `update` stores the values more compactly in an array.

Here is an example of `lazyUpdate`. Because only it contains two values exist in column C (2 and 5), sqrt(2) is computed exactly one time, and sqrt(5) is computed exactly one time. The values are cached for future use, so the subsequent two calculations of sqrt(5) are free from a compute and memory allocation perspective. This is most appropriately used when the distinct set of computational results is small relative to the row count.

```groovy order=source,result
source = newTable(
        stringCol("A", "The", "At", "Is", "On"),
        intCol("B", 1, 2, 3, 4),
        intCol("C", 5, 2, 5, 5),
)

result = source.lazyUpdate("Y = sqrt(C)")
```

# Summary of selection methods

The following table showcases Deephaven's five selection methods and provides a quick visual reference for the differences between them.

<table className="text--center">
  <thead>
    <tr>
      <th colSpan="1"></th>
      <th colSpan="2">Source columns in new table</th>
      <th colSpan="3">New column type</th>
    </tr>
    <tr>
      <th>Method Name</th>
      <th>Subset</th>
      <th>All</th>
      <th>In-memory</th>
      <th>Formula</th>
      <th>Memoized</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td scope="row" ><a href="../reference/table-operations/select/select.md">select</a></td>
      <td><Check/></td>
      <td><RedX/></td>
      <td><Check/></td>
      <td><RedX/></td>
      <td><RedX/></td>
    </tr>
    <tr>
      <td scope="row" ><a href="../reference/table-operations/select/view.md">view</a></td>
      <td><Check/></td>
      <td><RedX/></td>
      <td><RedX/></td>
      <td><Check/></td>
      <td><RedX/></td>
    </tr>
    <tr>
      <td scope="row" ><a href="../reference/table-operations/select/update.md">update</a></td>
      <td><RedX/></td>
      <td><Check/></td>
      <td><Check/></td>
      <td><RedX/></td>
      <td><RedX/></td>
    </tr>
    <tr>
      <td scope="row" ><a href="../reference/table-operations/select/update-view.md">update_view</a></td>
      <td><RedX/></td>
      <td><Check/></td>
      <td><RedX/></td>
      <td><Check/></td>
      <td><RedX/></td>
    </tr>
    <tr>
      <td scope="row" ><a href="../reference/table-operations/select/lazy-update.md">lazy_update</a></td>
      <td><RedX/></td>
      <td><Check/></td>
      <td><RedX/></td>
      <td><RedX/></td>
      <td><Check/></td>
    </tr>
  </tbody>
</table>

## Related documentation

- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)
- [`update`](../reference/table-operations/select/update.md)
- [`updateView`](../reference/table-operations/select/update-view.md)
- [`lazyUpdate`](../reference/table-operations/select/lazy-update.md)
- [Performance examples](/blog/2024/02/12/group-ungroup#compute-data-immediately-or-as-needed)
