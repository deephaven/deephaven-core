---
title: Select and create columns
---

You often need to select, create, or modify columns in a table. Deephaven has five table operations for this:

- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)
- [`update`](../reference/table-operations/select/update.md)
- [`update_view`](../reference/table-operations/select/update-view.md)
- [`lazy_update`](../reference/table-operations/select/lazy-update.md)

All five operations work on real-time (updating), static, and [snapshotted](../reference/table-operations/snapshot/snapshot.md) tables.

All five operations take the same kind of [formula](./formulas.md) arguments, but [`select`](../reference/table-operations/select/select.md) and [`view`](../reference/table-operations/select/view.md) must also list the source columns to keep. The five queries below produce the same table. They differ in when the engine computes the results and whether it stores them. To pick one, see [Choose the right column selection method](#choose-the-right-column-selection-method).

```python order=source,table_select,table_view,table_update,table_update_view,table_lazy_update
from deephaven import new_table
from deephaven.column import int_col

source = new_table([int_col("Column1", [1, 1, 2, 3, 4])]).update(
    formulas=["Column2 = i * 2"]
)

table_select = source.select(
    formulas=["Column1", "Column2", "NewColumnA = Column1 + Column2 * Column2"]
)
table_view = source.view(
    formulas=["Column1", "Column2", "NewColumnA = Column1 + Column2 * Column2"]
)

table_update = source.update(formulas=["NewColumnA = Column1 + Column2 * Column2"])
table_update_view = source.update_view(
    formulas=["NewColumnA = Column1 + Column2 * Column2"]
)

table_lazy_update = source.lazy_update(
    formulas=["NewColumnA = Column1 + Column2 * Column2"]
)
```

## Use query strings in column formulas

In all five operations, each [formula](./formulas.md) is a [query string](./query-string-overview.md). You can use query strings to:

- Transform data and do math.
- Use common [operators](./operators.md), [literals](./query-string-overview.md#literals), objects, and [special variables](../reference/query-language/variables/special-variables.md).
- [Cast data types](../reference/query-language/control-flow/casting.md).
- [Parse and manipulate strings](./work-with-strings.md).
- [Handle arrays](./work-with-arrays.md).
- [Use built-in functions](./built-in-functions.md).
- [Operate on time](../conceptual/time-in-deephaven.md).
- [Bin data](./downsampling.md).
- [Introduce ternaries](./ternary-if-how-to.md).

You can also call [Java methods](./java-classes.md), [Python functions](./python-functions.md), and [third-party library functions](./install-and-use-python-packages.md) within these table operations.

The following script uses query strings in an [`update`](../reference/table-operations/select/update.md) operation. The same formulas work in the other four operations.

```python order=source,result
# Create a sample table
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("X", ["A", "B", "C", "D", "E", "F", "G"]),
        int_col("Y", [1, -2, 3, -4, 5, -6, 7]),
        int_col("Z", [2, 3, 1, 2, 3, 1, 2]),
    ]
)


# A user-defined function used in a formula
def f(a, b):
    return a * b


# Demonstrate some formulas
result = source.update(
    formulas=[
        "ColSpecialVar = i",
        "ColOperator = Y * Z / 3",
        "ColCast = (int)ColOperator",
        "ColBuiltInFcn = absAvg(Y, Z)",
        "ColFormula = (int)f(Y, Z)",
        "ColTernary = ColCast <= 1 ? pow(ColFormula, 2) : pow(ColFormula, 3)",
        "ColStringStuff = X + `_hello`",
    ]
)
```

Each column name says which feature its formula shows. Dividing two `int` values returns a `double`, so `ColOperator` is a `double` column, and `ColCast` casts it back to `int`. These formulas are just a taste of what you can do with query strings.

## Choose the right column selection method

Which of the five operations you use depends on two choices:

- Whether the new table keeps all of the source columns or only the ones you list.
- How the engine produces the new column's values. There are three column types:
  - An _in-memory column_ computes its values in advance and stores them.
  - A _formula column_ stores only the formula. It computes a cell each time a consumer, such as a client or a downstream table, reads it.
  - A _memoized column_ computes a cell the first time a consumer reads it, then caches the result.

The following table shows each operation's behavior. The subsections below explain each choice.

<table className="text--center">
  <thead>
    <tr>
      <th colSpan="1"></th>
      <th colSpan="2">Source columns in new table</th>
      <th colSpan="3">New column type</th>
    </tr>
    <tr>
      <th>Operation</th>
      <th>Subset</th>
      <th>All</th>
      <th>In-memory</th>
      <th>Formula</th>
      <th>Memoized</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td scope="row" ><a href="../reference/table-operations/select/select.md"><code>select</code></a></td>
      <td><Check/></td>
      <td><RedX/></td>
      <td><Check/></td>
      <td><RedX/></td>
      <td><RedX/></td>
    </tr>
    <tr>
      <td scope="row" ><a href="../reference/table-operations/select/view.md"><code>view</code></a></td>
      <td><Check/></td>
      <td><RedX/></td>
      <td><RedX/></td>
      <td><Check/></td>
      <td><RedX/></td>
    </tr>
    <tr>
      <td scope="row" ><a href="../reference/table-operations/select/update.md"><code>update</code></a></td>
      <td><RedX/></td>
      <td><Check/></td>
      <td><Check/></td>
      <td><RedX/></td>
      <td><RedX/></td>
    </tr>
    <tr>
      <td scope="row" ><a href="../reference/table-operations/select/update-view.md"><code>update_view</code></a></td>
      <td><RedX/></td>
      <td><Check/></td>
      <td><RedX/></td>
      <td><Check/></td>
      <td><RedX/></td>
    </tr>
    <tr>
      <td scope="row" ><a href="../reference/table-operations/select/lazy-update.md"><code>lazy_update</code></a></td>
      <td><RedX/></td>
      <td><Check/></td>
      <td><RedX/></td>
      <td><RedX/></td>
      <td><Check/></td>
    </tr>
  </tbody>
</table>

### Keep all source columns or only the ones you list

- [`select`](../reference/table-operations/select/select.md) and [`view`](../reference/table-operations/select/view.md) return only the columns you list.
- [`update`](../reference/table-operations/select/update.md), [`update_view`](../reference/table-operations/select/update-view.md), and [`lazy_update`](../reference/table-operations/select/lazy-update.md) return all of the original columns of the table, as well as the columns you list.

Notice below how `t_select` and `t_view` return only `Column2`, whereas the others return the original `Column1`, as well as the new `Column2`.

```python order=source,t_select,t_view,t_update,t_update_view,t_lazy_update
from deephaven import empty_table

source = empty_table(5).update(formulas=["Column1 = i"])

# select() and view() have identical syntax
# These create a table only with Column2
t_select = source.select(formulas=["Column2 = Column1 * Column1"])
t_view = source.view(formulas=["Column2 = Column1 * Column1"])

# update(), update_view(), and lazy_update() have identical syntax
# These create a table with the original set of columns (i.e., Column1) plus Column2
t_update = source.update(formulas=["Column2 = Column1 * Column1"])
t_update_view = source.update_view(formulas=["Column2 = Column1 * Column1"])
t_lazy_update = source.lazy_update(formulas=["Column2 = Column1 * Column1"])
```

To modify an existing column, give the formula that column's name. For example, `source.update(formulas=["Column1 = Column1 * 2"])` replaces `Column1` with the doubled values.

### Store new columns in memory or calculate them on demand

Each operation creates one of the three column types defined [above](#choose-the-right-column-selection-method). A column's consumers include clients and downstream tables in the [DAG](../conceptual/dag.md), the graph of dependent table operations.

- [`view`](../reference/table-operations/select/view.md) and [`update_view`](../reference/table-operations/select/update-view.md) create _formula columns_, which store only their formula and compute values on demand, recalculating a cell each time a consumer reads it.
- [`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md) create _in-memory columns_. They calculate the values and write them to memory, then, on each [update cycle](../conceptual/table-update-model.md), recalculate only the rows that were added or whose input columns changed.
- [`lazy_update`](../reference/table-operations/select/lazy-update.md) creates _memoized columns_. It also computes cells on demand, but caches each result by its input values, so it does not recompute a repeated input. See [Cache results with `lazy_update`](#cache-results-with-lazy_update).

Use [`update_view`](../reference/table-operations/select/update-view.md) or [`view`](../reference/table-operations/select/view.md) when one or more of the following is true:

1. The formula is fast to compute.
2. Consumers read only a small fraction of the rows.
3. Consumers read each cell only a few times.
4. You need to minimize memory use.

These conditions aren't always obvious. Prefer [`update_view`](../reference/table-operations/select/update-view.md) or [`view`](../reference/table-operations/select/view.md) in situations like these, which usually meet condition 2 or 3:

- The table is the last node in the [DAG](../conceptual/dag.md) and it isn't obvious how consumers use the calculated column.
- The table updates in real time, so the new column's values change often, but clients and other consumers read it only occasionally.
- The table is big, and the column is an intermediate result used only once in downstream calculations.
- The only consumers of the new column are people using a GUI. A GUI reads only the rows visible in its viewport, which is a small number of rows.

Otherwise, use [`select`](../reference/table-operations/select/select.md) or [`update`](../reference/table-operations/select/update.md). If the formula has few distinct input values relative to the number of rows, consider [`lazy_update`](../reference/table-operations/select/lazy-update.md) instead. See [Cache results with `lazy_update`](#cache-results-with-lazy_update).

> [!CAUTION]
> If a formula calls a non-deterministic method, such as one that returns a random number or the current time, or reads a mutable structure, use [`select`](../reference/table-operations/select/select.md) or [`update`](../reference/table-operations/select/update.md). The other operations can give inconsistent results:
>
> - [`view`](../reference/table-operations/select/view.md) and [`update_view`](../reference/table-operations/select/update-view.md) can return a different value each time a consumer reads a cell, so downstream operations on the column have undefined behavior.
> - [`lazy_update`](../reference/table-operations/select/lazy-update.md) can return one cached value for every row with the same input values.

To turn formula columns into in-memory columns later, call [`select`](../reference/table-operations/select/select.md) with no arguments. It evaluates every formula column and stores the results. Columns that are already in memory are usually kept as they are, without a copy.

> [!NOTE]
> **Further reading**
>
> The [blog post on computing data immediately or as needed](/blog/2024/02/12/group-ungroup#compute-data-immediately-or-as-needed) compares [`update`](../reference/table-operations/select/update.md) and [`update_view`](../reference/table-operations/select/update-view.md) on a 135-million-row table, timing both the creation of the new column and a downstream [DAG](../conceptual/dag.md) calculation that reads every row of it.

## Cache results with `lazy_update`

[`lazy_update`](../reference/table-operations/select/lazy-update.md) computes column formulas on demand, like [`update_view`](../reference/table-operations/select/update-view.md), and caches each result in a map keyed by the formula's input values.

- It computes nothing for a cell until a consumer reads it.
- It computes the formula only once for each distinct input.
- Like [`update`](../reference/table-operations/select/update.md) and [`update_view`](../reference/table-operations/select/update-view.md), it references the source table's existing columns instead of copying them.

Use [`lazy_update`](../reference/table-operations/select/lazy-update.md) when a formula has few distinct input values relative to the number of rows. It then uses less memory than [`update`](../reference/table-operations/select/update.md) and less computation than [`update_view`](../reference/table-operations/select/update-view.md). On a ticking table, the cache keeps every distinct input it has seen and never evicts any, so this advantage holds only while the number of distinct inputs stays small over the table's lifetime. With many distinct input values, [`update`](../reference/table-operations/select/update.md) is more efficient, because it stores results in an array, which is more compact than a map.

Here is an example of [`lazy_update`](../reference/table-operations/select/lazy-update.md). Column `C` contains only two distinct values (2 and 5), so the engine computes `sqrt(2)` and `sqrt(5)` once each, the first time a consumer reads a cell that needs them. The other two cells where `C` is 5 reuse the cached result instead of recomputing it.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("A", ["The", "At", "Is", "On"]),
        int_col("B", [1, 2, 3, 4]),
        int_col("C", [5, 2, 5, 5]),
    ]
)

result = source.lazy_update(formulas=["Y = sqrt(C)"])
```

> [!NOTE]
> [`lazy_update`](../reference/table-operations/select/lazy-update.md) does not cache vectorized [Python function](./python-functions.md) calls. For those calls, the formula runs for every cell, as it does in [`update_view`](../reference/table-operations/select/update-view.md).
>
> The engine usually vectorizes a formula that is only a call to a Python function, such as `Y = f(C)`, when every argument is a column, a variable, or a constant. A vectorized call makes one call into Python for each chunk (block) of rows instead of one call for each row.
>
> The engine doesn't vectorize a call whose argument count differs from the function's parameter count, such as a call that passes several values to `*args` or one that leaves out parameters that have defaults.

## Related documentation

- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)
- [`update`](../reference/table-operations/select/update.md)
- [`update_view`](../reference/table-operations/select/update-view.md)
- [`lazy_update`](../reference/table-operations/select/lazy-update.md)
- [Query strings](./query-string-overview.md)
- [Formulas](./formulas.md)
- [Python functions in query strings](./python-functions.md)
- [Directed acyclic graph (DAG)](../conceptual/dag.md)
- [Incremental update model](../conceptual/table-update-model.md)
- [Blog: Compute data immediately or as needed](/blog/2024/02/12/group-ungroup#compute-data-immediately-or-as-needed)
