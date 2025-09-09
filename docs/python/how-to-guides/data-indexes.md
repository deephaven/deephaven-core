---
title: Data Indexes
---

> [!IMPORTANT]
> This feature is currently experimental. The API and performance characteristics are subject to change.

This guide covers what data indexes are, how to create and use them, and how they can improve query performance. A data index maps key values in one or more key columns to the [row sets](../conceptual/table-update-model.md#describing-table-updates) containing the relevant data. Data indexes are used to improve the speed of data access operations in tables. Many table operations will use a data index if it is present. Every data index is backed by a table, with the index stored in a column called `dh_row_set`.

Data indexes can improve the speed of filtering operations. Additionally, data indexes can be useful if multiple query operations need to compute the same data index on a table. This is common when a table is used in multiple joins or aggregations. If the table does not have a data index, each operation will internally create the same index. If the table does have a data index, the individual operations will not need to create their own indexes and can execute faster and use less RAM.

## Create a data index

A data index can be created from a source table and one or more key columns using [`data_index`](../reference/engine/data-index.md):

```python test-set=1 order=source
from deephaven.experimental.data_index import data_index
from deephaven import empty_table

source = empty_table(10).update("X = i")
source_index = data_index(source, "X")
```

> [!IMPORTANT]
> When a new data index is created, it is not immediately computed. The index is computed when a table operation first uses it or the `table` attribute is called on the index. This is an important detail when trying to understand performance data.

Every data index is backed by a table. The backing table can be retrieved with the `table` attribute:

```python test-set=1 order=result
result = source_index.table
```

The `source` table in the above example doesn't create a very interesting data index. Each key value in `X` only appears once. The following example better illustrates a data index:

```python order=source_index,source
from deephaven.experimental.data_index import data_index
from deephaven import empty_table

source = empty_table(10).update("X = 100 + i % 5")
source_index = data_index(source, "X").table
```

When looking at `source`, we can see that the value `100` in `X` appears in rows 0 and 5. The value `101` appears in rows 1, 6, and so on. Each row in `dh_row_set` contains the rows where each unique key value appears. Let's look at an example for more than one key column.

```python order=source_index,source
from deephaven.experimental.data_index import data_index
from deephaven import empty_table

source = empty_table(20).update(["X = i % 4", "Y = i % 2"])
source_index = data_index(source, ["X", "Y"]).table
```

There are only four unique keys in `X` and `Y` together. The resultant column `dh_row_set` shows the row sets in which each unique key appears.

All of the previous examples create tables with flat data indexes. A flat data index is one in which the row sets are sequential. However, it is not safe to assume that all data indexes are flat, as this is uncommon in practice. Consider the following example where the data index is not flat:

```python order=result,source_index,source
from deephaven.experimental.data_index import data_index
from deephaven import empty_table
from deephaven import agg

source = empty_table(25).update(
    ["Key = 100 + randomInt(0, 4)", "Value = randomDouble(0, 100)"]
)

source_index = data_index(source, "Key").table

result = source_index.where("Key in 100, 102")
```

## Retrieve and verify a data index

You can verify whether a data index exists for a particular table using one of two methods:

- [`has_data_index`](../reference/engine/has-data-index.md): Returns a boolean indicating whether a data index exists for the table and the given key column(s).
- [`data_index`](../reference/engine/data-index.md): If `create_if_absent` is `False`, it returns the existing data index if there is one or `None` if there is no index.

The following example creates data indexes for a table and checks if they exist based on different key columns. [`has_data_index`](../reference/engine/has-data-index.md) is used to check if the index exists.

```python order=:log,source
from deephaven.experimental.data_index import data_index, has_data_index
from deephaven import empty_table

source = empty_table(100).update(
    [
        "Timestamp = '2024-04-25T12:00:00Z' + (ii * 1_000_000)",
        "Key1=ii%8",
        "Key2=ii%11",
        "Key3=ii%19",
        "value=-ii",
    ]
)

index_1_3 = data_index(source, ["Key1", "Key3"])

has_index_1 = has_data_index(source, ["Key1"])
has_index_2_3 = has_data_index(source, ["Key2", "Key3"])
has_index_1_3 = has_data_index(source, ["Key1", "Key3"])

print(f"There is a data index for source on Key 1: {has_index_1}")
print(f"There is a data index for source on Keys 2 and 3: {has_index_2_3}")
print(f"There is a data index for source on Keys 1 and 3: {has_index_1_3}")

index_2 = data_index(source, ["Key2"], create_if_absent=False)

has_index_2 = has_data_index(source, ["Key2"])

print(f"There is a data index for source on Key 2: {has_index_2}")
```

## Usage in queries

> [!WARNING]
> Data indexes are [weakly reachable](https://en.wikipedia.org/wiki/Weak_reference). Assign the index to a variable to ensure it does not get garbage collected.

If a table has a data index and an engine operation can leverage it, it will be automatically used. The following table operations can use data indexes:

- Join:
  - [`exact_join`](../reference/table-operations/join/exact-join.md)
  - [`natural_join`](../reference/table-operations/join/natural-join.md)
  - [`aj`](../reference/table-operations/join/aj.md)
  - [`raj`](../reference/table-operations/join/raj.md)
- Group and aggregate:
  - [Single aggregators](./dedicated-aggregations.md)
  - [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
  - [`update_by`](../reference/table-operations/update-by-operations/updateBy.md)
- Filter:
  - [`where`](../reference/table-operations/filter/where.md)
  - [`where_in`](../reference/table-operations/filter/where-in.md)
  - [`where_not_in`](../reference/table-operations/filter/where-not-in.md)
- Select:
  - [`select_distinct`](../reference/table-operations/select/select-distinct.md)
- Sort:
  - [`sort`](../reference/table-operations/sort/sort.md)
  - [`sort_descending`](../reference/table-operations/sort/sort-descending.md)

The following subsections explore different table operations. The code blocks below use the following tables:

```python test-set=2 order=source,source_right,source_right_distinct
from deephaven.experimental.data_index import data_index
from deephaven import empty_table
from deephaven import agg

source = empty_table(100_000).update(
    [
        "Timestamp = '2024-04-25T12:00:00Z' + (ii * 1_000_000)",
        "Key1 = ii%11",
        "Key2 = ii%47",
        "Key3 = ii%499",
        "Value = ii",
    ]
)

source_right = empty_table(400).update(
    [
        "Timestamp = '2024-04-25T12:00:00Z' + (ii * 1_000_000)",
        "Key1 = ii%11",
        "Key2 = ii%43",
        "Value = ii/2",
    ]
)

source_right_distinct = source_right.select_distinct(["Key1", "Key2"])
```

### Filter

The engine will use single and multi-column indexes to accelerate exact match filtering. Range filtering does not benefit from an index.

> [!NOTE]
> The Deephaven engine only uses a [`DataIndex`](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.DataIndex) when the keys exactly match what is needed for an operation. For example, if a data index is present for the columns `X` and `Y`, it will not be used if the engine only needs an index for column `X`.

The following filters can use the index created atop the code block:

```python test-set=2 order=result_k1,result_k1_in
source_k1_index = data_index(source, ["Key1"])

result_k1 = source.where("Key1 = 7")
result_k1_in = source.where("Key1 in 2,4,6,8")
```

The following example can only use the index for the first filter:

```python test-set=2 order=result_k1_k2
source_k1_index = data_index(source, ["Key1"])

result_k1_k2 = source.where(["Key1=7", "Key2=11"])
```

This is true for filtering one table based on another as well. The first filter in the following example can use `source_k1_k2_index`, whereas the second example cannot use `source_right_k1_index` because the index does not match the filter:

```python test-set=2 order=result_wherein,result_wherenotin
source_k1_k2_index = data_index(source, ["Key1", "Key2"])
source_right_k1_index = data_index(source, ["Key1"])

result_wherein = source.where_in(source_right_distinct, cols=["Key1", "Key2"])
result_wherenotin = source.where_not_in(source_right_distinct, cols=["Key1", "Key2"])
```

### Join

Different joins will use indexes differently:

| Method                                                                     | Can use indexes?   | Can use left table index? | Can use right table index? |
| -------------------------------------------------------------------------- | ------------------ | ------------------------- | -------------------------- |
| [`exact_join`](../reference/table-operations/join/exact-join.md)           | :white_check_mark: | :white_check_mark:        | :no_entry_sign:            |
| [`natural_join`](../reference/table-operations/join/natural-join.md)       | :white_check_mark: | :white_check_mark:        | :no_entry_sign:            |
| [`aj`](../reference/table-operations/join/aj.md)                           | :white_check_mark: | :white_check_mark:        | :white_check_mark:         |
| [`raj`](../reference/table-operations/join/raj.md)                         | :white_check_mark: | :white_check_mark:        | :white_check_mark:         |
| [`join`](../reference/table-operations/join/join.md)                       | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |
| [`left_outer_join`](../reference/table-operations/join/left-outer-join.md) | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |
| [`full_outer_join`](../reference/table-operations/join/full-outer-join.md) | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |
| [`multi_join`](../reference/table-operations/join/multi-join.md)           | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |
| [`range_join`](../reference/table-operations/join/range-join.md)           | :no_entry_sign:    | :no_entry_sign:           | :no_entry_sign:            |

#### Natural join

When performing a [`natural_join`](../reference/table-operations/join/natural-join.md) without data indexes, the engine performs a linear scan of the `source` (left) table's rows. Each key is hashed and a map is created to the specified joining (right) table. If a data index exists for the `source` (left) table, the engine skips the scan and uses the index directly to create the mapping.

Consider a [`natural_join`](../reference/table-operations/join/natural-join.md) on `source` and `source_right`. The following example can use `source_k1_k2_index` to accelerate the join, but not `source_right_k1_k2_index` because the operation cannot use the right table index:

```python test-set=2 order=result_naturaljoined
source_k1_k2_index = data_index(source, ["Key1", "Key2"])
source_right_k1_k2_index = data_index(source_right, ["Key1", "Key2"])

result_naturaljoined = source.natural_join(
    source_right, on=["Key1", "Key2"], joins=["RhsValue=Value"]
)
```

#### As-of join

The exact match in an [`aj`](../reference/table-operations/join/aj.md) or an [`raj`](../reference/table-operations/join/raj.md) can be accelerated using data indexes, eliminating the task of grouping the source table rows by key. The following example can use the indexes created because they match the exact match column used in the join:

```python test-set=2
source_k1_index = data_index(source, ["Key1"])
source_right_k1_index = data_index(source_right, ["Key1"])

result_asofjoined = source.aj(
    source_right,
    on=["Key1", "Timestamp >= Timestamp"],
    joins=["RhsValue=Value", "RhsTimestamp=Timestamp"],
)
```

### Sort

Sorting operations can be accelerated using both full and partial indexes.

- A full index matches all sorting columns.
- A partial index matches a subset of sorting columns.

When a full index is present, the engine only needs to sort the index rows.

A sort operation can only use a partial index if the partial index matches the _first_ sorting column. When a sort operation uses a partial index, the engine sorts the first column using the index and then sorts any remaining columns naturally.

The following sort operation uses the index because it matches the sort columns (ordering does not matter):

```python test-set=2 order=result_sorted_k2_k1
source_k1_k2_index = data_index(source, ["Key1", "Key2"])

result_sorted_k2_k1 = source.sort_descending(order_by=["Key2", "Key1"])
```

The following sort operation uses the index when sorting the first key column only:

```python test-set=2 order=result_sorted_k1_k3
source_k1_index = data_index(source, ["Key1"])

result_sorted_k1_k3 = source.sort(order_by=["Key1", "Key3"])
```

The following sort operation does not use any indexes, as the only partial index (on `Key1`) does not match the first sort column:

```python test-set=2 order=result_sorted_k3_k1
source_k1_index = data_index(source, ["Key1"])
source_right_k1_index = data_index(source_right, ["Key1"])

result_sorted_k3_k1 = source.sort(order_by=["Key3", "Key1"])
```

### Aggregate

Aggregations without data indexes require the engine to group all similar keys and perform the calculation on subsets of rows. The index allows the engine to skip the grouping step and immediately begin calculating the aggregation. The following aggregation uses the index because it matches the aggregation key columns:

```python test-set=2 order=result_agged
source_k1_k2_index = data_index(source, ["Key1", "Key2"])

result_agged = source.agg_by(
    [
        agg.sum_(cols=["Sum=Value"]),
        agg.avg(cols=["Avg=Value"]),
        agg.std(cols=["Std=Value"]),
    ],
    by=["Key1", "Key2"],
)
```

## Persist data indexes with Parquet

Deephaven can save data indexes along with your data when you [write a table to Parquet files](./data-import-export/parquet-export.md). By default, it saves all available indexes. You can also choose to save only some indexes. If you try to save an index that doesn't exist yet, Deephaven will create a temporary index just for the saving process. When you [load data from a Parquet file](./data-import-export/parquet-import.md), it also loads any saved indexes.

The following example writes a table plus indexes to Parquet and then reads a table plus indexes from the Parquet files.

```python order=:log
from deephaven import empty_table
from deephaven import agg
from deephaven.experimental.data_index import data_index, has_data_index
from deephaven.parquet import read, write
from deephaven.liveness_scope import liveness_scope

source = empty_table(100_000).update(
    [
        "Timestamp = '2024-04-25T12:00:00Z' + (ii * 1_000_000)",
        "Key1=ii%11",
        "Key2=ii%47",
        "Key3=ii%499",
        "value=ii",
    ]
)

# Write to disk and specify two sets of index key columns
write(source, path="/data/indexed.parquet", index_columns=[["Key1"], ["Key1", "Key2"]])

# Load the table and the indexes from disk
disk_table = read("/data/indexed.parquet")

# Show that the table loaded from disk has indexes
print(has_data_index(disk_table, ["Key1"]))
print(has_data_index(disk_table, ["Key1", "Key2"]))

# Open a new liveness scope so the indexes are released after writing
with liveness_scope() as scope:
    index_key1 = data_index(source, ["Key1"])
    index_key1_key2 = data_index(source, ["Key1", "Key2"])

    # Write to disk - indexes are automatically persisted
    write(source, path="/data/indexed.parquet")

# Load the table and the indexes from disk
disk_table_new = read("/data/indexed.parquet")

# Show that the table loaded from disk has indexes
print(has_data_index(disk_table_new, ["Key1"]))
print(has_data_index(disk_table_new, ["Key1", "Key2"]))
```

## Performance

Data indexes improve performance when used in the right context. If used in the wrong context, data indexes can make performance worse. The following list provides guidelines to consider when determining if data indexes will improve performance:

- A data index is more likely to help performance if the data within an index group is located near (in memory) other data in the group. This avoids index fragmentation and poor data access patterns.
- Reusing a data index multiple times is necessary to overcome the cost of initially computing the index.
- Generally speaking, the higher the key cardinality, the better the performance improvement.

### Benchmark scripts

If you're interested in how data indexes impact query performance, the scripts below collect performance metrics for each operation. You will notice that performance improves in some instances and is worse in others.

This first script sets up the tests:

```python skip-test
from deephaven.experimental.data_index import data_index, has_data_index
from deephaven import empty_table
from deephaven.table import Table
from typing import List, Any
from time import time
import jpy

# Disable memoization for perf testing
jpy.get_type("io.deephaven.engine.table.impl.QueryTable").setMemoizeResults(False)


def time_it(name: str, f: Callable[[], Any]) -> Any:
    """
    Time a function execution.
    """
    start = time()
    rst = f()
    end = time()
    exec_time = end - start
    print(f"{name} took {(exec_time):.4f} seconds.")
    return rst, exec_time


def add_index(t: Table, by: List[str]):
    """
    Add a data index.
    By adding .table, the index calculation is forced to be now instead of when it is first used.

    Args:
    - t: Table to add the index to.
    - by: Columns to index on.

    Returns:
    - The index.
    """
    print(f"Adding data index: {by}")

    def compute_index():
        idx = data_index(t, by)
        # call .table to force index computation here -- to benchmark the index creation separately -- not for production
        idx.table
        return idx


def run_test(n_i_keys: int, n_j_keys: int, create_idxs: list[bool]) -> Any:
    """
    Run a series of performance benchmark tests.

    Args:
    - n_i_keys: Number of unique keys for column I.
    - n_j_keys: Number of unique keys for column J.
    - create_idxs: List of booleans to determine which indexes to create (I, J, and [I, J])
    """
    t = empty_table(100_000_000).update(
        ["I = ii % n_i_keys", "J = ii % n_j_keys", "V = random()"]
    )
    t_lastby = t.last_by(["I", "J"])

    if create_idxs[0]:
        idx1_i = add_index(t, ["I"])
    if create_idxs[1]:
        idx1_j = add_index(t, ["J"])
    if create_idxs[2]:
        idx1_ij = add_index(t, ["I", "J"])

    idx_ij = has_data_index(t, ["I", "J"])
    idx_i = has_data_index(t, ["I"])
    idx_j = has_data_index(t, ["J"])
    print(f"Has index: ['I', 'J']={idx_ij} ['I']={idx_i} ['J']={idx_j}")
    ret, t_where = time_it("where", lambda: t.where(["I = 3", "J = 6"]))
    ret, t_countby = time_it("count_by", lambda: t.count_by("Count", ["I", "J"]))
    ret, t_sumby = time_it("sum_by", lambda: t.sum_by(["I", "J"]))
    ret, t_nj = time_it(
        "natural_join", lambda: t.natural_join(t_lastby, ["I", "J"], "VJ = V")
    )

    return [t_where, t_countby, t_sumby, t_nj]
```

Here's a follow-up script that runs tests for both low- and high-cardinality cases, both with and without data indexes:

```python skip-test
from deephaven.column import double_col, string_col
from deephaven import new_table

print("Low cardinality, no data indexes")
times_lc_ni = run_test(n_i_keys=100, n_j_keys=7, create_idxs=[False, False, False])

print("Low cardinality, with data indexes")
times_lc_wi = run_test(n_i_keys=100, n_j_keys=7, create_idxs=[True, True, True])

print("High cardinality, no data indexes")
times_hc_ni = run_test(n_i_keys=100, n_j_keys=997, create_idxs=[False, False, False])

print("High cardinality, with data indexes")
times_hc_wi = run_test(n_i_keys=100, n_j_keys=997, create_idxs=[True, True, True])

results = new_table(
    [
        string_col("Operation", ["where", "count_by", "sum_by", "natural_join"]),
        double_col("TimesLowCardinalityNoIndex", times_lc_ni),
        double_col("TimesLowCardinalityWithIndex", times_lc_wi),
        double_col("TimesHighCardinalityNoIndex", times_hc_ni),
        double_col("TimesHighCardinalityWithIndex", times_hc_wi),
    ]
)
```

It can also be helpful to plot the results:

```python skip-test
from deephaven.plot.figure import Figure

fig = (
    Figure()
    .plot_cat(
        series_name="Low cardinality (700 keys), no indexes.",
        t=results,
        category="Operation",
        y="Times_LowCardinality_NoIndex",
    )
    .plot_cat(
        series_name="Low cardinality (700 keys), with indexes.",
        t=results,
        category="Operation",
        y="Times_LowCardinality_WithIndex",
    )
    .plot_cat(
        series_name="High cardinality (99,700 keys), no indexes.",
        t=results,
        category="Operation",
        y="Times_HighCardinality_NoIndex",
    )
    .plot_cat(
        series_name="High cardinality (99,700 keys), with indexes.",
        t=results,
        category="Operation",
        y="Times_HighCardinality_WithIndex",
    )
    .show()
)
```

## Related documentation

- [Combined aggregations](./dedicated-aggregations.md)
- [Filter table data](./use-filters.md)
- [Export to Parquet](./data-import-export/parquet-export.md)
- [Import to Parquet](./data-import-export/parquet-import.md)
- [Joins: exact and relational](./joins-exact-relational.md)
- [Joins: inexact and range](./joins-timeseries-range.md)
- [Sort table data](./sort.md)
- [`data_index`](../reference/engine/data-index.md)
- [`has_data_index`](../reference/engine/has-data-index.md)
- [`DataIndex` Pydoc](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.DataIndex)
