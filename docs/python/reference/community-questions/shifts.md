---
title: How do row keys and positional indices behave during table operations?
---

_When working with tables in Deephaven, I understand that "shifts" can happen when tables grow beyond their allocated row key slots. How do the row key (k) and positional index (i) attributes change during these shifts, and what impact does this have on downstream operations?_

When you [`merge`](../table-operations/merge/merge.md) tables in Deephaven, the engine pre-allocates row key slots for each constituent table. Understanding how row keys (`k`) and positional indices (`i`) behave during growth and shifts is crucial for working with formulas that reference these attributes.

## Key terms

Before diving into the details, let's define the key concepts:

- **Row key (`k`)**: A unique identifier assigned to each row in a table. Row keys are used internally by the engine to track rows and are typically sequential integers starting from 0. See [RowSet](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) in the Javadoc for more details.

- **Positional index (`i` and `ii`)**: The current position of a row within a table, starting from 0 for the first row. Unlike row keys, positional indices always reflect the current order of rows.

- **Shifts**: When rows need to be inserted between existing rows (such as in sorted tables), Deephaven communicates row shift data that moves ranges of row keys by a positive or negative shift delta. This allows the engine to maintain proper ordering without spurious modifications. The row data itself doesn't change, but the row keys do. See [Incremental update model: Shifts](../../conceptual/table-update-model.md#shifts) for detailed technical information.

- **Constituent tables**: The individual tables that make up a larger table structure, such as tables being merged, joined, or subtables within a partitioned table. Each constituent table gets its own allocated range of row keys in the result.

- **Row key slots**: Pre-allocated ranges of row keys reserved for each constituent table. By default, the engine allocates in units of 65,536 (2^16) row keys. For example, table A might get slots 0–65,535, while table B gets slots 65,536–131,071. The unit is configurable via the `UnionRedirection.allocationUnit` system property.

- **Refreshing tables**: Tables that can change over time (live data), as opposed to static tables that never change after creation. This includes:
  - tables created from live data sources (like [`time_table`](../table-operations/create/timeTable.md), Kafka streams, etc.).
  - tables that are the result of operations on refreshing tables.
  - any table where rows can be added, removed, or shifted after creation.
    In contrast, **[static tables](../../conceptual/table-types.md#static-tables)** never change after creation — they contain fixed data that remains constant.

- **Append-only tables**: A special type of refreshing table where new rows are only added (never moved or deleted), making positional indices safe to use. See [Table types - Append-only](../../conceptual/table-types.md#specialization-1-append-only) for more information.

> [!CAUTION]
> **Formulas using the positional indices `i`/`ii`, the row key `k`, or column array variables throw `IllegalArgumentException` on most refreshing tables.** The engine blocks these formulas by default to prevent incorrect results. `i` and `ii` are allowed on append-only and blink tables, `k` on add-only and blink tables, and column arrays only on blink tables. You can override this check with the system property `io.deephaven.engine.table.impl.select.AbstractFormulaColumn.allowUnsafeRefreshingFormulas=true`, but doing so **will produce incorrect results** when table shifts occur. These formulas are safe to use on static tables or on append-only tables (like `time_table("PT1s").update("X = ii")`) where rows are only added, never moved.

## Row key allocation and shifts

When you create a merged table, the engine reserves contiguous ranges of row keys for each constituent table:

```python test-set=1 order=table_a,table_b,my_merged_table
from deephaven import merge, new_table
from deephaven.column import string_col

# Create two tables with sample data
table_a = new_table([string_col("SomeValue", ["apple", "banana"])])
table_b = new_table([string_col("SomeValue", ["zebra", "bear"])])

# Merge them
my_merged_table = merge(
    [
        table_a.view(formulas=["SourceTable=`table_a`", "SomeValue"]),
        table_b.view(formulas=["SourceTable=`table_b`", "SomeValue"]),
    ]
)
```

Initially, the engine allocates:

- **`table_a`**: row key slots \[0, 65535\]
- **`table_b`**: row key slots \[65536, 131071\]

## Initial state with few rows

With two rows in each table, the merged table looks like:

| SourceTable | SomeValue | i | k     |
| ----------- | --------- | - | ----- |
| `table_a`   | `apple`   | 0 | 0     |
| `table_a`   | `banana`  | 1 | 1     |
| `table_b`   | `zebra`   | 2 | 65536 |
| `table_b`   | `bear`    | 3 | 65537 |

## What happens during a shift

Suppose `table_a` were a refreshing table that later grows beyond its allocated 65,536 slots. The engine must shift `table_b` to make room. After adding 65,536 new rows to `table_a`, the allocation becomes:

- **`table_a`**: row key slots \[0, 131071\]
- **`table_b`**: row key slots \[131072, 196607\]

The merged table now looks like:

| SourceTable | SomeValue   | i     | k      |
| ----------- | ----------- | ----- | ------ |
| `table_a`   | `apple`     | 0     | 0      |
| `table_a`   | `banana`    | 1     | 1      |
| ...         | ...         | ...   | ...    |
| `table_a`   | `avocado`   | 65535 | 65535  |
| `table_a`   | `peach`     | 65536 | 65536  |
| `table_a`   | `blueberry` | 65537 | 65537  |
| `table_b`   | `zebra`     | 65538 | 131072 |
| `table_b`   | `bear`      | 65539 | 131073 |

## Key behavior: row keys change, but row data doesn't

The critical insight is that the `zebra` and `bear` rows **were not modified** — they were simply shifted to new row key positions. Their row keys changed from 65536/65537 to 131072/131073, but the actual row data remains unchanged.

## Impact on formulas using positional index (i)

This behavior has important implications for formulas that reference the positional index `i`. Consider what would happen if you tried to add a formula that depends on `i`:

```python test-set=1 order=my_merged_table_with_idx
# On a refreshing table, this would throw IllegalArgumentException unless you enable unsafe formulas.
# The static tables in this example allow it, but on live data the engine blocks it (see the caution above).
my_merged_table_with_idx = my_merged_table.update("MyRowIdx = SomeValue + i")
```

Downstream operations don't know that the positional index `i` changed for the `zebra`/`bear` rows when rows are added ahead of them. **This is why the engine blocks such formulas by default on refreshing tables** — they would produce incorrect results after shifts occur.

If you enabled unsafe formulas on a refreshing table, `MyRowIdx` would evolve like this:

**Initially:**

| SourceTable | SomeValue | i | k     | MyRowIdx |
| ----------- | --------- | - | ----- | -------- |
| `table_a`   | `apple`   | 0 | 0     | apple0   |
| `table_a`   | `banana`  | 1 | 1     | banana1  |
| `table_b`   | `zebra`   | 2 | 65536 | zebra2   |
| `table_b`   | `bear`    | 3 | 65537 | bear3    |

**After adding one row to `table_a`:**

| SourceTable | SomeValue | i | k     | MyRowIdx |
| ----------- | --------- | - | ----- | -------- |
| `table_a`   | `apple`   | 0 | 0     | apple0   |
| `table_a`   | `banana`  | 1 | 1     | banana1  |
| `table_a`   | `orange`  | 2 | 2     | orange2  |
| `table_b`   | `zebra`   | 3 | 65536 | zebra2   |
| `table_b`   | `bear`    | 4 | 65537 | bear3    |

Notice that `zebra` and `bear` now have positional indices 3 and 4, but they keep the `MyRowIdx` values `zebra2` and `bear3` because those rows were never modified.

**After a major shift (65,538 rows in `table_a`):**

| SourceTable | SomeValue   | i     | k      | MyRowIdx       |
| ----------- | ----------- | ----- | ------ | -------------- |
| `table_a`   | `apple`     | 0     | 0      | apple0         |
| `table_a`   | `banana`    | 1     | 1      | banana1        |
| `table_a`   | `orange`    | 2     | 2      | orange2        |
| ...         | ...         | ...   | ...    | ...            |
| `table_a`   | `avocado`   | 65535 | 65535  | avocado65535   |
| `table_a`   | `peach`     | 65536 | 65536  | peach65536     |
| `table_a`   | `blueberry` | 65537 | 65537  | blueberry65537 |
| `table_b`   | `zebra`     | 65538 | 131072 | zebra2         |
| `table_b`   | `bear`      | 65539 | 131073 | bear3          |

The `zebra` and `bear` rows maintain their original `MyRowIdx` values because the engine never re-evaluates formulas for rows that weren't actually modified — this is the key performance benefit of the shift mechanism.

## Why shifts exist

Shifts are a performance optimization that allows the engine to avoid re-evaluating formulas, joins, and aggregations when rows are simply moved to accommodate table growth. Only when row data is actually modified does the engine need to recalculate dependent operations.

For more detailed information about shifts and the table update model, see [Incremental update model: Shifts](../../conceptual/table-update-model.md#shifts).

While this example focuses on [`merge`](../table-operations/merge/merge.md) operations, shifts can reach any operation downstream of a refreshing table, whether they originate in that table or in an operation such as sort or merge. The same safety concerns about positional indices apply regardless of which operation triggers the shift. Note that even without shifts, any operations that add or remove rows from refreshing tables can similarly cause positional indices (`i`, `ii`) to become incorrect in downstream operations.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not answered in our documentation, [join our Community](/slack) and ask it there. We are happy to help!
