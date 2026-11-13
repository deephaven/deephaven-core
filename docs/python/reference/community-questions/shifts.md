---
title: How do row keys and positional indices behave during table operations?
---

_When working with tables in Deephaven, I understand that "shifts" can happen when tables grow beyond their allocated row key slots. How do the row key (k) and positional index (i) attributes change during these shifts, and what impact does this have on downstream operations?_

**Shifts can occur with any table operation that works with live/refreshing data** - including merges, joins, updates, selects, and other operations. For example, when you merge tables, the engine reserves a range of row keys for each input table. If an input table outgrows its range, the engine shifts other tables' row keys to make room. Understanding how row keys (`k`) and positional indices (`i`) behave during these changes is crucial for working with formulas that reference these attributes.

## Key terms

Before diving into the details, let's define the key concepts:

- **Row key (`k`)**: A unique identifier assigned to each row in a table. Row keys are used internally by the engine to track rows and are typically sequential integers starting from 0. See [RowSet](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) in the Javadoc for more details.

- **Positional index (`i` and `ii`)**: The current position of a row within a table, starting from 0 for the first row. Unlike row keys, positional indices always reflect the current order of rows.

- **Shifts**: When rows need to be inserted between existing rows (such as in sorted tables), Deephaven communicates row shift data that moves ranges of row keys by a positive or negative shift delta. This allows the engine to maintain proper ordering without spurious modifications. The row data itself doesn't change, but the row keys do. See [Incremental update model: Shifts](../../conceptual/table-update-model.md#shifts) for detailed technical information.

- **Constituent tables**: The individual tables that make up a larger table structure, such as tables being merged, joined, or subtables within a partitioned table. Each constituent table gets its own allocated range of row keys in the result.

- **Row key slots**: Pre-allocated ranges of row keys reserved for each constituent table. For example, table A might get slots 0-4095, while table B gets slots 4096-8191.

- **Refreshing tables**: Tables that can change over time (live data), as opposed to static tables that never change after creation. This includes:
  - tables created from live data sources (like `time_table`, Kafka streams, etc.).
  - tables that are the result of operations on refreshing tables.
  - any table where rows can be added, removed, or shifted after creation.

- **[Static tables](../../conceptual/table-types.md#static-tables)**: Tables that never change after creation - they contain fixed data that remains constant.

- **Append-only tables**: A special type of refreshing table where new rows are only added (never moved or deleted), making positional indices safe to use. See [Table types - Append-only](../../conceptual/table-types.md#specialization-1-append-only) for more information.

> [!CAUTION]
> **Formulas using positional indices (`i`, `ii`, `k`) or [column array variables](../../how-to-guides/work-with-arrays.md#using-the-underscore-operator) are unsafe on refreshing tables and will cause `IllegalArgumentException` errors.** The engine blocks these formulas by default to prevent incorrect results. While you can override this with the system property `io.deephaven.engine.table.impl.select.AbstractFormulaColumn.allowUnsafeRefreshingFormulas=true`, doing so **will produce incorrect results** when table shifts occur. Additionally, any table operations that add or remove rows can also cause positional indices to become incorrect. **Note that shifts can happen with any table operation, not just merges** - including joins, updates, selects, and other operations on live/refreshing tables. Positional indices `i` and `ii` are safe on static tables and [append-only tables](../../conceptual/table-types.md#specialization-1-append-only) (like `time_table("PT1s").update("X = ii")`), where rows are only added, never moved. The row key `k` is safe on static tables and [add-only tables](../../conceptual/table-types.md#specialization-2-add-only). [Column array variables](../../how-to-guides/work-with-arrays.md#using-the-underscore-operator) have stricter rules and are safe only on static tables or [blink tables](../../conceptual/table-types.md#specialization-3-blink).

## Row key allocation and shifts

When you create a merged table, the engine reserves contiguous ranges of row keys for each constituent table:

```python test-set=1 order=table_a,table_b,my_merged_table
from deephaven import merge, new_table
from deephaven.column import string_col

# Create two tables with sample data
table_a = new_table([string_col("someValue", ["apple", "banana"])])
table_b = new_table([string_col("someValue", ["zebra", "bear"])])

# Merge them
my_merged_table = merge(
    [
        table_a.view(formulas=["sourceTable=`table_a`", "someValue"]),
        table_b.view(formulas=["sourceTable=`table_b`", "someValue"]),
    ]
)
```

Initially, the engine might allocate:

- **`table_a`**: row key slots \[0, 4095\]
- **`table_b`**: row key slots \[4096, 8191\]

## Initial state with few rows

When `table_a` and `table_b` each contain two rows, the merged table assigns each table a separate range of row keys:

| sourceTable | someValue | i | k    |
| ----------- | --------- | - | ---- |
| `table_a`   | `apple`   | 0 | 0    |
| `table_a`   | `banana`  | 1 | 1    |
| `table_b`   | `zebra`   | 2 | 4096 |
| `table_b`   | `bear`    | 3 | 4097 |

## What happens during a shift

When a constituent table outgrows its reserved row key range, the engine shifts other constituent tables to make room. If `table_a` grows beyond its allocated 4096 slots, the engine shifts `table_b` to a higher range. After adding 4096 new rows to `table_a`, the allocation becomes:

- **table_a**: row key slots \[0, 8191\]
- **table_b**: row key slots \[8192, 12287\]

The merged table now looks like:

| sourceTable | someValue   | i    | k    |
| ----------- | ----------- | ---- | ---- |
| `table_a`   | `apple`     | 0    | 0    |
| `table_a`   | `banana`    | 1    | 1    |
| ...         | ...         | ...  | ...  |
| `table_a`   | `avocado`   | 4095 | 4095 |
| `table_a`   | `peach`     | 4096 | 4096 |
| `table_a`   | `blueberry` | 4097 | 4097 |
| `table_b`   | `zebra`     | 4098 | 8192 |
| `table_b`   | `bear`      | 4099 | 8193 |

## Key behavior: row keys change, but row data doesn't

During a shift, moved rows get new row keys but keep their original data. The `zebra` and `bear` rows **were not modified** - they were simply shifted to new row key positions. Their row keys changed from 4096/4097 to 8192/8193, but the actual row data remains unchanged.

## Impact on formulas using positional index (i)

Because shifts change row keys without re-evaluating formulas, any formula that references the positional index `i` can produce stale or incorrect results. Consider what would happen if you tried to add a formula that depends on `i`:

```python should-fail
# This will fail with IllegalArgumentException unless you enable unsafe formulas
# DO NOT enable unsafe formulas unless you understand the risks (see CAUTION above)
my_merged_table_with_idx = my_merged_table.update("MyRowIdx = someValue + i")
```

This demonstrates the key insight: downstream operations don't know that the positional index `i` changed for the `zebra`/`bear` rows during the shift. **This is why the engine blocks such formulas by default** - they would produce incorrect results after shifts occur.

**Before the shift:**

| sourceTable | someValue | i | k    | MyRowIdx |
| ----------- | --------- | - | ---- | -------- |
| `table_a`   | `apple`   | 0 | 0    | apple0   |
| `table_a`   | `banana`  | 1 | 1    | banana1  |
| `table_b`   | `zebra`   | 2 | 4096 | zebra2   |
| `table_b`   | `bear`    | 3 | 4097 | bear3    |

**After adding one row to `table_a`:**

| sourceTable | someValue | i | k    | MyRowIdx |
| ----------- | --------- | - | ---- | -------- |
| `table_a`   | `apple`   | 0 | 0    | apple0   |
| `table_a`   | `banana`  | 1 | 1    | banana1  |
| `table_a`   | `orange`  | 2 | 2    | orange2  |
| `table_b`   | `zebra`   | 3 | 4096 | zebra2   |
| `table_b`   | `bear`    | 4 | 4097 | bear3    |

Notice that both the new `orange` row and the existing `zebra` row appear to have the same positional index behavior, but the `zebra` and `bear` rows retain their original `MyRowIdx` values ("zebra2", "bear3") because those rows were never actually modified.

**After a major shift (4098 rows in `table_a`):**

| sourceTable | someValue   | i    | k    | MyRowIdx      |
| ----------- | ----------- | ---- | ---- | ------------- |
| `table_a`   | `apple`     | 0    | 0    | apple0        |
| `table_a`   | `banana`    | 1    | 1    | banana1       |
| `table_a`   | `orange`    | 2    | 2    | orange2       |
| ...         | ...         | ...  | ...  | ...           |
| `table_a`   | `avocado`   | 4095 | 4095 | avocado4095   |
| `table_a`   | `peach`     | 4096 | 4096 | peach4096     |
| `table_a`   | `blueberry` | 4097 | 4097 | blueberry4097 |
| `table_b`   | `zebra`     | 4098 | 8192 | zebra2        |
| `table_b`   | `bear`      | 4099 | 8193 | bear3         |

The `zebra` and `bear` rows maintain their original `MyRowIdx` values because the engine never re-evaluates formulas for rows that weren't actually modified - this is the key performance benefit of the shift mechanism.

## Why shifts exist

Shifts are a performance optimization that allows the engine to avoid re-evaluating formulas, joins, and aggregations when rows are simply moved to accommodate table growth. Only when row data is actually modified does the engine need to recalculate dependent operations.

For more detailed information about shifts and the table update model, see [Incremental update model: Shifts](../../conceptual/table-update-model.md#shifts).

While this example focuses on merge operations, **shifts can occur with any table operation** that works with live/refreshing data, including joins, updates, selects, and other operations. The same safety concerns about positional indices apply regardless of which operation triggers the shift. Note that even without shifts, any operations that add or remove rows from refreshing tables can similarly cause positional indices (`i`, `ii`) to become incorrect in downstream operations.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not answered in our documentation, [join our Community](/slack) and ask it there. We are happy to help!
