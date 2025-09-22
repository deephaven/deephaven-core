---
title: How do row keys and positional indices behave during merge table operations?
sidebar_label: How do row keys and positional indices behave during merge table operations?
---

_When merging tables in Deephaven, I understand that "shifts" can happen when one table grows beyond its allocated row key slots. How do the row key (k) and positional index (i) attributes change during these shifts, and what impact does this have on downstream operations?_

When you merge tables in Deephaven, the engine pre-allocates row key slots for each constituent table. Understanding how row keys (`k`) and positional indices (`i`) behave during growth and shifts is crucial for working with formulas that reference these attributes.

## Key terms

Before diving into the details, let's define the key concepts:

- **Row key (`k`)**: A unique identifier assigned to each row in a table. Row keys are used internally by the engine to track rows and are typically sequential integers starting from 0.

- **Positional index (`i`)**: The current position of a row within a table, starting from 0 for the first row. Unlike row keys, positional indices always reflect the current order of rows.

- **Shifts**: When a table grows beyond its pre-allocated row key space, the engine may need to move (shift) other tables' rows to new row key positions to make room. The row data itself doesn't change, but the row keys do.

- **Constituent tables**: The individual tables that are being merged together. Each constituent table gets its own allocated range of row keys in the merged result.

- **Row key slots**: Pre-allocated ranges of row keys reserved for each constituent table. For example, table A might get slots 0-4095, while table B gets slots 4096-8191.

- **Refreshing tables**: Tables that can change over time (live data), as opposed to static tables that never change after creation. Append-only tables are a special type of refreshing table where new rows are only added (never moved or deleted), making positional indices safe to use.

> [!CAUTION] > **Formulas using positional indices (`i`, `ii`, `k`) or column array variables are unsafe on refreshing tables and will cause `IllegalArgumentException` errors.** The engine blocks these formulas by default to prevent incorrect results. While you can override this with the system property `io.deephaven.engine.table.impl.select.AbstractFormulaColumn.allowUnsafeRefreshingFormulas=true`, doing so **will produce incorrect results** when table shifts occur. **Note that shifts can happen with any table operation, not just merges** - including joins, updates, selects, and other operations on live/refreshing tables. These formulas are safe to use on static tables or append-only tables (like `time_table("PT1s").update("X = ii")`) where shifts cannot happen because rows are only added, never moved.

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

- **table_a**: row key slots \[0, 4095\]
- **table_b**: row key slots \[4096, 8191\]

## Initial state with few rows

When each table gets one row, the merged table looks like:

| sourceTable | someValue | i   | k    |
| ----------- | --------- | --- | ---- |
| `table_a`   | `apple`   | 0   | 0    |
| `table_a`   | `banana`  | 1   | 1    |
| `table_b`   | `zebra`   | 2   | 4096 |
| `table_b`   | `bear`    | 3   | 4097 |

## What happens during a shift

If `table_a` grows beyond its allocated 4096 slots, the engine must shift `table_b` to make room. After adding 4096 new rows to `table_a`, the allocation becomes:

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

The critical insight is that the `zebra` and `bear` rows **were not modified** - they were simply shifted to new row key positions. Their row keys changed from 4096/4097 to 8192/8193, but the actual row data remains unchanged.

## Impact on formulas using positional index (i)

This behavior has important implications for formulas that reference the positional index `i`. Consider this example:

```python should-fail
# This will fail with IllegalArgumentException unless you enable unsafe formulas
# DO NOT enable unsafe formulas unless you understand the risks (see DANGER banner above)
my_merged_table_with_idx = my_merged_table.update("MyRowIdx = someValue + i")
```

This demonstrates the key insight: downstream operations don't know that the positional index `i` changed for the `zebra`/`bear` rows during the shift. **This is why the engine blocks such formulas by default** - they would produce incorrect results after shifts occur.

**Before the shift:**

| sourceTable | someValue | i   | k    | MyRowIdx |
| ----------- | --------- | --- | ---- | -------- |
| `table_a`   | `apple`   | 0   | 0    | apple0   |
| `table_a`   | `banana`  | 1   | 1    | banana1  |
| `table_b`   | `zebra`   | 2   | 4096 | zebra2   |
| `table_b`   | `bear`    | 3   | 4097 | bear3    |

**After adding one row to `table_a`:**

| sourceTable | someValue | i   | k    | MyRowIdx |
| ----------- | --------- | --- | ---- | -------- |
| `table_a`   | `apple`   | 0   | 0    | apple0   |
| `table_a`   | `banana`  | 1   | 1    | banana1  |
| `table_a`   | `orange`  | 2   | 2    | orange2  |
| `table_b`   | `zebra`   | 3   | 4096 | zebra2   |
| `table_b`   | `bear`    | 4   | 4097 | bear3    |

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

While this example focuses on merge operations, **shifts can occur with any table operation** that works with live/refreshing data, including joins, updates, selects, and other operations. The same safety concerns about positional indices apply regardless of which operation triggers the shift.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not answered in our documentation, [join our Community](/slack) and ask it there. We are happy to help!
