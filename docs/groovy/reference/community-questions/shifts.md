---
title: How do row keys and positional indices behave during table operations?
sidebar_label: How do row keys and positional indices behave during table operations?
---

_When working with tables in Deephaven, I understand that "shifts" can happen when tables grow beyond their allocated row key slots. How do the row key (k) and positional index (i) attributes change during these shifts, and what impact does this have on downstream operations?_

**Shifts can occur with any table operation that works with live/refreshing data** - including merges, joins, updates, selects, and other operations. When you merge tables in Deephaven, the engine pre-allocates row key slots for each constituent table. Understanding how row keys (`k`) and positional indices (`i`) behave during growth and shifts is crucial for working with formulas that reference these attributes.

## Key terms

Before diving into the details, let's define the key concepts:

- **Row key (`k`)**: A unique identifier assigned to each row in a table. Row keys are used internally by the engine to track rows and are typically sequential integers starting from 0.

- **Positional index (`i` and `ii`)**: The current position of a row within a table, starting from 0 for the first row. Unlike row keys, positional indices always reflect the current order of rows.

- **Shifts**: When rows need to be inserted between existing rows (such as in sorted tables), Deephaven communicates row shift data that moves ranges of row keys by a positive or negative shift delta. This allows the engine to maintain proper ordering without spurious modifications. The row data itself doesn't change, but the row keys do. See [Table update model - Shifts](../../conceptual/table-update-model.md#shifts) for detailed technical information.

- **Constituent tables**: The individual tables that make up a larger table structure, such as tables being merged, joined, or subtables within a partitioned table. Each constituent table gets its own allocated range of row keys in the result.

- **Row key slots**: Pre-allocated ranges of row keys reserved for each constituent table. For example, table A might get slots 0-4095, while table B gets slots 4096-8191.

- **Refreshing tables**: Tables that can change over time (live data), as opposed to static tables that never change after creation. This includes:

  - tables created from live data sources (like `timeTable`, Kafka streams, etc.).
  - tables that are the result of operations on refreshing tables.
  - any table where rows can be added, removed, or shifted after creation.
    In contrast, **[static tables](../../conceptual/table-types.md#static-tables)** never change after creation - they contain fixed data that remains constant.

- **Append-only tables**: A special type of refreshing table where new rows are only added (never moved or deleted), making positional indices safe to use. See [Table types - Append-only](../../conceptual/table-types.md#specialization-1-append-only) for more information.

> [!CAUTION] > **Formulas using positional indices (`i`, `ii`, `k`) or column array variables are unsafe on refreshing tables and will cause `IllegalArgumentException` errors.** The engine blocks these formulas by default to prevent incorrect results. While you can override this with the system property `io.deephaven.engine.table.impl.select.AbstractFormulaColumn.allowUnsafeRefreshingFormulas=true`, doing so **will produce incorrect results** when table shifts occur. Additionally, any table operations that add or remove rows can also cause positional indices to become incorrect. **Note that shifts can happen with any table operation, not just merges** - including joins, updates, selects, and other operations on live/refreshing tables. These formulas are safe to use on static tables or append-only tables (like `timeTable("PT1s").update("X = ii")`) where shifts cannot happen because rows are only added, never moved.

## Row key allocation and shifts

When you create a merged table, the engine reserves contiguous ranges of row keys for each constituent table:

```groovy test-set=1 order=tableA,tableB,myMergedTable
// Create two tables with sample data
tableA = newTable(
    col("someValue", "apple", "banana")
)
tableB = newTable(
    col("someValue", "zebra", "bear")
)

// Merge them
myMergedTable = merge(
    tableA.view("sourceTable=`tableA`", "someValue"),
    tableB.view("sourceTable=`tableB`", "someValue")
)
```

Initially, the engine might allocate:

- **`tableA`**: row key slots \[0, 4095\]
- **`tableB`**: row key slots \[4096, 8191\]

## Initial state with few rows

When each table gets one row, the merged table looks like:

| sourceTable | someValue | i | k    |
| ----------- | --------- | - | ---- |
| `tableA`    | `apple`   | 0 | 0    |
| `tableA`    | `banana`  | 1 | 1    |
| `tableB`    | `zebra`   | 2 | 4096 |
| `tableB`    | `bear`    | 3 | 4097 |

## What happens during a shift

If `tableA` grows beyond its allocated 4096 slots, the engine must shift `tableB` to make room. After adding 4096 new rows to `tableA`, the allocation becomes:

- **`tableA`**: row key slots \[0, 8191\]
- **`tableB`**: row key slots \[8192, 12287\]

The merged table now looks like:

| sourceTable | someValue   | i    | k    |
| ----------- | ----------- | ---- | ---- |
| `tableA`    | `apple`     | 0    | 0    |
| `tableA`    | `banana`    | 1    | 1    |
| ...         | ...         | ...  | ...  |
| `tableA`    | `avocado`   | 4095 | 4095 |
| `tableA`    | `peach`     | 4096 | 4096 |
| `tableA`    | `blueberry` | 4097 | 4097 |
| `tableB`    | `zebra`     | 4098 | 8192 |
| `tableB`    | `bear`      | 4099 | 8193 |

## Key behavior: row keys change, but row data doesn't

The critical insight is that the `zebra` and `bear` rows **were not modified** - they were simply shifted to new row key positions. Their row keys changed from 4096/4097 to 8192/8193, but the actual row data remains unchanged.

## Impact on formulas using positional index (i)

This behavior has important implications for formulas that reference the positional index `i`. Consider what would happen if you tried to add a formula that depends on `i`:

```groovy should-fail
// This will fail with IllegalArgumentException unless you enable unsafe formulas
// DO NOT enable unsafe formulas unless you understand the risks (see DANGER banner above)
def myMergedTableWithIdx = myMergedTable.update("MyRowIdx = someValue + i")
```

This demonstrates the key insight: downstream operations don't know that the positional index `i` changed for the `zebra`/`bear` rows during the shift. **This is why the engine blocks such formulas by default** - they would produce incorrect results after shifts occur.

**Before the shift:**

| sourceTable | someValue | i | k    | MyRowIdx |
| ----------- | --------- | - | ---- | -------- |
| `tableA`    | `apple`   | 0 | 0    | apple0   |
| `tableA`    | `banana`  | 1 | 1    | banana1  |
| `tableB`    | `zebra`   | 2 | 4096 | zebra2   |
| `tableB`    | `bear`    | 3 | 4097 | bear3    |

**After adding one row to `tableA`:**

| sourceTable | someValue | i | k    | MyRowIdx |
| ----------- | --------- | - | ---- | -------- |
| `tableA`    | `apple`   | 0 | 0    | apple0   |
| `tableA`    | `banana`  | 1 | 1    | banana1  |
| `tableA`    | `orange`  | 2 | 2    | orange2  |
| `tableB`    | `zebra`   | 3 | 4096 | zebra2   |
| `tableB`    | `bear`    | 4 | 4097 | bear3    |

Notice that both the new `orange` row and the existing `zebra` row appear to have the same positional index behavior, but the `zebra` and `bear` rows retain their original `MyRowIdx` values ("zebra2", "bear3") because those rows were never actually modified.

**After a major shift (4098 rows in `tableA`):**

| sourceTable | someValue   | i    | k    | MyRowIdx      |
| ----------- | ----------- | ---- | ---- | ------------- |
| `tableA`    | `apple`     | 0    | 0    | apple0        |
| `tableA`    | `banana`    | 1    | 1    | banana1       |
| `tableA`    | `orange`    | 2    | 2    | orange2       |
| ...         | ...         | ...  | ...  | ...           |
| `tableA`    | `avocado`   | 4095 | 4095 | avocado4095   |
| `tableA`    | `peach`     | 4096 | 4096 | peach4096     |
| `tableA`    | `blueberry` | 4097 | 4097 | blueberry4097 |
| `tableB`    | `zebra`     | 4098 | 8192 | zebra2        |
| `tableB`    | `bear`      | 4099 | 8193 | bear3         |

The `zebra` and `bear` rows maintain their original `MyRowIdx` values because the engine never re-evaluates formulas for rows that weren't actually modified - this is the key performance benefit of the shift mechanism.

## Why shifts exist

Shifts are a performance optimization that allows the engine to avoid re-evaluating formulas, joins, and aggregations when rows are simply moved to accommodate table growth. Only when row data is actually modified does the engine need to recalculate dependent operations.

For more detailed information about shifts and the table update model, see [Table update model - Shifts](../../../conceptual/table-update-model.md#shifts).

While this example focuses on merge operations, **shifts can occur with any table operation** that works with live/refreshing data, including joins, updates, selects, and other operations. The same safety concerns about positional indices apply regardless of which operation triggers the shift. Note that even without shifts, any operations that add or remove rows from refreshing tables can similarly cause positional indices (`i`, `ii`) to become incorrect in downstream operations.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not answered in our documentation, [join our Community](/slack) and ask it there. We are happy to help!
