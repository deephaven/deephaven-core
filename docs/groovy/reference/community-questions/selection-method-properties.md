---
title: How efficient are my table selection operations?
---

_How much memory do my [`select`](../table-operations/select/select.md) and [`update`](../table-operations/select/update.md) operations waste? Are they efficient?_

[`select`](../table-operations/select/select.md) and [`update`](../table-operations/select/update.md) create in-memory columns of data. [`view`](../table-operations/select/view.md) and [`updateView`](../table-operations/select/update-view.md) do not store results, and [`lazyUpdate`](../table-operations/select/lazy-update.md), by default, stores a cache of results keyed by input values.

Deephaven recommends using [`select`](../table-operations/select/select.md) and [`update`](../table-operations/select/update.md) when calculations are expensive, or the results are needed for multiple downstream operations. These methods can cause issues when data gets sufficiently large, as they can consume a lot of memory.

Run the following Groovy script in the console to measure how efficiently your tables use memory.

### What the script measures

The script checks only columns backed by a [`SparseArrayColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/sources/SparseArrayColumnSource.html), which stores data in blocks of 1,024 consecutive row keys. Tables that have computed in-memory columns (created by [`select`](../table-operations/select/select.md) or [`update`](../table-operations/select/update.md)) typically use this column source. The script skips tables that have no `SparseArrayColumnSource` columns and tables with an empty row set.

The script computes three values for each table:

- **Ideal blocks**: the fewest blocks the table's rows could fit in.
- **Actual blocks**: the number of blocks the table's row keys actually occupy.
- **Efficiency**: ideal blocks divided by actual blocks, as a percentage. An efficiency of 100% means the table's row keys are packed into the fewest possible blocks.

### Example setup

First, create a table with `SparseArrayColumnSource` columns so the script has something to measure:

```groovy ticking-table order=null
t1 = timeTable("PT1s").update("X = randomDouble(-1, 1)").where("X > -0.75")
```

### Diagnostic script

```groovy skip-test
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource

checkEfficiency = { varName, table ->
    def hasSparse = table.getColumnSources().any { it instanceof SparseArrayColumnSource }
    if (!hasSparse) return null

    def rowSet = table.getRowSet()
    if (rowSet.isEmpty()) return null

    def blockSet = new LongOpenHashSet()
    rowSet.forAllRowKeys({ rowKey -> blockSet.add(rowKey >> 10) })

    long idealBlocks = (rowSet.size() + 1023) / 1024
    long actualBlocks = blockSet.size()
    double efficiency = ((double) idealBlocks / actualBlocks) * 100

    println varName + ": ideal blocks=" + idealBlocks + ", actual blocks=" + actualBlocks + ", efficiency=" + efficiency + "%"
    return [varName, efficiency]
}

def resultList = []
new HashMap(getBinding().getVariables()).each { name, value ->
    if (value instanceof Table) {
        def result = checkEfficiency(name, value)
        if (result != null) resultList.add(result)
    }
}
resultList.sort { e1, e2 -> Double.compare(e2[1], e1[1]) }
println resultList
```

Once `t1` has accumulated some rows, running the script prints output like:

```text
t1: ideal blocks=1, actual blocks=1, efficiency=100.0%
[[t1, 100.0]]
```

> [!TIP]
> Perfect efficiency is not always possible, especially with complex or refreshing tables. Aim for higher efficiency, but don't worry if you can't reach 100% in every case.

## Related documentation

- [Select, view, and update data](../../how-to-guides/use-select-view-update.md)
- [`isFlat`](../table-operations/metadata/isFlat.md)
