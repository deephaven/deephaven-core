---
title: How efficient are my table selection operations?
sidebar_label: How efficient are my table selection operations?
---

_How much memory do my [`select`](../table-operations/select/select.md) and [`update`](../table-operations/select/update.md) operations waste? Are they efficient?_

[`select`](../table-operations/select/select.md) and [`update`](../table-operations/select/update.md) create in-memory columns of data, whereas [`view`](../table-operations/select/view.md), [`updateView`](../table-operations/select/update-view.md), and [`lazyUpdate`](../table-operations/select/lazy-update.md) do not.

Deephaven recommends using [`select`](../table-operations/select/select.md) and [`update`](../table-operations/select/update.md) when calculations are expensive, or the results are needed for multiple downstream operations. These methods can cause issues when data gets sufficiently large, as they can consume a lot of memory.

How efficient are your [`select`](../table-operations/select/select.md) and [`update`](../table-operations/select/update.md) operations? This page gives you a query to run in the Deephaven console to find out.

To better understand how efficient your memory usage is, execute the following Groovy code:

```groovy skip-test
import gnu.trove.TCollections
import gnu.trove.set.hash.TLongHashSet
import gnu.trove.map.hash.TObjectIntHashMap
import gnu.trove.impl.Constants
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource
import io.deephaven.util.type.TypeUtils

simpleSizeLookupBuilder = new TObjectIntHashMap<>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, 0)
simpleSizeLookup = TCollections.unmodifiableMap(simpleSizeLookupBuilder)

accumulateBlocks = { long rowKey, TLongHashSet longSet -> longSet.add(rowKey >> 10) }
getSimpleDataRecordSizeInBytes = { type -> simpleSizeLookup.get(TypeUtils.getBoxedType(type)) }

checkSparsity = { bindingVarEntry, list ->
    def varName = bindingVarEntry.getKey()
    def varValue = bindingVarEntry.getValue()
    if (varValue instanceof Table) {
        def s = varValue.getColumnSources().stream()
        s = s.filter({cs -> cs instanceof SparseArrayColumnSource})
        s = s.map({cs -> cs.getType()})
        s = s.mapToInt({cst -> int result = getSimpleDataRecordSizeInBytes(cst); return result == 0 ? 8 : result; })
        colByteSizeSum = s.sum()
        if (colByteSizeSum > 0) {
            def longSet = new TLongHashSet()
            long idealBlocks = ((varValue.getRowSet().size() + 1023) / 1024)
            varValue.getRowSet().forAllRowKeys({rowKey -> accumulateBlocks(rowKey, longSet)})
            long actualBlocks = longSet.size()
            double efficiency = (idealBlocks / actualBlocks) * 100
            println varName + ": ideal blocks=" + idealBlocks + ", actual blocks=" + actualBlocks + ", efficiency=" + efficiency + "%"
            list.add([varName, efficiency])
        }
    }

}

bindingVars = new HashMap(getBinding().getVariables())
resultList = []
bindingVars.entrySet().forEach({e -> checkSparsity(e, resultList)})
resultList.sort({e1, e2 -> -1 * Long.compare(e1.get(1), e2.get(1))})
println resultList
```

An efficiency of 100% means that the selection operations are perfectly efficient - there is no wasted memory. This is the ideal case.

> **Tip:** Perfect efficiency is not always possible, especially with complex or refreshing tables. Aim for higher efficiency, but don't worry if you can't reach 100% in every case.

An important detail about this code is that it checks memory efficiency for columns backed by a [`SparseArrayColumnSource`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/sources/SparseArrayColumnSource.html). This does not apply to all columns. The simplest way to construct a table with columns backed by these is to create a [non-flat](../table-operations/metadata/isFlat.md) refreshing table with in-memory columns. You can force all columns into memory with [`select`](../table-operations/select/select.md).

```groovy ticking-table order=null
t1 = timeTable("PT1s").update("X = randomDouble(-1, 1)").where("X > -0.75")
```

```text
t1: ideal blocks=1, actual blocks=1, efficiency=100.0%
[[t1, 100.0]]
```

> [!NOTE]
> A "flat" table has a flat row set. That means the row set increases monotonically from 0 to the number of rows minus one with no gaps.

## Related documentation

- [Select, view, and update data](../../how-to-guides/use-select-view-update.md)
- [`isFlat`](../table-operations/metadata/isFlat.md)
