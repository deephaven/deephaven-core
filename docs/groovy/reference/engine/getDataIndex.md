---
title: getDataIndex
---

Retrieves an existing data index for a table and key column(s), or returns `null` if none exists. It never creates an index. To create one, use [`getOrCreateDataIndex`](./getOrCreateDataIndex.md).

This method is part of the `io.deephaven.engine.table.impl.indexer.DataIndexer` class and is typically used via a static import.

## Syntax

```groovy syntax
index = getDataIndex(someTable, "KeyColumn1")
index = getDataIndex(someTable, "KeyColumn1", "KeyColumn2")
```

## Parameters

<ParamTable>
<Param name="table" type="Table">
The table from which to retrieve the data index.
</Param>
<Param name="keyColumnNames" type="String...">
The name(s) of the key column(s) defining the index (varargs).
</Param>
<Param name="keyColumnNames" type="Collection<String>">
The name(s) of the key column(s) defining the index (collection).
</Param>
</ParamTable>

## Returns

The `DataIndex`, or `null` if no usable index exists.

## Examples

### Example 1: Retrieve an index, don't create if absent

```groovy
import static io.deephaven.engine.table.impl.indexer.DataIndexer.getDataIndex
import static io.deephaven.engine.table.impl.indexer.DataIndexer.getOrCreateDataIndex
import static io.deephaven.engine.table.impl.indexer.DataIndexer.hasDataIndex

// Create a source table
def sourceTable = emptyTable(100).update(
    "Key1 = (int)i % 10",
    "Value = i"
)

// Attempt to get an index on Key1
def indexKey1_noCreate = getDataIndex(sourceTable, "Key1") // Retrieve only if exists
println "Index on Key1 (no create): ${indexKey1_noCreate}"  // Output: null
println "Has index on Key1 after attempt: ${hasDataIndex(sourceTable, "Key1")}" // Output: false

// Now, explicitly create an index on Key1
def actualIndexKey1 = getOrCreateDataIndex(sourceTable, "Key1")

// Attempt to get the index on Key1 again
def indexKey1_exists = getDataIndex(sourceTable, "Key1") // Retrieve only if exists
println "Index on Key1 (exists): ${indexKey1_exists}"  // Output: (DataIndex object)
println "Is indexKey1_exists the same as actualIndexKey1? ${indexKey1_exists == actualIndexKey1}" // Output: true
```

### Example 2: Create an index if absent

```groovy
import static io.deephaven.engine.table.impl.indexer.DataIndexer.getOrCreateDataIndex
import static io.deephaven.engine.table.impl.indexer.DataIndexer.hasDataIndex

// Create a source table
def sourceTable2 = emptyTable(50).update(
    "Symbol = \"SYM\" + (int)(i % 3)",
    "Price = i * 1.1"
)

// Create an index on Symbol with getOrCreateDataIndex, since getDataIndex never creates one
println "Has index on Symbol BEFORE: ${hasDataIndex(sourceTable2, "Symbol")}" // Output: false
def indexSymbol_create = getOrCreateDataIndex(sourceTable2, "Symbol") // Create if missing
println "Index on Symbol (create): ${indexSymbol_create}"  // Output: (DataIndex object)
println "Has index on Symbol AFTER: ${hasDataIndex(sourceTable2, "Symbol")}" // Output: true
```

## Related documentation

- [Create data indexes](../../how-to-guides/data-indexes.md)
- [`hasDataIndex`](./hasDataIndex.md)
- [`getOrCreateDataIndex`](./getOrCreateDataIndex.md)
- [`DataIndexer` Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/indexer/DataIndexer.html)
