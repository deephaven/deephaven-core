---
title: getDataIndex
---

Retrieves an existing data index for a given table and specified key column(s). It can optionally create the index if it does not already exist.

This method is part of the `io.deephaven.engine.table.impl.indexer.DataIndexer` class and is typically used via a static import.

## Syntax

```groovy syntax
index = getOrCreateDataIndex(someTable, "KeyColumn1") // Create if missing
index = getDataIndex(someTable, "KeyColumn1", "KeyColumn2") // Retrieve only if exists
index = getOrCreateDataIndex(someTable, "KeyColumn1", "KeyColumn2") // Create if missing
```

## Parameters

<ParamTable>
<Param name="table" type="Table">
The table from which to retrieve the data index.
</Param>
<Param name="createIfAbsent" type="boolean">
If `true`, creates the data index if it doesn't exist. If `false`, returns `null` if the index doesn't exist.
</Param>
<Param name="keyColumnNames" type="String...">
The name(s) of the key column(s) defining the index (varargs).
</Param>
<Param name="keyColumnNames" type="String[]">
The name(s) of the key column(s) defining the index (array).
</Param>
</ParamTable>

## Returns

Returns the `DataIndex` object if found or created. Returns `null` if `createIfAbsent` is `false` and the index does not exist.

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

### Example 2: Retrieve an index, create if absent

```groovy
import static io.deephaven.engine.table.impl.indexer.DataIndexer.getDataIndex
import static io.deephaven.engine.table.impl.indexer.DataIndexer.hasDataIndex

// Create a source table
def sourceTable2 = emptyTable(50).update(
    "Symbol = \"SYM\" + (int)(i % 3)",
    "Price = i * 1.1"
)

// Attempt to get an index on Symbol
println "Has index on Symbol BEFORE: ${hasDataIndex(sourceTable2, "Symbol")}" // Output: false
def indexSymbol_create = getOrCreateDataIndex(sourceTable2, "Symbol") // Create if missing
println "Index on Symbol (create): ${indexSymbol_create}"  // Output: (DataIndex object)
println "Has index on Symbol AFTER: ${hasDataIndex(sourceTable2, "Symbol")}" // Output: true
```

## Related documentation

- [Create data indexes](../../how-to-guides/data-indexes.md)
- [`hasDataIndex`](./hasDataIndex.md)
- [`getOrCreateDataIndex`](./getOrCreateDataIndex.md) (To be created)
- [`DataIndexer` Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/indexer/DataIndexer.html)
