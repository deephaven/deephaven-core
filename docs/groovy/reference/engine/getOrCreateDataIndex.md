---
title: getOrCreateDataIndex
---

Retrieves an existing data index for a given table and specified key column(s), or creates the index if it does not already exist.

This method is part of the `io.deephaven.engine.table.impl.indexer.DataIndexer` class and is typically used via a static import.

## Syntax

```groovy syntax
index = getOrCreateDataIndex(someTable, "KeyColumn1")
index = getOrCreateDataIndex(someTable, "KeyColumn1", "KeyColumn2")
index = getOrCreateDataIndex(someTable, ["KeyColumn1", "KeyColumn2"] as String[])
```

## Parameters

<ParamTable>
<Param name="table" type="Table">
The table from which to retrieve or create the data index.
</Param>
<Param name="keyColumnNames" type="String...">
The name(s) of the key column(s) defining the index (varargs).
</Param>
<Param name="keyColumnNames" type="String[]">
The name(s) of the key column(s) defining the index (array).
</Param>
</ParamTable>

## Returns

Returns the existing or newly created `DataIndex` object.

## Examples

```groovy
import static io.deephaven.engine.table.impl.indexer.DataIndexer.getOrCreateDataIndex
import static io.deephaven.engine.table.impl.indexer.DataIndexer.hasDataIndex

// Create a source table
def sourceTable = emptyTable(100).update(
    "Category = \"Cat\" + (int)(i % 4)",
    "ID = i"
)

// Check if an index exists on Category (it doesn't yet)
println "Has index on Category BEFORE: ${hasDataIndex(sourceTable, "Category")}"  // Output: false

// Get or create an index on Category
def indexCategory = getOrCreateDataIndex(sourceTable, "Category")
println "Index on Category: ${indexCategory}"  // Output: (DataIndex object)

// Check if an index exists on Category (it does now)
println "Has index on Category AFTER: ${hasDataIndex(sourceTable, "Category")}"  // Output: true

// Get or create an index on Category and ID
def indexCategoryId = getOrCreateDataIndex(sourceTable, "Category", "ID")
println "Index on Category, ID: ${indexCategory}"  // Output: (DataIndex object)
println "Has index on Category, ID: ${hasDataIndex(sourceTable, "Category", "ID")}" // Output: true
```

## Related documentation

- [Create data indexes](../../how-to-guides/data-indexes.md)
- [`hasDataIndex`](./hasDataIndex.md)
- [`getDataIndex`](./getDataIndex.md)
- [`DataIndexer` Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/indexer/DataIndexer.html)
