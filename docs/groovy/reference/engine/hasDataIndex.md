---
title: hasDataIndex
---

Checks if a data index exists for a given table and specified key column(s).

This method is part of the `io.deephaven.engine.table.impl.indexer.DataIndexer` class and is typically used via a static import.

## Syntax

```groovy syntax
result = hasDataIndex(someTable, "KeyColumn1")
result = hasDataIndex(someTable, "KeyColumn1", "KeyColumn2")
result = hasDataIndex(someTable, ["KeyColumn1", "KeyColumn2"] as String[])
```

## Parameters

<ParamTable>
<Param name="table" type="Table">
The table to check for an existing data index.
</Param>
<Param name="keyColumnNames" type="String...">
The name(s) of the key column(s) defining the index (varargs).
</Param>
<Param name="keyColumnNames" type="String[]">
The name(s) of the key column(s) defining the index (array).
</Param>
</ParamTable>

## Returns

Returns `true` if a data index exists for the table and key column(s); `false` otherwise.

## Examples

The following example demonstrates how to use `hasDataIndex` to check for the existence of data indexes on a source table.

```groovy
import static io.deephaven.engine.table.impl.indexer.DataIndexer.getOrCreateDataIndex
import static io.deephaven.engine.table.impl.indexer.DataIndexer.hasDataIndex

// Create a source table
def sourceTable = emptyTable(100).update(
    "Key1 = (int)i % 10",
    "Key2 = (int)i % 5",
    "Value = i"
)

// Create an index on Key1
def indexKey1 = getOrCreateDataIndex(sourceTable, "Key1")

// Check if an index exists for Key1
def hasIndexOnKey1 = hasDataIndex(sourceTable, "Key1")
println "Index exists on Key1: ${hasIndexOnKey1}"  // Output: true

// Check if an index exists for Key2 (it doesn't yet)
def hasIndexOnKey2 = hasDataIndex(sourceTable, "Key2")
println "Index exists on Key2: ${hasIndexOnKey2}"  // Output: false

// Check if an index exists for Key1 and Key2 (it doesn't yet)
def hasIndexOnKey1Key2 = hasDataIndex(sourceTable, "Key1", "Key2")
println "Index exists on Key1, Key2: ${hasIndexOnKey1Key2}" // Output: false
```

## Related documentation

- [Create data indexes](../../how-to-guides/data-indexes.md)
- [`getDataIndex`](./getDataIndex.md)
- [`getOrCreateDataIndex`](./getOrCreateDataIndex.md) (To be created)
- [`DataIndexer` Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/indexer/DataIndexer.html)
