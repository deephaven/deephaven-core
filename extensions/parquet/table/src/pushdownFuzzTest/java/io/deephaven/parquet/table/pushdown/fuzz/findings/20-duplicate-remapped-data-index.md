# Finding 20 — aliasing an indexed column threw when a second column was also indexed

**Severity:** medium-high — a `select` that aliases an indexed column fails outright on a table carrying
a second data index. It throws rather than returning wrong data, but the query is legitimate and there is
no workaround short of dropping an index.
**Status:** **FIXED**, with regression test.
**Repro test:** `extensions/parquet/table/src/test/java/io/deephaven/parquet/table/AliasedColumnDataIndexPropagationTest.java`
**Fuzzer seed:** `6547813402343559854L`

## Symptom

```
java.lang.IllegalStateException: Attempted to add a duplicate index
    RemappedDataIndex-1274547241[1] for key columns [Col2]
  at io.deephaven.engine.table.impl.indexer.DataIndexer.addDataIndex(DataIndexer.java:327)
  at io.deephaven.engine.table.impl.QueryTable.lambda$propagateDataIndexes$44(QueryTable.java:1952)
  at io.deephaven.engine.table.impl.QueryTable.propagateDataIndexes(QueryTable.java:1942)
  at io.deephaven.engine.table.impl.QueryTable.selectOrUpdate(QueryTable.java:1743)
  at io.deephaven.engine.table.impl.UncoalescedTable.select(UncoalescedTable.java:224)
```

The fuzzer reported it as `building the in-memory oracle failed`, because the oracle is
`diskTable.select()`. It is not an oracle problem: the same `select` is what any user would write.

## Reduction

```java
ParquetTools.writeKeyValuePartitionedTable(
        source.partitionBy("Part"),                              // Part is auto-indexed
        dest,
        ParquetInstructions.builder().addIndexColumns("Key").build());

ParquetTools.readTable(dest)
        .updateView("KeyAlias = Key")                            // a second alias of Key
        .select();                                               // throws
```

Three ingredients, established by bisecting the variants:

| Ingredient | Needed? |
| --- | --- |
| An index on the aliased column (`Key`) | **yes** — without it, `Key` never enters the maps |
| An index on a second column (`Part`, here automatic from partitioning) | **yes** — it is the one added twice |
| Written to a **single location** | **yes** — see below |
| Row count | no — 1, 2, 3 and 200 all fail |
| A read-instruction column rename | no |
| Coalescing before the `select` | no |

## Root cause

`QueryTable.propagateDataIndexes` carries the source table's data indexes onto a `select` result by
remapping their key column sources. `select` may produce several aliases of one source column, and an
index can only name one of them, so the method builds a **list** of old-to-new maps — one per unique
combination of alias choices — and then remaps every index once per map:

```java
for (final DataIndex dataIndex : dataIndexes) {
    oldToNewMaps.forEach(map -> {
        if (Collections.disjoint(dataIndex.keyColumnNamesByIndexedColumn().keySet(), map.keySet())) {
            return;
        }
        dataIndexer.addDataIndex(dataIndex.remapKeyColumns(map));
    });
}
```

The `disjoint` guard skips an index that shares no column with the map at all, but that is not the
condition that matters. A `RemappedDataIndex` is determined by the mappings for **its own** key columns —
`RemappedDataIndex`'s constructor looks up only `sourceIndex.keyColumnNamesByIndexedColumn()` in the map
and ignores every other entry. So two maps that differ *only* in columns some index does not key on
describe the *same* remapped index for it.

That is exactly what one aliased indexed column plus one un-aliased indexed column produces. `Key`'s two
aliases fork the list into two maps; `Part` is mapped identically in both:

```
map A: { Key -> Key_result,      Part -> Part_result }
map B: { Key -> KeyAlias_result, Part -> Part_result }
```

Remapping `Key`'s index gives two distinct indexes, one per alias — correct, and the reason the list
exists. Remapping `Part`'s index gives the same index twice, and the second `addDataIndex` is correctly
rejected.

**Why a single location matters.** `propagateDataIndexes` returns immediately unless
`rowSet == resultTable.getRowSet()`. A regioned table spanning several locations has a non-flat row set,
so `select` flattens, the row sets differ, and propagation never runs — which is why the multi-location
form of the identical query worked. One location gives a flat row set that `select` preserves. This also
explains why the defect resisted an in-memory reproduction: `select` on an in-memory table always
produces its own row set.

## Fix

`engine/table/src/main/java/io/deephaven/engine/table/impl/QueryTable.java` — restrict each map to the
index's own key columns and keep the distinct restrictions, which is precisely the set of genuinely
different remappings:

```java
final Set<Map<ColumnSource<?>, ColumnSource<?>>> distinctKeyColumnMaps = new LinkedHashSet<>();
for (final Map<ColumnSource<?>, ColumnSource<?>> map : oldToNewMaps) {
    final Map<ColumnSource<?>, ColumnSource<?>> keyColumnMap = new HashMap<>();
    map.forEach((oldSource, newSource) -> {
        if (indexedKeySources.contains(oldSource)) {
            keyColumnMap.put(oldSource, newSource);
        }
    });
    if (!keyColumnMap.isEmpty()) {
        distinctKeyColumnMaps.add(keyColumnMap);
    }
}
distinctKeyColumnMaps.forEach(map -> dataIndexer.addDataIndex(dataIndex.remapKeyColumns(map)));
```

An empty restriction subsumes the old `Collections.disjoint` check: no key column of this index was
remapped, so there is nothing to do. Deduplication is on the restricted map, so the number of indexes
added is the number of genuinely distinct remappings — no more, and importantly no fewer.

Deliberately *not* done: making `addDataIndex` tolerate a duplicate. The caller was asking for the same
index twice; silently accepting that hides the redundancy and leaves `addDataIndex`'s contract weaker for
every other caller.

## Verification

- `AliasedColumnDataIndexPropagationTest`, 10 tests. **Before the fix 6 fail and the 4 controls pass**
  (several locations, no second index, no alias, and alias-in-`select`); after, all 10 pass. The controls
  matter here: the fix removes work, so a test that only checks "no longer throws" would also pass if the
  fix had thrown the indexes away.
- `everyAliasStillCarriesAnIndex` asserts an index survives for `Key`, both of its aliases, and `Part`.
- `indexedFiltersStillAgree` checks 8 filters over the aliases against the same data in memory with no
  indexes at all, so a surviving-but-wrong index would be caught rather than assumed correct.
- The fuzzer seed passes.
- `:engine-table:test` and `:engine-table:testParallel` filtered to the data-index, indexer, select and
  `QueryTable` suites: 0 failures.

## Note on `aliasIntroducedBySelectItself`

`select("Key", "KeyAlias = Key", ...)` — the alias introduced by the `select` rather than by a preceding
`updateView` — was verified **not** to reproduce the defect before the fix. It is retained as a control,
not as a second repro; the write-up says so rather than implying the two spellings are equivalent.
