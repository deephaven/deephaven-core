# Finding 6 — a disjunction mixing a renamed and an un-renamed column tripped an internal assertion

**Severity:** medium — an ordinary query fails with an internal assertion, and only on the deferred
(uncoalesced) path, so the same filter works in memory and throws against a parquet-backed table.
**Status:** **FIXED**, with regression test.
**Repro test:** `engine/table/src/test/java/io/deephaven/engine/table/impl/MatchFilterPartialRenameTest.java`
**Fuzzer seed:** `-4715342832495625892L` (`maxTableSize=1000`) — **now passes end to end**.
**Share of the run that found it:** 6 of 36 failures.

## Symptom

```
disk table threw but memory table did not: io.deephaven.base.verify.AssertionFailure:
    Assertion failed: asserted newName != null, instead newName == null.
```

The fuzzer's case was three columns with `Col1` renamed to `Col1_r`, and the filter
`or(isNotNull(Col1_r), isNull(Col2))`. Reduced with no parquet at all:

```java
final Table deferred = new DeferredViewTable(resultDefinition, "d",
        new DeferredViewTable.TableReference(source), null,
        SelectColumn.from(Selectable.parse("Renamed = Value"), Selectable.parse("Other")), null);

deferred.where(Filter.or(
        Filter.isNotNull(ColumnName.of("Renamed")),   // renamed column
        Filter.isNull(ColumnName.of("Other"))));      // un-renamed column  -> AssertionFailure
```

## Root cause

Two consumers of one map disagreed about what an absent key means.

`DeferredViewTable.getFilters` splits each filter into a part it can push below the view and a part it
cannot. For the pushable part it builds the rename map from the filter's own columns:

```java
final Map<String, String> myRenames = Stream.of(filter.getColumns(), filter.getColumnArrays())
        .flatMap(Collection::stream)
        .filter(renames::containsKey)      // only the columns that ARE renamed
        .collect(Collectors.toMap(Function.identity(), renames::get));
```

So the map is **partial**: a filter column that passes through the view unchanged is simply absent.
That is fine, and intended — `AbstractConditionFilter` reads its copy of the map with
`outerToInnerNames.getOrDefault(name, name)` everywhere, treating absent as unchanged, so
`ConditionFilter.renameFilter` accepted a partial map happily.

`MatchFilter.renameFilter` instead required the map to be total:

```java
final String newName = renames.get(columnName);
Assert.neqNull(newName, "newName");
```

A single match filter never noticed, because `myRenames.isEmpty()` short-circuits the un-renamed case
and a lone renamed column produces a map containing exactly it. A **disjunction** is what brings the
two together: `visit(DisjunctiveFilter)` recurses into every sub-filter with the *same* map, computed
across the whole disjunction's columns. The sub-filter on `Col2` was therefore handed
`{Col1_r -> Col1}` — a map naming only its sibling's column — and the assertion fired.

Absent genuinely does mean unchanged here. A filter only reaches this code when `isPostView` is
false, i.e. none of its columns are view-computed, so every column is a real source column that is
either renamed (and in the map) or passed through under the same name.

Note that writing the filter as *text* did not reproduce it: `"a || b"` parses to a single
`ConditionFilter`, which tolerated the partial map. It takes a real `DisjunctiveFilter` of
`MatchFilter`s — which is what the declarative `Filter.or(...)` API and the fuzzer both build.

## Fix

[`MatchFilter.renameFilter`](../../../../../../../../../../../engine/table/src/main/java/io/deephaven/engine/table/impl/select/MatchFilter.java)
adopts the same contract as its sibling:

```java
final String newName = renames.getOrDefault(columnName, columnName);
```

The `Assert.neqNull` is gone, and the contract — *the map need not be total; an absent column is
unchanged* — is now stated in the javadoc of both `MatchFilter.renameFilter` and
`AbstractConditionFilter.renameFilter`, which is where its absence let the two implementations drift
apart. `DeferredViewTable` is unchanged: building the map partially is cheaper than padding it with
identity entries, and is now the documented input.

## Verification

- `MatchFilterPartialRenameTest`, 8 tests: the reported disjunction, in both column orders; a
  disjunction of equality match filters (the `values` branch rather than `strValues`); a conjunction
  nested inside a disjunction, to exercise the recursion more than one level deep; the two
  single-column controls; and a cross-check that every filter returns the same count as when applied
  after `select()` materializes the view. Five of the eight failed before the fix.
- A control records that a *top-level* `Filter.and(...)` never failed: `where` flattens it into
  separate filters, each with a map total over its own column.
- Full `:engine-table:test` passes, including `DeferredViewTableTest`'s 34 tests.
- Seed `-4715342832495625892L` passes, and joins `INTERESTING_SEEDS`.
