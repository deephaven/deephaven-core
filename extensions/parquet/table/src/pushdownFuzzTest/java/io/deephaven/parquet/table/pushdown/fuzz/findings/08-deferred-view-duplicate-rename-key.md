# Finding 8 — a filter naming both a column and its column-array form crashed the deferred-view filter split

**Jira:** [DH-23637](https://deephaven.atlassian.net/browse/DH-23637) — filed for findings 6, 7 and 8 together
**Severity:** medium — a legal filter fails outright with an internal `IllegalStateException`, and only
against a deferred (uncoalesced) table, so it works in memory and throws on disk.
**Status:** **FIXED**, with regression test.
**Repro test:** `engine/table/src/test/java/io/deephaven/engine/table/impl/DeferredViewColumnArrayRenameTest.java`
**How it was found:** not by a fuzzer seed — it broke a test method I had written for finding 7. The
fuzzer does not currently generate column-array (`Col_`) references, so it could not have found this
one; see *Fuzzer coverage* below.

## Symptom

```
java.lang.IllegalStateException: Duplicate key Renamed (attempted merging values Value and Value)
    at java.util.stream.Collectors.duplicateKeyException(Collectors.java:135)
    at io.deephaven.engine.table.impl.DeferredViewTable.applyFilterRenamings
```

```java
final Table deferred = /* DeferredViewTable renaming Value -> Renamed */;
deferred.where("Renamed == Renamed_[0]");   // throws
```

## Root cause

`DeferredViewTable.applyFilterRenamings` builds each filter's rename map by concatenating the two
lists of names the filter reports:

```java
final Map<String, String> myRenames = Stream.of(filter.getColumns(), filter.getColumnArrays())
        .flatMap(Collection::stream)
        .filter(renames::containsKey)
        .collect(Collectors.toMap(Function.identity(), renames::get));
```

A filter such as `"Renamed == Renamed_[0]"` uses `Renamed` as a value *and* `Renamed_` as an array, so
`AbstractConditionFilter` reports `Renamed` in **both** lists — `getColumns()` from `usedColumns`, and
`getColumnArrays()` from `usedColumnArrays` with the `_` suffix stripped. Each list is individually
`distinct()`, but nothing dedupes across them, and `Collectors.toMap` has no merge function, so the
second occurrence throws.

Nothing was ambiguous: both occurrences map the column to the same inner name, which the exception
message states itself — *"attempted merging values Value and Value"*. The duplicate was purely a
crash.

## Fix

[`DeferredViewTable.applyFilterRenamings`](../../../../../../../../../../../engine/table/src/main/java/io/deephaven/engine/table/impl/DeferredViewTable.java)
adds `.distinct()` before the collector, with a comment recording why a name can appear twice.

`.distinct()` rather than a merge function: a merge function would suggest the two values might differ
and that one had been chosen, when in fact they are always equal. Deduplicating the *names* says what
is actually true.

## Fuzzer coverage

`FuzzFilters` builds filters over column values only; it never emits a `Col_` column-array reference,
which is why eight runs never produced this. That is a genuine gap in the bench rather than a property
of the code under test — column arrays go through the same rename mapping and the same
`DeferredViewTable` split as ordinary column references, and finding 7 shows that path is where the
defects are. Worth adding a column-array filter generator.

## Verification

- `DeferredViewColumnArrayRenameTest`, 6 tests: the reported filter; the array form alone (naming the
  column only via `getColumnArrays`); two renamed columns each used both ways; a renamed column used
  both ways alongside an un-renamed one; a non-zero array offset, to confirm the array really is the
  renamed column; and a cross-check that five filters return the same counts as when applied after
  `select()`.
- Reverting the one-line fix makes five of the six fail, so the test does pin the defect.
- Full `:engine-table:test` passes.
