# Finding 7 — a formula filter pushed through nested renaming views lost all but the last renaming

**Jira:** [DH-23637](https://deephaven.atlassian.net/browse/DH-23637) — filed for findings 6, 7 and 8 together
**Severity:** medium-high — a query that works in memory fails to compile against the same data on
disk, for a shape as ordinary as a rename followed by a duplicating `updateView`.
**Status:** **FIXED**, with regression test.
**Repro test:** `engine/table/src/test/java/io/deephaven/engine/table/impl/NestedDeferredViewRenameTest.java`
**Fuzzer seeds (8, all now passing):** `7177646707619336702L`, `273087235408284003L`,
`8429452456633422855L`, `8540064508133314173L`, `8922140309403778699L`, `-4605199251911937283L`,
`8576325184258344286L`, `7496982466862244149L`
**Share of the run that found it:** 6 of 36 failures.

## Symptom

```
disk table threw but memory table did not:
FormulaCompilationException: Formula compilation error for:
    Col0_r != null && dup0_Col0_r != null && Col0_r == dup0_Col0_r
  caused by: QueryLanguageParser$ParserResolutionFailure:
    Cannot find variable or class dup0_Col0_r
```

`dup0_Col0_r` is a column that plainly exists — the filter was built from the table's own definition.
Reduced with no parquet:

```java
final Table deferred = /* DeferredViewTable renaming Value -> Renamed */;
final Table withDuplicate = deferred.updateView("Dup = Renamed");
withDuplicate.where("Renamed != null && Dup != null && Renamed == Dup");   // throws
```

## Root cause

`ConditionFilter.renameFilter` does not rewrite the formula text. It keeps the formula as written and
carries the renaming alongside it, in `AbstractConditionFilter.outerToInnerNames`, which is applied
when the filter is initialized:

```java
public ConditionFilter renameFilter(Map<String, String> renames) {
    return new ConditionFilter(formula, renames);   // replaces the map
}
```

That is correct for a single hop and wrong for more than one, because it **replaces** the map instead
of folding the new mapping into the existing one. And a filter really is renamed repeatedly:
`DeferredViewTable.splitAndApplyFilters` pushes a filter down through each nested deferred view in
turn via `CopiedTableReference.getWithWhere`, renaming at every level. The stack from the reduction
shows three nested calls.

Walking the two-level case:

| level | that level's renames | filter's map after `renameFilter` | formula variables |
| --- | --- | --- | --- |
| `updateView("Dup = Renamed")` | `{Dup → Renamed}` | `{Dup → Renamed}` | `Renamed`, `Dup` — both resolve to `Renamed`, which exists here |
| `renameColumns("Renamed = Value")` | `{Renamed → Value}` | `{Renamed → Value}` — **`Dup → Renamed` gone** | `Dup` maps to nothing, and the source has no `Dup` |

So the filter finally initializes against the source definition holding only `Value`, with a map that
never mentions `Dup`. `FormulaAnalyzer.parseFormula` registers `Renamed` (from the map) and `Value`
(from the definition) as available variables; `Dup` is neither, and the parser reports it as unknown.

Two levels arise from something entirely ordinary — a rename, then a view that duplicates the renamed
column — which is exactly what the fuzzer generated with
`renameColumns("Col0_r = Col0")` followed by `updateView("dup0_Col0_r = Col0_r")`.

`MatchFilter.renameFilter` was **not** affected: it stores the column name directly and rewrites it on
each hop, so it composes naturally.

## Fix

A new `AbstractConditionFilter.composeRenames`, used by
[`ConditionFilter.renameFilter`](../../../../../../../../../../../engine/table/src/main/java/io/deephaven/engine/table/impl/select/ConditionFilter.java):

```java
return new ConditionFilter(formula, composeRenames(renames));
```

`composeRenames` follows each name the filter already maps through the new mapping, then adds the new
mapping's own entries for names the filter does not map:

```java
for (final Map.Entry<String, String> entry : outerToInnerNames.entrySet()) {
    final String intermediateName = entry.getValue();
    composed.put(entry.getKey(), renames.getOrDefault(intermediateName, intermediateName));
}
for (final Map.Entry<String, String> entry : renames.entrySet()) {
    composed.putIfAbsent(entry.getKey(), entry.getValue());
}
```

`putIfAbsent` in the second loop because a name the filter already maps refers to its own outer name
space, which takes precedence. In the walked example the composed map becomes
`{Dup → Value, Renamed → Value}`, and both formula variables resolve.

The empty-map case returns `renames` unchanged, so a first rename allocates nothing.

## Verification

- `NestedDeferredViewRenameTest`, 6 tests: the reported formula; the duplicate column alone; **three**
  levels of renaming, so the composition is shown to chain rather than merely handle two; a formula
  mixing a renamed column, its duplicate and an untouched one; the single-level case that always
  worked; and a cross-check that five filters return the same counts as when applied after `select()`
  materializes the views.
- Full `:engine-table:test` passes.
- All 8 fuzzer seeds pass and join `INTERESTING_SEEDS`. This closes the whole
  `OLD_FINDINGS.md` finding 3 cluster, which had recorded these as
  "Suspected cause (unverified) ... Worth checking `DeferredViewTable` filter splitting".

## Found on the way

`where("Dup == Dup_[0]")` — a filter naming both a column and its column-array form — crashes
`DeferredViewTable` with `IllegalStateException: Duplicate key Dup`. Different mechanism, same method;
taken up as finding 8.
