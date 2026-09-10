# Finding 19 — a case-insensitive match filter mishandled a null match value at every arity

**Severity:** high — `where("S icase in null, `a`")` threw a `NullPointerException` for one, two or three
match values, and **silently returned the wrong rows** for four or more. No parquet, no pushdown switch, no
nulls in the data required.
**Status:** **FIXED**, with regression test.
**Repro test:** `engine/table/src/test/java/io/deephaven/engine/table/impl/select/CaseInsensitiveMatchFilterNullTest.java`
**Fuzzer seeds (4, all now passing):** `-3193954278432445066L`, `-3417280133301762829L`,
`-4791489248932458532L`, `-8415955675519703733L` — 4 of the 7 failures in a 12,470-case sweep.

## Symptom

Two different symptoms from one root cause, selected by how many values the filter was given.

**One, two or three values — throws:**

```
io.deephaven.engine.exceptions.TableInitializationException:
    Error while initializing where([Col1 icase not in [null, null, null]]):
    an exception occurred while performing the initial filter
  caused by: java.lang.NullPointerException:
    Cannot invoke "String.equalsIgnoreCase(String)" because "this.value1" is null
```

**Four or more values — wrong answer, no exception:** the null is dropped from the value list, so
`S icase in null, `aa`, `BB`, `cc`` does not match the null rows, and `icase not in` returns them.

The fuzzer reported these as `memory table threw but disk table did not` and — for
`-8415955675519703733L` — the reverse, `disk table threw but memory table did not`. The direction is
incidental: whichever path evaluated the filter over a non-empty chunk was the one that threw, and
which that was depended on how much the other path had already pruned.

## Reduction

```java
TableTools.newTable(TableTools.stringCol("S", "Aa", "aA", "bB", null, "cC", "dD", "eE"))
        .where("S icase in null, `aa`");   // NPE
```

Note what is *not* needed: the column need not contain a null. The receiver of `equalsIgnoreCase` is
the filter's own value, so a null there throws on the first row evaluated whatever the data holds.

## Root cause

`StringChunkMatchFilterFactory.makeCaseInsensitiveFilter` dispatches on the number of match values.
Both branches were unsound for a null value, in different ways:

- **Arity 1, 2, 3** — dedicated chunk filters compare with `storedValue.equalsIgnoreCase(columnValue)`.
  The receiver is the *filter's* value, so a null one throws for every row.

- **Arity 4+** — a `KeyedObjectHashSet` over `CIStringKey`. That key's `equalKey` and `hashKey` are
  already null-safe, which is why this path did not throw — but the set itself cannot hold a null, and
  `add(null)` is **dropped rather than refused**, so the value silently left the list. This is the worse
  of the two: a wrong answer with nothing to notice.

Case-sensitive `in` has always matched null against null (`S == null` is a match filter on null), so
this also made `icase` disagree with `in` on the one input where case folding cannot distinguish them.

## Fix

`engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfilter/StringChunkMatchFilterFactory.java`:

- A null-safe `equalsIgnoreCaseNullSafe(filterValue, columnValue)` — `filterValue == null ? columnValue
  == null : filterValue.equalsIgnoreCase(columnValue)` — used by all six specialized filters. This is
  deliberately the same rule as `CIStringKey.equalKey` and as case-sensitive `in`: null matches only null.
- A small `CaseInsensitiveValueSet` for the 4+ path that keeps null membership in a `matchesNull` field
  instead of handing it to the set that cannot store it.

The choice of semantics is not a judgment call: `icase` differs from `in` only by case folding, and
folding is not defined on null, so the two must agree there.

## Verification

- `CaseInsensitiveMatchFilterNullTest`: 14 tests, covering each arity on both sides of the inversion,
  null in leading and trailing positions, repeated nulls (the shape the fuzzer generated), an all-null
  column, agreement with case-sensitive `in` on null-only lists, and a no-null control. **12 of the 13
  original tests failed before the fix**; all pass after.
- The four fuzzer seeds pass.
- `:engine-table:test` filtered to the match-filter, chunk-filter and where-filter suites: 98 tests, 0
  failures.

## Not related to DH-23488 / PR #8425

Checked, because the areas sound adjacent — that PR even deletes a
`CaseInsensitiveStringMatchPushdownHandlerTest`. It does not cover this:

- `013df0238` (#8425) is already an **ancestor of this branch**, three commits before the campaign
  started, so the sweep that found these four failures already included it.
- Its changes are entirely under `extensions/parquet/table/.../location/` — the parquet **statistics**
  pushdown handlers, which decide whether a row group *might* match. This defect is in
  `engine/table/.../chunkfilter/`, the row-by-row evaluation that runs after any pushdown, and it
  reproduces on an in-memory `TableTools.newTable` with no parquet involved.
