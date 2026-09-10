# Finding 17 — sorted-column match pushdown used the ordering's equality, so `!= NaN` dropped rows

**Jira:** [DH-23502](https://deephaven.atlassian.net/browse/DH-23502) — **supersedes this finding**; see
*Superseded by DH-23502* below.
**Severity:** high — silent wrong results from `sort` plus `where`, with no parquet and no pushdown
switch involved. Rows are both dropped and invented depending on direction.
**Status:** **FIXED here as a stopgap.** Retire this fix when DH-23502 merges.
**Repro test:** `extensions/parquet/table/src/test/java/io/deephaven/parquet/table/SortedFloatNanMatchTest.java`
**Fuzzer seed:** `-5472033891179623763L`

## Symptom

```
filtered results differ: Result table has size 21 vs. expected 25
filter=Col0_r != fuzzP2        (fuzzP2 = Float.NaN)
```

The column was a sorted `float` ending in four `NaN`s; the disk path dropped exactly those four rows.
And it is not parquet-specific — three ordinary operations reproduce it in memory:

```java
final Table t = TableTools.newTable(TableTools.floatCol("F",
        -0.0f, 0.0f, 1.0f, 1.0f, 3.14f, Float.POSITIVE_INFINITY,
        Float.NaN, Float.NaN, Float.NaN, Float.NaN));

t.where("F != NaN").size();            // 10  correct
t.sort("F").where("F != NaN").size();  //  6  WRONG
t.sort("F").where("F == NaN").size();  //  4  WRONG -- should be 0
```

## Root cause

Deephaven deliberately keeps two different notions of float equality:

| | implementation | `NaN` vs `NaN` | `-0.0` vs `0.0` |
| --- | --- | --- | --- |
| ordering | `compareTo(float, float)` → `Double.compare` | equal | `-0.0` sorts below |
| equality | `eq(float, float)` → `a == b` | not equal | equal |

The first is a total order, which sorting needs; the second is IEEE, which `==` and `!=` need. Both
are correct for their purpose.

A sorted binary search finds the run of values `v` where `compareTo(v, target) == 0`. That answers a
match filter only where the two notions agree — and at `NaN` and `±0.0` they do not. So:

- `col != NaN` should match **every** row, since `NaN != NaN` is true. The search finds the `NaN` run
  and the inverted filter subtracts it, dropping those rows.
- `col == NaN` should match **nothing**. The search returns the `NaN` run.
- `col == 0.0` should match `-0.0` as well. The search finds only the `0.0` run.

Two sites do this search, and both were wrong:

1. `SortedColumnPushdownManager` — the in-memory path, reached whenever the table carries
   `SortedColumnsAttribute`, which `sort()` publishes. This is the severe one: no parquet, and
   `DISABLE_WHERE_PUSHDOWN_SORTED_COLUMN_LOCATION` is the only thing that would have masked it.
2. `ParquetColumnRegionChar` and its replicated `Float`/`Double` variants — the per-region path, which
   is what the fuzzer hit.

## Fix

A single `SortedColumnPushdownManager.sortedSearchAgreesWithEquality(Object[] values)` returns false
when any match value is `NaN` or `±0.0`:

- The in-memory path folds it into the existing `supportedMatchFilter` gate in `wrap(...)`, so the
  manager is not installed at all and the filter is evaluated normally.
- The parquet region template calls the same method and declines the action, in both
  `estimatePushdownAction` and `performPushdownAction`. Written as one call in the char template, so
  every replicated variant picks it up unchanged.

The cost is losing the optimization for a rare filter — an exact `==`/`!=` against `NaN` or zero on a
sorted float column — in exchange for the right answer. Everything else keeps it.

**`in` and `not in` are deliberately untouched.** Their `MatchFilter` compares *boxed* values, and
`Float.equals` is bitwise, so `NaN` already equals itself there: `F in NaN` matches the four `NaN`
rows both sorted and unsorted. That is consistent with the ordering, so the search is the right
question for them. (`in` and `==` therefore disagree with each other about `NaN` — measured, not
inferred — but that is a pre-existing engine choice, not something this fix should quietly change.)

## A note on the oracle

The test's oracle is the same values held plainly in memory, **not** `disk.select()`. That matters
here: `select()` inherits the sortedness claim, and the in-memory sorted path had the very same
defect, so `disk.select()` would have cheerfully agreed with the wrong answer. The fuzzer escaped this
only because its case reached the region path while its oracle did not. It is a good reminder that an
oracle built by transforming the same table can share the table's bugs.

## Superseded by DH-23502

DH-23502 ("Pushdown - Core: Sorted-column pushdown errors; binary-search kernels (T-01)", *Ready to
review* as of 2026-09-10) covers this defect as **PD-026**, P0:

> Gating ignores `nanMatch`; kernel `eq` treats `NaN==NaN` while residual follows IEEE →
> `sort().where("X == NaN")`/`!= NaN` wrong vs unsorted. Reproduced (`TestPD026`). *Confirmed and
> fixed.*

Same defect, same two sites, independently reproduced. **Its fix is better than the one here**: it
consults `MatchFilter`'s existing `nanMatch` option, so the optimization survives where it is
correct, whereas this stopgap declines the sorted action outright for any `NaN` or `±0.0` match
value. Take DH-23502's version.

### What to retire, once DH-23502 is merged

1. `SortedColumnPushdownManager.sortedSearchAgreesWithEquality(Object[])` — delete the method and the
   `&& sortedSearchAgreesWithEquality(matchFilter.getValues())` clause added to `supportedMatchFilter`
   in `wrap(...)`.
2. `ParquetColumnRegionChar` — delete the two guard calls (in `estimatePushdownAction` and
   `performPushdownAction`) and its `SortedColumnPushdownManager` import, then re-run
   `./gradlew replicateRegionsAndRegionedSources` so the eight replicated variants follow.
3. Mark this file **CLOSED — superseded by DH-23502** and set the index row's status accordingly.

Expect a **merge conflict** in both of those files: DH-23502 edits the same gate and the same
template. Resolve in favour of DH-23502.

### Two things to re-verify at merge, not assume

- **`SortedFloatNanMatchTest` should be kept**, not deleted with the fix: it asserts the user-visible
  property end to end (`sort().where()` and the parquet path together), which DH-23502's own tests
  approach from the kernel and chunk-filter side. It should pass unchanged under either fix — but
  **`inAndNotInAreUnchanged` is the one to watch.** It pins today's behaviour that `F in NaN` matches
  the `NaN` rows while `F == NaN` matches none. If DH-23502's `nanMatch` handling unifies those, that
  assertion will fail and should be updated to the new intended semantics rather than "fixed" back.
- DH-23502's **PD-027** covers a case this finding's tests do not: an `upper == +Infinity && inclusive`
  range shortcut wrongly including `NaN` rows as an exact match. `rangeFiltersAgree` here checks
  `< NaN` and `>= NaN` but not `<= +Infinity`, and it passed — so that is a genuine gap here, covered
  there.

### Not superseded

DH-23502's **PD-028** (`Object*BinarySearchKernel` breaking on compare-equal/equals-unequal types,
e.g. `BigDecimal` `1.0` vs `1.00`) is *adjacent to* but **not the same as** finding 18, which is about
a location's index file being read back as `BigInteger` for a scale-0 `BigDecimal` column — a schema
inference mismatch, not a kernel navigation one. Do not assume finding 18 is closed by this ticket.
Its **PD-031** ("sorted pushdown results always exact — as designed") is useful context for findings
11 and 18: the empty `maybeMatch` is the declared contract, so the correct remedy for an unanswerable
input is the decline path, which is what both of those fixes use.

## Verification

- `SortedFloatNanMatchTest`, 10 tests: `!= NaN` and `== NaN`; `== 0.0` matching `-0.0` and its
  inverse; a multi-value match including `NaN`; ordinary float matches still working; range filters
  including at the `NaN` boundary; a `double` column, covering the replicated copy; the pure in-memory
  `sort().where()` case; and `in`/`not in` asserted unchanged. Four fail without the fix, including
  the in-memory one.
- Full `:engine-table:test`, `:extensions-parquet-table:test` and `:extensions-parquet-base:test`
  pass.
- Seed `-5472033891179623763L` passes and joins `INTERESTING_SEEDS`.
