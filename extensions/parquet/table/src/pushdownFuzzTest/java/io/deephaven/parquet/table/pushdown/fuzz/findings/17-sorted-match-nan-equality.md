# Finding 17 — sorted-column match pushdown used the ordering's equality, so `!= NaN` dropped rows

**Jira:** [DH-23502](https://deephaven.atlassian.net/browse/DH-23502) — **supersedes this finding**; see
*Superseded by DH-23502* below.
**Severity:** high — silent wrong results from `sort` plus `where`, with no parquet and no pushdown
switch involved. Rows are both dropped and invented depending on direction.
**Status:** **CLOSED — superseded by [DH-23502](https://deephaven.atlassian.net/browse/DH-23502)**, merged
upstream as [#8452](https://github.com/deephaven/deephaven-core/pull/8452) on 2026-09-11. The stopgap this
branch carried was retired when upstream was pulled in; see *Retirement* below.
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

### Retirement — done

DH-23502 merged upstream on 2026-09-11 (#8452) and this branch was rebased onto it. Its fix is at the
source rather than at the two consumers: `MatchFilter.init` now removes an inert `NaN` from its values,
so every consumer sees one reduced set and the binary search can no longer be asked a question it would
answer differently from the filter. The parquet sorted region action is corrected by that change with no
code of its own.

The stopgap's production changes were dropped in the rebase, exactly as planned above:

1. `SortedColumnPushdownManager.sortedSearchAgreesWithEquality(Object[])` and its clause in
   `supportedMatchFilter` — gone.
2. The seven `*ColumnBinarySearchKernel` variants and the seven `ParquetColumnRegion*` guards — gone;
   DH-23502 rewrites those files wholesale.

The merge conflict this section predicted did occur, in all seven `ParquetColumnRegion*` files, and was
resolved in favour of DH-23502 as directed. Kept from the stopgap commit: this write-up, the regression
test, and the fuzzer seed.

### The two things to re-verify — both now measured

- **`SortedFloatNanMatchTest` was kept, and all 10 tests pass unchanged** against DH-23502, with no
  edit to any assertion. In particular `inAndNotInAreUnchanged` still holds: DH-23502's `nanMatch`
  handling did *not* unify `in` with `==`, so `F in NaN` still matches the `NaN` rows while
  `F == NaN` matches none. The test also still covers `±0.0` — `equalsZeroMatchesNegativeZero` and
  `notEqualsZeroExcludesBothZeros` pass — so upstream's narrower fix reaches the zero case too, which
  was worth checking rather than assuming: the stopgap declined for `±0.0` explicitly and DH-23502
  does not mention it.
- **PD-027 (the inclusive `+Infinity` upper bound) is covered upstream.** DH-23502 replaces the
  `MAX_FLOAT`/`MAX_DOUBLE` test with `isNaN(upper) && upperInclusive`, since Deephaven ordering sorts
  `NaN` above positive infinity. The gap this finding's `rangeFiltersAgree` left open is closed there.

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
- **Re-verified against upstream after the stopgap was dropped (2026-09-11):** with production code at
  upstream/main verbatim and no local fix of any kind, seed `-5472033891179623763L` passes, and
  `SortedFloatNanMatchTest` passes 10/10. This is the only one of this campaign's 22 findings that
  upstream fixed independently.
