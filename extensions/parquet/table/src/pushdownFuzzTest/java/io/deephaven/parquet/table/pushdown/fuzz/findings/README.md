# Pushdown fuzzer findings (DH-23557)

One file per finding, `NN-slug.md`, numbered in the order the bench found them. Each write-up
records the symptom, the reducing seed, a standalone repro, the root cause, the fix, and how the fix
was verified.

The prior round of findings — recorded before this list was restarted — is kept verbatim in
[`../OLD_FINDINGS.md`](../OLD_FINDINGS.md). Nothing here is carried over from it; a defect that
appears in both was rediscovered independently.

## Index

| # | Finding | Jira | Severity | Status |
| --- | --- | --- | --- | --- |
| 1 | [An explicit `RowGroupInfo.maxRows` on a zero-row table fails the write](01-empty-table-row-group-split.md) | — | medium | fixed |
| 2 | [`epochNanos` decided representability from the seconds alone](02-epoch-nanos-boundary.md) | — | high | fixed |
| 3 | [A pre-epoch `LocalDateTime` with a sub-second part cannot be read back from parquet](03-pre-epoch-local-date-time-materializers.md) | [DH-23609](https://deephaven.atlassian.net/browse/DH-23609) | high | fixed |
| 4 | [A key-value partitioned write with no partitions crashed inside the writer](04-empty-partitioned-write-crash.md) | — | medium | fixed |
| 5 | [A partitioning column's type was inferred from its directory names, so it did not round trip](05-partitioning-column-type-not-recorded.md) | — | high | fixed |
| 6 | [A disjunction mixing a renamed and an un-renamed column tripped an internal assertion](06-match-filter-partial-rename-map.md) | — | medium | fixed |
| 7 | [A formula filter pushed through nested renaming views lost all but the last renaming](07-condition-filter-rename-not-composed.md) | — | medium-high | fixed |
| 8 | [A filter naming both a column and its column-array form crashed the deferred-view filter split](08-deferred-view-duplicate-rename-key.md) | — | medium | fixed |
| 9 | [Parquet-space names leaked into two table-space APIs, dropping rows](09-parquet-name-space-leak.md) | — | high | fixed |
| 10 | [A deferred `renameColumns` that swaps or rotates names produced the wrong data](10-deferred-rename-not-simultaneous.md) | [DH-23611](https://deephaven.atlassian.net/browse/DH-23611) | high | fixed |
| 11 | [Every column of a multi-column sort was claimed as independently sorted, over-returning rows](11-multi-column-sort-claimed-per-column.md) | — | high | fixed |
| 12 | [A partition value containing a colon could not be written at all](12-colon-in-partition-value.md) | — | medium | fixed |
| 13 | [A filter naming an indexed column and an alias of it threw on the disk table](13-duplicate-alias-data-index-rename.md) | — | medium | fixed |
| 14 | [Building an error message with `Strings.of` discarded the real exception](14-strings-of-masks-the-real-exception.md) | — | medium | fixed |
| 15 | [Ordering incomparable types failed with an opaque `ClassCastException`](15-chained-comparison-and-incomparable-ordering.md) | — | medium | fixed (diagnostic), plus a bench correction |
| 16 | [**Bench defect:** a table-wide sort order was claimed on a layout that partitioning had destroyed](16-bench-partitioned-sort-claim.md) | — | high (bench) | fixed |
| 17 | [Sorted-column match pushdown used the ordering's equality, so `!= NaN` dropped rows](17-sorted-match-nan-equality.md) | [DH-23502](https://deephaven.atlassian.net/browse/DH-23502) | high | **closed — fixed upstream**, stopgap retired |
| 18 | [A data index read back with a different column type, so matches silently found nothing](18-data-index-type-mismatch.md) | — | high | fixed |
| 19 | [A case-insensitive match filter mishandled a null match value at every arity](19-icase-match-filter-null-value.md) | — | high | fixed |
| 20 | [Aliasing an indexed column threw when a second column was also indexed](20-duplicate-remapped-data-index.md) | — | medium-high | fixed |
| 21 | [**Bench defect:** an unguarded String method receiver made the oracle comparison meaningless](21-unguarded-string-method-receiver.md) | — | medium (bench) | fixed |
| 22 | [With a `_metadata` file, every location pruned against the first file's statistics](22-metadata-file-row-group-statistics.md) | — | high | fixed |

## Suggested PR grouping

Grouped by the production files each fix actually touches, rather than by topic, so that findings which
edit one file travel together. No group exceeds 10 findings; the binding constraint here is file
overlap, not size.

| PR | Findings | Code touched | Why together |
| --- | --- | --- | --- |
| **A — parquet write path** | 1, 4, 5, 12 | `ParquetTools`, `ParquetTableWriter`, `ParquetKeyValuePartitionedLayout`, `PartitioningColumnInfo` (new), `TableInfo`, `ParquetUtils`, `RowGroupTableIteratorVisitor`, `URIStreamKeyValuePartitionLayout` | All are "a dataset that cannot be written, or cannot be read back as written". 4 and 5 both edit `ParquetTools`; 5 and 12 are both partition-value encoding |
| **B — temporal round trip** | 2, 3 | `DateTimeUtils`, `ZonedDateTimeCodec`, the three `LocalDateTime*Materializer`s | Both are epoch-offset arithmetic losing pre-epoch and boundary values. The smallest and most self-contained group |
| **C — filter pushdown through renaming views** | 6, 7, 8, 10 | `MatchFilter`, `ConditionFilter`, `AbstractConditionFilter`, `DeferredViewTable` | 6 and 7 both edit `AbstractConditionFilter`; 8 and 10 both edit `DeferredViewTable`. One story: pushing a filter below a view that renames columns |
| **D — what a location claims about its data** | 9, 11, 13, 18, 22 | `ParquetTableLocation` (9, 13, 18, 22), `TableLocation`, `SourceTable` | **Four of the five edit `ParquetTableLocation`**, so splitting them guarantees conflicts. 11 belongs with them as the same question one layer up: 9 documents `TableLocation.getSortedColumns`' name space, 11 fixes what `SourceTable` publishes from it |
| **E — engine data indexes** | 14, 20 | `DataIndexPushdownManager`, `QueryTable.propagateDataIndexes` | Both are data-index lifecycle. 14 is diagnostic-only and rides cheaply |
| **F — value comparison semantics** | 15, 19 | `QueryLanguageFunctionUtils` (via `GenerateQueryLanguageFunctions`), `StringChunkMatchFilterFactory` | Both are "how a filter compares a value". Split 15 out if the generated-file/replicator review is awkward to combine |

### Deliberately excluded

- **Finding 17** — **nothing left to upstream.** [DH-23502](https://deephaven.atlassian.net/browse/DH-23502)
  merged as [#8452](https://github.com/deephaven/deephaven-core/pull/8452) and fixes the defect at the
  source; this branch's stopgap was dropped when that was pulled in. What remains is the write-up, the
  regression test `SortedFloatNanMatchTest` (10/10 against upstream, unchanged), and the fuzzer seed —
  all of which ride along with the bench. See
  [17-sorted-match-nan-equality.md](17-sorted-match-nan-equality.md).
- **Findings 16 and 21** — bench defects, entirely inside this source set (`FuzzLayout`, `FuzzFilters`).
  Nothing to upstream separately.

### Split the seed list out first

Every finding commit also adds its regression seeds to `PushdownFuzzerTest.INTERESTING_SEEDS`, and this
source set is not upstream yet. Split as-is, all six PRs conflict on that one file. Either:

1. **Land the bench as PR 0** — the `pushdownFuzzTest` source set and `findings/`, carrying the complete
   seed list — so the six fix PRs contain no bench changes at all. This is the simpler option.
2. Strip the seed additions from each fix PR and add them together in a final bench PR.

## Sweep history

Each sweep is a fixed 10-minute wall-clock budget at `maxTableSize=1000`, with `failFast=false` so one
run reports every failure it finds. Findings from each sweep were fixed before the next began.

| Sweep | Cases | Failures | Rate | Findings produced |
| --- | --- | --- | --- | --- |
| A | 12,470 | 7 | 0.056% | 19, 20, 21 |
| B | 10,734 | 1 | 0.009% | 22 |
| C | 12,043 | **0** | 0 | — |

Sweep C is the campaign's exit criterion: ten minutes of generated cases with no failure.

## Revalidating against upstream

This branch is long-lived, so upstream `main` moves under it. Each time upstream is pulled in, every
finding is re-checked against the new base before the fixes are proposed for merge. A finding that
upstream has since fixed on its own must be **dropped from this branch**, not shipped: carrying it
means a conflicting second fix for a defect that no longer exists.

### The procedure

1. **Rebase onto upstream.** `git rebase --onto upstream/main <old-base>`. This branch has always been
   integrated by rebase, not merge; keep it that way so the DH-23557 commits stay a reviewable stack.
2. **Build a bench-on-upstream tree.** Branch from the rebased tip and restore every file under
   `src/main/` that this branch touches to its upstream version — reverting modifications and deleting
   files this branch added:

   ```bash
   git switch -c experiment/bench-on-upstream
   for f in $(git diff --name-only upstream/main..HEAD | grep '/src/main/'); do
       if git cat-file -e upstream/main:$f 2>/dev/null; then
           git checkout upstream/main -- "$f"
       else
           git rm -q "$f"
       fi
   done
   git diff --stat upstream/main -- '*/src/main/*'   # must be empty
   ```

   **Keep everything else** — in particular `extensions/parquet/table/build.gradle`, which is what
   wires the `pushdownFuzzTest` source set, and the whole source set itself. The bench compiles
   against public API only, so it builds against plain upstream unchanged.

3. **Replay every regression seed** on that tree:

   ```bash
   ./gradlew :extensions-parquet-table:pushdownFuzzTest -PforceTest=true \
       --tests '*PushdownFuzzerTest.testInterestingSeeds' \
       -DPushdownFuzzer.maxTableSize=1000 -DPushdownFuzzer.failFast=false \
       -DPushdownFuzzer.maxFailures=200
   ```

   `failFast=false` matters: one run then reports every seed that failed rather than stopping at the
   first.

4. **Cross-check structurally.** A seed result is evidence; the file list is proof:

   ```bash
   comm -12 <(git diff --name-only upstream/main..HEAD | grep '/src/main/' | sort) \
            <(git diff --name-only <old-base>..upstream/main | grep '/src/main/' | sort)
   ```

   Upstream cannot have fixed a finding without touching a file that finding's fix touches.

### The bench corrections are not part of "our fixes"

Findings 16 and 21 are **bench** defects — the fuzzer was asserting something false, or generating an
invalid case. Their corrections live in the `pushdownFuzzTest` source set and are **always kept**,
including in the bench-on-upstream tree. Reverting them would make the bench report failures against
upstream that are the bench's own fault, and would silently undo a correction that nothing else
re-derives. The same goes for the bench half of finding 15. Only `src/main/` is reverted, which is
exactly the line between "a fix we are proposing" and "a correction to the instrument".

### Reading the results

- **A seed that passes** on the bench-on-upstream tree is a real positive: with no local fix of any
  kind, upstream handles the case. Either the finding is fixed upstream, or the seed stopped
  reproducing.
- **A seed that fails proves nothing about its own finding.** With every fix reverted at once, an
  earlier defect can mask a later one. Measured on 2026-09-11: finding 19's seed
  `-8415955675519703733` failed on finding 2's `DateTimeOverflowException`, and finding 11's seed
  `1681357320861610709` failed on finding 3's `Invalid value for NanoOfSecond (... ): -1` while reading
  a pre-epoch `LocalDateTime` back — neither reached the defect it was recorded for. Resolve these with
  step 4, or by reverting that one finding's commit in isolation.

### 2026-09-11 — rebase onto upstream `3ed1d774b`

15 new upstream commits. All 28 branch commits replayed; one conflicted.

Of 36 regression seeds, **6 passed** on the bench-on-upstream tree:

| Seed(s) | Finding | Reading |
| --- | --- | --- |
| `-5472033891179623763` | 17 | **Genuinely fixed upstream** by DH-23502 (#8452) |
| `-1220343102263136052`, `-3258625118121555365` | 16 | Bench defect; passes because the bench correction is kept. Expected |
| `-9218664977068266450`, `5844000365408086213` | 21 | Bench defect; same. Expected |
| `-7423979211207825555` | 9 | Stopped reproducing, but finding 9 is **not** fixed — its sibling seed `-6688467811848818630` still drops rows on upstream |

The structural check agrees: of the 25 production files this branch touches, the only one any of the
15 new upstream commits also touches is `MatchFilter.java`, and that is DH-23502's change.

**Conclusion: exactly one finding — 17 — can be marked fixed upstream.** Its stopgap was dropped;
findings 1–16 and 18–22 remain this branch's to land. The PR grouping above is unchanged except that
group C's `MatchFilter` edit now sits on top of DH-23502's.

## Carry-over from the previous round

All 19 case seeds recorded in [`../OLD_FINDINGS.md`](../OLD_FINDINGS.md) were replayed against this
branch. **14 now pass**; `1681357320861610709` (7a) was then fixed as finding 11 and `-2281078010550439077`
(7d) as finding 12, `428667830982598836` (part of 3) as finding 13, and the two cross-type seeds
(`5492728113478971232`, `6249197149364475148`, its findings 5 and 7b) as finding 15. **All 19 now
pass.**

Everything in that file is accounted for: its finding 1 was DH-23602, fixed before this campaign
began; findings 2, 4 and the write-path items became findings 3, 9, 1, 4 and 2 here; and its finding 3
and 7c clusters were resolved by findings 6, 7 and 10. Its two remaining classes — cross-type comparison
divergence and partition values containing colons — became findings 15 and 12.

## How the bench is run for this campaign

```bash
./gradlew :extensions-parquet-table:pushdownFuzzTest -PforceTest=true \
    --tests '*PushdownFuzzerTest.testFuzzer' \
    -DPushdownFuzzer.baseSeed=<n> -DPushdownFuzzer.cases=-1 \
    -DPushdownFuzzer.maxMinutes=10 -DPushdownFuzzer.maxTableSize=1000 \
    -DPushdownFuzzer.failFast=false -DPushdownFuzzer.maxFailures=200
```

Replay a single case seed (never as a `baseSeed` — case seeds come from
`new Random(baseSeed).nextLong()`):

```bash
./gradlew :extensions-parquet-table:pushdownFuzzTest -PforceTest=true \
    --tests '*PushdownFuzzerTest.testFuzzer' \
    -DPushdownFuzzer.maxTableSize=1000 -DPushdownFuzzer.dumpOnMismatch=true \
    "-DPushdownFuzzer.seeds=<seed>"
```

A seed only reproduces at the `maxTableSize` it was found at, because that bound is consumed while
generating the case.

## Suppressions

This campaign runs with **no failure-mode suppressions**: the bench generates every shape it can,
and every resulting failure is triaged and fixed rather than worked around. Two generator
constraints remain, and neither hides a defect:

- **A partitioning column is not also given an explicit data index.** `addIndexColumns` on a
  partitioning column is rejected by design (`Cannot add index on partitioning column`) — those
  columns get an automatic `PartitioningColumnDataIndex`.
- **A partitioning column is not renamed by `addColumnNameMapping`.** Its name comes from the
  directory key rather than from the parquet schema, so a read-time mapping cannot apply to it.

`LOCAL_DATE_TIME.partitionable()` is `false` because `PartitionFormatter`/`PartitionParser` have no
`LocalDateTime` entry at all — an unimplemented type rather than a broken one.

Merged tables are never given a `SortedColumnsAttribute` by the bench. The attribute is an
unvalidated caller assertion, and the concatenation of two sorted tables is not sorted, so declaring
one there would be the bench asserting something false. (The engine reaching that same false claim
on its own was DH-23602, now fixed.)
