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
| 17 | [Sorted-column match pushdown used the ordering's equality, so `!= NaN` dropped rows](17-sorted-match-nan-equality.md) | [DH-23502](https://deephaven.atlassian.net/browse/DH-23502) | high | fixed (stopgap; retire on DH-23502 merge) |
| 18 | [A data index read back with a different column type, so matches silently found nothing](18-data-index-type-mismatch.md) | — | high | fixed |
| 19 | [A case-insensitive match filter mishandled a null match value at every arity](19-icase-match-filter-null-value.md) | — | high | fixed |
| 20 | [Aliasing an indexed column threw when a second column was also indexed](20-duplicate-remapped-data-index.md) | — | medium-high | fixed |
| 21 | [**Bench defect:** an unguarded String method receiver made the oracle comparison meaningless](21-unguarded-string-method-receiver.md) | — | medium (bench) | fixed |
| 22 | [With a `_metadata` file, every location pruned against the first file's statistics](22-metadata-file-row-group-statistics.md) | — | high | fixed |

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
    -DPushdownFuzzer.maxMinutes=1 -DPushdownFuzzer.maxTableSize=1000 \
    -DPushdownFuzzer.failFast=false -DPushdownFuzzer.maxFailures=100
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
