# Pushdown fuzzer findings (DH-23557)

One file per finding, `NN-slug.md`, numbered in the order the bench found them. Each write-up
records the symptom, the reducing seed, a standalone repro, the root cause, the fix, and how the fix
was verified.

The prior round of findings — recorded before this list was restarted — is kept verbatim in
[`../OLD_FINDINGS.md`](../OLD_FINDINGS.md). Nothing here is carried over from it; a defect that
appears in both was rediscovered independently.

## Index

| # | Finding | Severity | Status |
| --- | --- | --- | --- |
| 1 | [An explicit `RowGroupInfo.maxRows` on a zero-row table fails the write](01-empty-table-row-group-split.md) | medium | fixed |
| 2 | [`epochNanos` decided representability from the seconds alone](02-epoch-nanos-boundary.md) | high | fixed |

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
