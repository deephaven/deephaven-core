# Reclaiming aggregation states

An incremental aggregation keeps one state per group. This document describes how a refreshing keyed aggregation
reclaims the states of groups whose rows have all been removed, the modes that control it, and their parameters. It is
written for contributors to the engine.

## Terms

- **State:** the per-group storage of an aggregation — its entry in the hash table plus one cell in each result column.
- **Output position:** the index of a state's cells in the result columns. It is also the state's row key in the result
  table.
- **Block:** a run of `ArrayBackedColumnSource.BLOCK_SIZE` (2048) output positions. The array-backed column sources
  allocate storage one block at a time.
- **Closed block:** a block whose positions have all been assigned to states. New states are only ever assigned after
  every existing state, so no state is ever assigned to a closed block again.
- **Released block:** a closed block whose states have all been removed, and whose storage has been freed.
- **Tombstone:** a hash table slot whose state has been removed. It keeps its key so that probes stay correct, and a
  rehash drops it.

## What reclaiming changes

Without reclaiming, a group that empties keeps its state and its output position. A group that returns reuses that
state. Memory therefore grows with every group ever seen, not with the groups that currently have rows.

With reclaiming, a state that is empty at the end of a cycle leaves the result and the hash table. Every reclaiming
mode keeps these rules:

- **New states go after every existing state.** A group that returns on a later cycle is a new state at a new output
  position, after every group that has rows. States are never placed into free positions in the middle of the result.
- **Moves keep the order of the states.** When the collapse moves states to free blocks, the states keep their
  relative order. Downstream listeners see the moves as the shifts of a `TableUpdate`.
- **Output positions are never given back.** Blocks are released in place, so the positions assigned grow with every
  group ever created. See [Costs and limits](#costs-and-limits) for what bounds them.
- **A group keeps its row key while it has rows,** unless the mode collapses blocks. A group that empties and returns
  is a new row, at the end of the result. This differs from the encounter-order promise of an aggregation that does not
  reclaim.
- **Storage is released after the cycle.** A block's storage is freed by a terminal notification, once every listener
  in the cycle has run, so previous values stay readable for the whole cycle that removed the states. The array sources
  return a released block's storage to their element type's recycler, where a later block borrows it rather than
  allocating one.

## Modes

`StateReclaimMode` selects the mode. The public `aggBy` family uses `StateReclaimMode.configured()`, which reads the
[configuration properties](#parameters) when the aggregation is created. Engine code can choose a mode for a single
aggregation — see [Choosing a mode per call](#choosing-a-mode-per-call).

### None

`StateReclaimMode.none()` never removes a state. An empty state keeps its output position, and a returning group
reuses it. This is how aggregations behaved before reclaiming existed. It uses the hash table without tombstones.

### Release blocks

`StateReclaimMode.releaseBlocks(collapseFreeFraction)` frees the storage of each closed block once every state in it
has been removed. With the default parameter, no state ever moves:

- The result columns' storage tracks the groups that have rows, but only at block granularity. A block that holds even
  one long-lived state is never released, so random churn with some long-lived groups releases few blocks. The hash
  table does not shrink (see [Costs and limits](#costs-and-limits)), so it stays sized for the most groups the
  aggregation has had at once.
- Output positions are never given back, so the positions assigned grow with every group ever created.

The **collapse** (`collapseFreeFraction` below 1) releases blocks that random churn would otherwise leave nearly empty.
A closed block at least this fraction free, and with at least one position free, is sparse; a full block never is. A run
is two or more sparse blocks with nothing but released blocks between them. A run is collapsed by packing its live
states into its sparse blocks, first to last, keeping their order, and releasing the sparse blocks this empties. Nothing
moves onto a released block. A run collapses only if that frees at least one block. Each cycle moves no more live states
than its input rows added, modified, and removed; a run that does not fit waits for a later cycle. The collapse frees
memory but does not give output positions back.

## Parameters

The configuration properties set the mode that `StateReclaimMode.configured()` returns. It reads them as each
aggregation is created, so changing them affects only aggregations created afterward. Tests change them through the
matching public static fields of `ChunkedOperatorAggregationHelper`.

| Property | Field | Default | Meaning |
| --- | --- | --- | --- |
| `ChunkedOperatorAggregationHelper.reclaimStates` | `RECLAIM_STATES` | `true` | Whether states are reclaimed at all. `false` selects `none`. |
| `ChunkedOperatorAggregationHelper.collapseFreeFraction` | `COLLAPSE_FREE_FRACTION` | `1.0` | The fraction free at which a closed block is sparse and may be collapsed. 1 or more never collapses. |

The defaults select `releaseBlocks(1)`: blocks are released as they empty and no state moves. The fraction must not be
`NaN`; `releaseBlocks` rejects one.

## Choosing a mode per call

Engine code that builds an aggregation can choose a mode for that aggregation alone:

- `ChunkedOperatorAggregationHelper.aggregation(control, factory, input, preserveEmpty, initialKeys, reclaimMode,
  groupByColumns)`
- `QueryTable.aggNoMemo(factory, preserveEmpty, initialGroups, groupByColumns, reclaimMode)`

A consumer that looks up a group's current row key and then reads previous values at that row needs a mode that never
moves states. The tree table's source row lookup uses `none` for this reason, and the table-backed data index uses
`none` as well.

## When a mode applies

A mode other than `none` can apply only when all of the following hold:

- Every operator of the aggregation can reclaim states (`IterativeChunkedAggregationOperator.canReclaimStates()`, false
  by default). Group-by, partition-by, formula, and rollup operators cannot. Neither can the operators whose states
  never become empty: approximate percentiles, whose t-digests cannot remove values, and the min, max, first, and last
  operators used only for add-only and blink input, which are accepted with any mode as described below.
- The aggregation does not preserve empty groups.
- The aggregation has no initial groups.

What happens when they do not hold depends on where the mode came from:

- **The configured mode** (`StateReclaimMode.configured()`, which the public `aggBy` family uses) degrades silently: the
  aggregation behaves as with `none`.
- **A mode chosen explicitly** (`releaseBlocks(...)` rather than `configured()`) fails the
  aggregation with an `IllegalArgumentException` naming the reason — for example, the result columns whose operators
  cannot reclaim states. `StateReclaimMode.isConfigured()` tells the two apart.

Static, add-only, append-only, and blink input never reclaims states, since no state is ever removed (a blink
aggregation ignores its input's removals), so any mode is accepted for it and none of these conditions is checked.

## Costs and limits

- **Returning groups cost more.** A group that empties and returns is a new state, and a new row at the end of the
  result, where `none` would reuse its state. Workloads whose groups keep returning run slower with reclaiming.
- **Output positions are ints.** No mode gives positions back, so every reclaiming mode fails with `Aggregation output
  positions exhausted` once 2^31 - 1 states have been created over the life of the aggregation. At 10,000 new groups
  per second that is about 2.5 days. Rather than moving states to keep positions low, which was tried and rejected (see
  [Block shifting](#block-shifting)), an aggregation that needs more can migrate to `long` output positions (see
  [Long output positions](#long-output-positions)).
- **The hash table does not shrink.** Tombstones keep their keys until a rehash drops them. Growth is driven by the live
  states: when tombstones alone cross the load factor, the table rehashes at the same size.
- **Key columns track previous values only when states can move.** The copied key columns start tracking previous
  values when the mode collapses, but not for release blocks without moves.

## Benchmark results

`AggregationIncrementalBenchmark` in `engine/benchmark` measures batches of 900 update cycles of a keyed `sumBy` over a
table of 1,000,000 rows, with 10,000 rows added and removed each cycle:

- **Add only:** rows are only added, each with a new group.
- **Sliding window:** each cycle removes the oldest rows, so groups empty in the order they were created.
- **Random churn:** each cycle removes rows chosen at random, so blocks thin out unevenly.

Each group owns one row or 100 consecutive rows. With one row per group every removal empties a group and every
addition creates one; with 100, most changes add rows to or remove rows from groups that already exist. A group
"returns" when its key comes back after its state has been removed; with returning groups, the keys cycle through a
space twice the number of groups in the table.

The same results are charted in
[aggregation-state-reclaiming-benchmarks.html](aggregation-state-reclaiming-benchmarks.html); open that file in a
browser, since GitHub shows only its source.

The columns:

- **Main:** main at 691450c217, which predates reclaiming. Every percentage is the change against this column.
- **`none`:** this branch without reclaiming, which isolates its other improvements to the update cycle.
- **Blocks:** `releaseBlocks(1)`, the default, which releases blocks and never moves a state.
- **Collapse 0.75** and **Collapse 0.5:** `releaseBlocks(0.75)` and `releaseBlocks(0.5)`, which also collapse runs of
  sparse blocks.
- **Main again:** main once more, after every other configuration, to show any drift over the run.

The branch was measured at f67af3c039, with the adaptive `SoftRecycler` of
[#8698](https://github.com/deephaven/deephaven-core/pull/8698) cherry-picked onto it. Main does not include #8698, so
the branch's columns combine both changes; its `none` column shows what #8698 alone does for random churn.

The numbers come from an Intel i9-14900KS (hybrid performance and efficiency cores) in an LXC container with 24 logical
CPUs and 128 GB, with the performance governor and turbo boost enabled, and otherwise idle. Each configuration ran two
JVM forks of three warmup and five measured batches; the error bounds are JMH's 99.9% intervals over the ten measured
batches. Main and Main again are within the error of each other in every workload, so the run did not drift.

### Time

Milliseconds per batch of 900 cycles; lower is better.

| Workload | Rows per group | Groups return | Main | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Main again |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 1379 ± 123 | 1356 ± 121 (−2%) | 1279 ± 84 (−7%) | 1307 ± 114 (−5%) | 1273 ± 129 (−8%) | 1395 ± 90 (+1%) |
| Sliding window | 1 | No | 1992 ± 62 | 2019 ± 138 (+1%) | 1318 ± 94 (−34%) | 1347 ± 80 (−32%) | 1319 ± 75 (−34%) | 2026 ± 129 (+2%) |
| Sliding window | 1 | Yes | 1056 ± 72 | 1073 ± 39 (+2%) | 1184 ± 58 (+12%) | 1241 ± 45 (+18%) | 1270 ± 44 (+20%) | 1055 ± 31 (−0%) |
| Random churn | 1 | No | 4472 ± 115 | 3560 ± 77 (−20%) | 3313 ± 66 (−26%) | 3429 ± 78 (−23%) | 3549 ± 138 (−21%) | 4515 ± 90 (+1%) |
| Random churn | 1 | Yes | 2927 ± 87 | 2616 ± 89 (−11%) | 3322 ± 69 (+13%) | 3438 ± 93 (+17%) | 3527 ± 77 (+20%) | 2837 ± 57 (−3%) |
| Add only | 100 | — | 199 ± 26 | 172 ± 11 (−13%) | 183 ± 23 (−8%) | 171 ± 12 (−14%) | 182 ± 26 (−9%) | 193 ± 38 (−3%) |
| Sliding window | 100 | No | 300 ± 37 | 316 ± 58 (+5%) | 295 ± 28 (−2%) | 314 ± 42 (+5%) | 307 ± 31 (+2%) | 318 ± 63 (+6%) |
| Sliding window | 100 | Yes | 317 ± 30 | 315 ± 29 (−1%) | 319 ± 31 (+1%) | 352 ± 27 (+11%) | 327 ± 27 (+3%) | 308 ± 33 (−3%) |
| Random churn | 100 | No | 847 ± 93 | 860 ± 117 (+2%) | 808 ± 84 (−5%) | 834 ± 87 (−2%) | 837 ± 81 (−1%) | 835 ± 74 (−1%) |
| Random churn | 100 | Yes | 779 ± 51 | 800 ± 66 (+3%) | 789 ± 74 (+1%) | 759 ± 40 (−3%) | 764 ± 54 (−2%) | 798 ± 45 (+2%) |

### Memory

The heap retained at the end of a batch, in megabytes, after a garbage collection, averaged over the measured
batches. It includes the source table and the benchmark's own data, and the blocks the recyclers hold, so compare the
modes with each other rather than reading the values as the aggregation's size.

| Workload | Rows per group | Groups return | Main | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Main again |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 676 | 675 (−0%) | 572 (−15%) | 578 (−14%) | 578 (−14%) | 675 (−0%) |
| Sliding window | 1 | No | 679 | 682 (+0%) | 186 (−73%) | 186 (−73%) | 186 (−73%) | 679 (+0%) |
| Sliding window | 1 | Yes | 182 | 182 (−0%) | 130 (−29%) | 130 (−28%) | 131 (−28%) | 181 (−0%) |
| Random churn | 1 | No | 716 | 731 (+2%) | 484 (−32%) | 267 (−63%) | 240 (−66%) | 716 (+0%) |
| Random churn | 1 | Yes | 219 | 221 (+1%) | 452 (+106%) | 212 (−3%) | 186 (−15%) | 219 (+0%) |
| Add only | 100 | — | 30 | 29 (−3%) | 28 (−6%) | 32 (+7%) | 32 (+6%) | 29 (−3%) |
| Sliding window | 100 | No | 30 | 30 (+2%) | 26 (−12%) | 26 (−12%) | 26 (−12%) | 30 (+1%) |
| Sliding window | 100 | Yes | 21 | 21 (+2%) | 25 (+19%) | 25 (+19%) | 25 (+19%) | 21 (+0%) |
| Random churn | 100 | No | 52 | 53 (+1%) | 48 (−8%) | 54 (+2%) | 54 (+2%) | 52 (−1%) |
| Random churn | 100 | Yes | 43 | 42 (−2%) | 42 (−2%) | 47 (+9%) | 47 (+9%) | 42 (−2%) |

### Longest cycle

The longest single cycle of a batch, in milliseconds, the worst over the ten measured batches. A single worst sample is
noisy, as the difference between Main and Main again shows, so read the percentages for the modes whose longest cycles
are consistently high rather than for small differences. With 100 rows per group every longest cycle is a few
milliseconds, where one slow sample swings the percentage widely.

| Workload | Rows per group | Groups return | Main | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Main again |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 62.7 | 60.9 (−3%) | 62.1 (−1%) | 69.2 (+10%) | 65.3 (+4%) | 64.5 (+3%) |
| Sliding window | 1 | No | 63.8 | 83.0 (+30%) | 21.2 (−67%) | 26.0 (−59%) | 18.2 (−71%) | 64.2 (+1%) |
| Sliding window | 1 | Yes | 12.5 | 17.9 (+43%) | 14.7 (+17%) | 15.6 (+25%) | 14.9 (+19%) | 14.7 (+18%) |
| Random churn | 1 | No | 65.5 | 65.5 (+0%) | 22.5 (−66%) | 16.7 (−75%) | 11.4 (−83%) | 52.4 (−20%) |
| Random churn | 1 | Yes | 11.0 | 11.4 (+3%) | 13.0 (+18%) | 14.7 (+33%) | 13.6 (+23%) | 12.5 (+13%) |
| Add only | 100 | — | 2.2 | 1.3 (−43%) | 1.0 (−57%) | 1.0 (−54%) | 1.7 (−26%) | 1.3 (−42%) |
| Sliding window | 100 | No | 0.9 | 1.1 (+32%) | 0.9 (+11%) | 1.6 (+82%) | 2.9 (+238%) | 2.4 (+178%) |
| Sliding window | 100 | Yes | 1.0 | 0.9 (−10%) | 1.0 (−3%) | 1.2 (+15%) | 1.1 (+5%) | 1.4 (+31%) |
| Random churn | 100 | No | 3.5 | 3.0 (−14%) | 3.1 (−9%) | 2.6 (−24%) | 3.0 (−12%) | 3.2 (−7%) |
| Random churn | 100 | Yes | 2.5 | 2.7 (+8%) | 2.9 (+16%) | 3.9 (+56%) | 3.5 (+39%) | 3.9 (+54%) |

### Ten million rows

The same workloads with one row per group over a table of 10,000,000 rows, with 100,000 rows added and removed each
cycle, run with a 48 GB heap.

Milliseconds per batch of 900 cycles:

| Workload | Groups return | Main | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Main again |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 15688 ± 404 | 15596 ± 366 (−1%) | 14048 ± 270 (−10%) | 14323 ± 228 (−9%) | 14316 ± 179 (−9%) | 15437 ± 649 (−2%) |
| Sliding window | No | 21972 ± 551 | 22137 ± 302 (+1%) | 15944 ± 72 (−27%) | 16197 ± 183 (−26%) | 16109 ± 245 (−27%) | 22143 ± 650 (+1%) |
| Sliding window | Yes | 12113 ± 100 | 12308 ± 114 (+2%) | 13283 ± 94 (+10%) | 13596 ± 86 (+12%) | 13592 ± 189 (+12%) | 12214 ± 98 (+1%) |
| Random churn | No | 56762 ± 499 | 38705 ± 288 (−32%) | 35124 ± 369 (−38%) | 36495 ± 181 (−36%) | 37247 ± 427 (−34%) | 56908 ± 375 (+0%) |
| Random churn | Yes | 38664 ± 300 | 26726 ± 194 (−31%) | 34923 ± 97 (−10%) | 34973 ± 172 (−10%) | 35415 ± 527 (−8%) | 38807 ± 283 (+0%) |

Retained heap, in megabytes:

| Workload | Groups return | Main | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Main again |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 5623 | 5624 (+0%) | 5082 (−10%) | 5093 (−9%) | 5091 (−9%) | 5622 (−0%) |
| Sliding window | No | 5659 | 5692 (+1%) | 898 (−84%) | 900 (−84%) | 900 (−84%) | 5656 (−0%) |
| Sliding window | Yes | 1427 | 1434 (+1%) | 898 (−37%) | 900 (−37%) | 900 (−37%) | 1426 (−0%) |
| Random churn | No | 5894 | 6194 (+5%) | 4327 (−27%) | 2138 (−64%) | 1863 (−68%) | 5894 (−0%) |
| Random churn | Yes | 1662 | 1810 (+9%) | 4119 (+148%) | 1693 (+2%) | 1407 (−15%) | 1663 (+0%) |

Longest cycle, in milliseconds:

| Workload | Groups return | Main | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Main again |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 492.8 | 494.3 (+0%) | 516.9 (+5%) | 506.4 (+3%) | 511.5 (+4%) | 495.5 (+1%) |
| Sliding window | No | 507.7 | 503.3 (−1%) | 101.6 (−80%) | 92.8 (−82%) | 100.1 (−80%) | 493.4 (−3%) |
| Sliding window | Yes | 124.7 | 124.7 (+0%) | 123.8 (−1%) | 105.7 (−15%) | 128.7 (+3%) | 110.8 (−11%) |
| Random churn | No | 422.4 | 289.8 (−31%) | 133.5 (−68%) | 86.4 (−80%) | 87.8 (−79%) | 548.1 (+30%) |
| Random churn | Yes | 138.2 | 80.6 (−42%) | 80.2 (−42%) | 77.5 (−44%) | 72.9 (−47%) | 95.6 (−31%) |

### Findings

- This branch without reclaiming (`none`) is 20% faster than main for random churn with one row per group when groups
  do not return, and 11% faster when they do; at ten million rows, 32% and 31%. Most of that comes from the adaptive
  recyclers of #8698, which main lacks: main's fixed recycler capacity turns away previous-value blocks that random
  churn needs again every cycle. Elsewhere it is within the error.
- When groups do not return, every reclaiming mode is faster than main for the sliding window, by 32% to 34%, and
  retains about a quarter of its heap; at ten million rows, 26% to 27% faster with a sixth of the heap. For random
  churn, releasing blocks alone is 26% faster and retains a third less heap; collapsing is 21% to 23% faster and retains
  a third of the heap. At ten million rows, releasing blocks alone is 38% faster and collapsing 34% to 36%, and add only
  is 9% to 10% faster with reclaiming.
- When groups return, the sliding window is slower with reclaiming, since returning groups are new states rather than
  reused ones: 12% releasing blocks alone, just inside the error, and 18% to 20% collapsing, and at ten million rows 10%
  to 12%. Random churn
  with returning groups is 13% slower releasing blocks alone and 17% to 20% slower collapsing at one million rows, but
  at ten million rows every reclaiming mode is 8% to 10% faster than main. Releasing blocks alone retains twice the heap
  of main there, and collapsing 3% to 15% less (at ten million rows, 2% more to 15% less).
- For random churn at ten million rows, releasing blocks alone is 4% to 6% faster than collapsing when groups do not
  return, and within 2% of it when they do; at one million rows the modes are within 7% of each other. Collapsing
  retains far less heap.
- Without the recycler changes (the branch at fb15638b78, with main's fixed recycler capacity), random churn with
  returning groups was 42% slower than main releasing blocks alone at one million rows and 23% slower at ten million.
- No mode gives output positions back, so every reclaiming mode assigns the same positions: with one row per group,
  10 million when groups do not return, and 8.71 to 10 million when they do, where main reuses 2 million; with 100 rows
  per group, 100,000, or 20,000 for random churn with returning groups.
- With 100 rows per group, the modes are within the error of each other for time.
- Reclaiming shortens the longest cycles when groups do not return: 18 to 26 ms for the sliding window and 11 to 23 ms
  for random churn, against main's 64 to 66 ms; at ten million rows, 93 to 102 ms and 86 to 134 ms, against 422 to 548
  ms for main and main again. When groups return, the longest cycles are 11 to 18 ms for main and every mode, and 73 to
  138 ms at ten million rows.

## Long output positions

Output positions are `int`s, and no mode gives them back, so an aggregation runs out after 2^31 - 1 states (see
[Costs and limits](#costs-and-limits)). Storing them as `long`s would remove the limit without moving states to give
positions back, which was tried and rejected (see [Block shifting](#block-shifting)): releasing blocks as they empty
bounds memory, and positions can keep increasing. Three experiments, not part of this change, measured what `long`
positions cost. They were run while block shifting was still a parameter of release blocks, so the modes below are
written with its parameters.

### What changed in the experiments

The two places that hold positions were widened separately:

- **The hash table's positions:** the tombstone state manager stores each slot's output position as a `long`, in both
  the main and the alternate table, with `long` sentinels. Its generated hashers use a `long` state type.
- **Everything downstream of the hash table:**
  - the destination chunk every aggregation operator receives, `LongChunk<RowKeys>` instead of `IntChunk<RowKeys>`,
    in 149 operator files;
  - the state managers' output position chunks;
  - the helper's shared position counter, its sort and run finding over destinations, and the initial state and
    capacity counts;
  - the block tracker's position arguments.

The experiments cover three of the four combinations:

| | `int` hash table | `long` hash table |
| --- | --- | --- |
| **`int` downstream** | this change | the first experiment |
| **`long` downstream** | the third experiment | the second experiment, a complete conversion |

Even the complete conversion keeps some `int`s:

- The block tracker's block index and the modified-states bitmap's word index, which limit positions to 2^42 and 2^37.
- `findPositionForKey` and `AggregationRowLookup`, which return `int` positions.
- The hash tables that never reclaim store `int` positions, since they never exceed them.

Releasing blocks also leaves each array-backed source's block array as long as the highest position ever assigned:
one reference per 2048 positions, most of them empty. Positions that grow without limit would eventually need a sparse
structure for the blocks.

### Results

The same benchmarks as [Benchmark results](#benchmark-results), one row per group, comparing this change's `int`
positions with each experiment; milliseconds per batch of 900 cycles. An asterisk marks a difference outside the error
bounds. Unless a table says otherwise, the numbers come from an Apple silicon Mac.

**The complete conversion, without reclaiming.** Both position domains are widened, but without reclaiming the
aggregation uses a hash table that never reclaims, which keeps `int` positions. So only the downstream positions are
wider in this benchmark, and nothing measurable changes:

| Size | Workload | Groups return | `int` | `long` | Change | Heap, MB |
| --- | --- | --- | --- | --- | --- | --- |
| 1M | Add only | — | 90 ± 18 | 94 ± 18 | +4% | 103 → 103 |
| 1M | Random churn | No | 217 ± 40 | 214 ± 27 | −1% | 110 → 110 |
| 10M | Add only | — | 948 ± 76 | 961 ± 102 | +1% | 673 → 674 |
| 10M | Sliding window | No | 1437 ± 78 | 1436 ± 43 | −0% | 680 → 682 |
| 10M | Random churn | No | 2582 ± 79 | 2622 ± 61 | +2% | 718 → 717 |
| 10M | Random churn | Yes | 1611 ± 65 | 1559 ± 104 | −3% | 217 → 218 |
| 100M | Add only | — | 12300 ± 755 | 12230 ± 1624 | −1% | 5617 → 5623 |
| 100M | Random churn | No | 30299 ± 12134 | 30175 ± 1882 | −0% | 5925 → 5932 |

**The complete conversion, releasing blocks with no moves** (`releaseBlocks(1, -1, false)`, now `releaseBlocks(1)`).
The hash table's positions are wider too:

| Size | Workload | Groups return | `int` | `long` | Change | Heap, MB |
| --- | --- | --- | --- | --- | --- | --- |
| 1M | Add only | — | 95 ± 21 | 104 ± 37 | +9% | 83 → 91 |
| 1M | Random churn | No | 213 ± 52 | 220 ± 20 | +4% | 58 → 62 |
| 10M | Add only | — | 849 ± 37 | 968 ± 30 | +14% * | 569 → 632 |
| 10M | Sliding window | No | 878 ± 49 | 939 ± 51 | +7% | 175 → 208 |
| 10M | Sliding window | Yes | 724 ± 46 | 759 ± 50 | +5% | 119 → 136 |
| 10M | Random churn | No | 2419 ± 103 | 2457 ± 161 | +2% | 467 → 500 |
| 10M | Random churn | Yes | 2441 ± 93 | 2485 ± 224 | +2% | 433 → 448 |
| 100M | Add only | — | 11340 ± 1511 | 11722 ± 1724 | +3% | 5072 → 5588 |
| 100M | Sliding window | No | 10815 ± 690 | 11336 ± 1357 | +5% | 887 → 1014 |
| 100M | Random churn | No | 29504 ± 1626 | 30508 ± 1765 | +3% | 4053 → 4312 |

**The third experiment, releasing blocks with no moves.** Downstream positions are `long` and the hash table's stay
`int`. Nothing measurable changes, including the heap:

| Size | Workload | Groups return | `int` | `long` downstream | Change | Heap, MB |
| --- | --- | --- | --- | --- | --- | --- |
| 1M | Add only | — | 91 ± 12 | 94 ± 7 | +3% | 83 → 83 |
| 1M | Random churn | No | 218 ± 56 | 220 ± 20 | +1% | 59 → 59 |
| 10M | Add only | — | 924 ± 50 | 859 ± 90 | −7% | 571 → 570 |
| 10M | Sliding window | No | 822 ± 27 | 872 ± 59 | +6% | 175 → 175 |
| 10M | Sliding window | Yes | 759 ± 182 | 724 ± 22 | −5% | 120 → 119 |
| 10M | Random churn | No | 2396 ± 101 | 2409 ± 177 | +1% | 468 → 469 |
| 10M | Random churn | Yes | 2448 ± 118 | 2409 ± 104 | −2% | 433 → 433 |
| 100M | Add only | — | 11465 ± 3351 | 11566 ± 1643 | +1% | 5076 → 5079 |
| 100M | Sliding window | No | 10909 ± 753 | 11082 ± 985 | +2% | 886 → 884 |
| 100M | Random churn | No | 29766 ± 1089 | 30058 ± 3970 | +1% | 4054 → 4059 |

**The first experiment, on a second machine.** Only the hash table's positions are wider. These numbers come from a
Linux container on an Intel Core i9-14900KS with 128 GB of RAM, which is slower than the Mac in absolute terms:

| Size | Mode | Workload | Groups return | `int` | `long` hash table | Change | Heap, MB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 10M | `releaseBlocks(1, -1, false)` | Add only | — | 1446 ± 212 | 1654 ± 278 | +14% | 570 → 634 |
| 10M | `releaseBlocks(1, -1, false)` | Sliding window | No | 1528 ± 289 | 1775 ± 254 | +16% | 186 → 218 |
| 10M | `releaseBlocks(1, -1, false)` | Random churn | No | 4242 ± 389 | 4458 ± 155 | +5% | 473 → 505 |
| 10M | `releaseBlocks(0.5, 0, true)` | Add only | — | 1482 ± 261 | 1639 ± 147 | +11% | 578 → 641 |
| 10M | `releaseBlocks(0.5, 0, true)` | Sliding window | No | 1683 ± 184 | 1826 ± 195 | +9% | 194 → 226 |
| 10M | `releaseBlocks(0.5, 0, true)` | Random churn | No | 4015 ± 54 | 4180 ± 377 | +4% | 238 → 270 |
| 100M | `releaseBlocks(1, -1, false)` | Add only | — | 13746 ± 6732 | 14964 ± 2324 | +9% | 5068 → 5596 |
| 100M | `releaseBlocks(1, -1, false)` | Sliding window | No | 16183 ± 1638 | 16967 ± 358 | +5% | 896 → 1025 |
| 100M | `releaseBlocks(1, -1, false)` | Random churn | No | 49006 ± 5216 | 48964 ± 3746 | −0% | 4059 → 4314 |
| 100M | `releaseBlocks(0.5, 0, true)` | Add only | — | 14118 ± 3163 | 15585 ± 846 | +10% | 5087 → 5607 |
| 100M | `releaseBlocks(0.5, 0, true)` | Random churn | No | 44588 ± 2668 | 44728 ± 1234 | +0% | 1701 → 1957 |

`releaseBlocks(0.5, 0, true)` collapsed at 0.5 and shifted blocks in bulk.

The initial build of 10M rows, with reclaiming:

- The complete conversion was within 5% either way without reclaiming, and 6% (`String` keys) to 10% (`long` keys)
  slower at 5 million keys with reclaiming.
- The third experiment was within 3% of `int` at 5 million keys.
- On the second machine, the first experiment was 5% slower at 5 million keys, and within 2% at 100,000.

### Findings

- Widening the positions downstream of the hash table costs nothing measurable, with or without reclaiming. The whole
  cost is the hash table's slots growing from 4 to 8 bytes: 2% to 16% more time, most where adding states dominates
  and least for random churn, and 5% to 15% more retained heap. Both machines agree.
- `long` positions everywhere would pay that cost in every aggregation that reclaims, to remove a limit that few
  reach.
- A hybrid would pay it only when needed: `long` positions everywhere except the hash table, which stores `int`
  positions until they reach a threshold such as 2^30 and is then converted once, as part of a rehash, to a hasher that
  stores `long`s. The third experiment shows that below the threshold this costs nothing, and with the downstream
  positions already `long`, the conversion is confined to the hash table.

The numbers come from one JVM fork per configuration, with five measured batches each, or three at 100M; treat
differences within the error bounds as noise.

## Rejected Alternatives

These designs were implemented or measured and then rejected.

### Block shifting

Block shifting moved whole blocks of states down over released blocks, keeping the states in order, so that output
positions were given back as well as memory. It was a pair of parameters of release blocks:

- `blockShiftFraction` started a shift once the released blocks were at least this fraction of the positions assigned,
  so that many blocks did not shift to fill very small holes.
- `bulkShift` chose how the shift proceeded. Without it, the shift swept toward the end over several cycles, each
  moving no more live states than its input rows, and gave the released blocks back when it reached the end. With it,
  the shift waited until it could move every block after the first released one in one cycle, paid for by a credit that
  each cycle's added and removed states earned and that carried across cycles.

A sweeping shift chases a tail that grows with each cycle's new groups, and it moves blocks near the front that empty
soon after, so for a sliding window with `L` live groups the positions assigned peaked near `3 L`. The bulk shift ran
whenever the credit reached `L`, so the positions assigned stayed near `1.5 L`, but it concentrated the work: every live
state after the first released block moved in one cycle.

Its benefit was lower output positions. Its costs:

- **Latency.** The bulk shift had the longest cycles for random churn, 19 to 42 ms against 8 to 15 ms for
  releasing blocks alone or the sweeping shift.
- **Memory.** Every shifting mode retained slightly more heap than releasing blocks alone for the sliding window,
  180 to 191 MB against 175 MB.
- **Code.** The array sources moved whole blocks by reference, completed a block's previous values before moving it,
  allocated destination blocks that an earlier move had left unallocated, and shrank their capacity when blocks were
  released through the end. The helper composed the collapse and the shift into one shift for downstream listeners,
  and the state manager updated the hash table for every live state in the moved blocks.

The positions it kept low matter only for the `int` limit of 2^31 - 1 states created over the life of an aggregation.
Rather than moving states to stay under it, an aggregation that needs more can migrate to `long` output positions: the
[Long output positions](#long-output-positions) experiments show that `long` positions downstream of the hash table
cost nothing, and a hash table that converts to `long` positions at a threshold pays their cost only once it is
needed.

Measured with the benchmark described in [Benchmark results](#benchmark-results), one row per group, on an Apple
silicon Mac, with percentages against main with #8676, which was not yet merged. Blocks releases blocks with no moves.
The shifting modes were `releaseBlocks(0.75, 0, false)`, which collapsed and swept (Collapse, sweep);
`releaseBlocks(1, 0, true)`, which shifted in bulk without collapsing (Bulk); and `releaseBlocks(0.75, 0, true)` and
`releaseBlocks(0.5, 0, true)`, which collapsed and shifted in bulk. The last two columns collapse without shifting, from
a later run on the same Mac.

Time, in milliseconds per batch of 900 cycles:

| Workload | Groups return | Main + #8676 | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk | Collapse 0.75 | Collapse 0.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 949 ± 116 | 873 ± 83 (−8%) | 922 ± 137 (−3%) | 950 ± 32 (+0%) | 917 ± 65 (−3%) | 922 ± 43 (−3%) | 891 ± 43 (−6%) | 986 ± 141 (+4%) |
| Sliding window | No | 1367 ± 87 | 822 ± 43 (−40%) | 907 ± 77 (−34%) | 968 ± 128 (−29%) | 906 ± 16 (−34%) | 926 ± 70 (−32%) | 867 ± 116 (−37%) | 860 ± 30 (−37%) |
| Sliding window | Yes | 595 ± 30 | 726 ± 90 (+22%) | 890 ± 51 (+50%) | 784 ± 56 (+32%) | 810 ± 47 (+36%) | 871 ± 22 (+46%) | 763 ± 59 (+28%) | 753 ± 24 (+27%) |
| Random churn | No | 2827 ± 127 | 2318 ± 70 (−18%) | 2537 ± 121 (−10%) | 2818 ± 89 (−0%) | 2374 ± 92 (−16%) | 2420 ± 79 (−14%) | 2302 ± 109 (−19%) | 2325 ± 70 (−18%) |
| Random churn | Yes | 1873 ± 49 | 2460 ± 211 (+31%) | 2470 ± 175 (+32%) | 2868 ± 392 (+53%) | 2393 ± 155 (+28%) | 2274 ± 210 (+21%) | 2242 ± 79 (+20%) | 2215 ± 23 (+18%) |

Retained heap, in megabytes:

| Workload | Groups return | Main + #8676 | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk | Collapse 0.75 | Collapse 0.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 675 | 568 (−16%) | 569 (−16%) | 568 (−16%) | 567 (−16%) | 568 (−16%) | 567 (−16%) | 570 (−16%) |
| Sliding window | No | 676 | 175 (−74%) | 180 (−73%) | 191 (−72%) | 190 (−72%) | 190 (−72%) | 183 (−73%) | 183 (−73%) |
| Sliding window | Yes | 179 | 119 (−34%) | 124 (−31%) | 135 (−25%) | 135 (−25%) | 135 (−25%) | 127 (−29%) | 127 (−29%) |
| Random churn | No | 715 | 468 (−35%) | 254 (−64%) | 469 (−34%) | 253 (−65%) | 234 (−67%) | 259 (−64%) | 235 (−67%) |
| Random churn | Yes | 217 | 433 (+100%) | 197 (−9%) | 432 (+99%) | 196 (−10%) | 175 (−19%) | 203 (−6%) | 178 (−18%) |

Output positions assigned, with the change against Blocks. Without a shift, every mode assigns as many positions as
Blocks, and add only assigns 10 million in every mode, so those are omitted; the sliding window assigns the same
whether groups return or not:

| Workload | Groups return | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk |
| --- | --- | --- | --- | --- | --- | --- |
| Sliding window | Either | 10.00M | 2.96M (−70%) | 1.49M (−85%) | 1.49M (−85%) | 1.49M (−85%) |
| Random churn | No | 10.00M | 4.02M (−60%) | 8.59M (−14%) | 2.76M (−72%) | 2.57M (−74%) |
| Random churn | Yes | 8.71M | 3.25M (−63%) | 8.65M (−1%) | 2.62M (−70%) | 2.40M (−72%) |

Longest cycle, in milliseconds:

| Workload | Groups return | Main + #8676 | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk | Collapse 0.75 | Collapse 0.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 19.3 | 25.8 (+34%) | 11.4 (−41%) | 13.5 (−30%) | 14.9 (−23%) | 14.0 (−28%) | 18.2 (−6%) | 13.2 (−31%) |
| Sliding window | No | 46.4 | 8.7 (−81%) | 6.8 (−85%) | 10.3 (−78%) | 10.0 (−79%) | 10.0 (−78%) | 9.6 (−79%) | 9.6 (−79%) |
| Sliding window | Yes | 5.9 | 6.8 (+14%) | 6.2 (+4%) | 6.1 (+3%) | 6.1 (+4%) | 6.7 (+13%) | 7.6 (+28%) | 6.2 (+5%) |
| Random churn | No | 14.4 | 9.5 (−34%) | 8.5 (−41%) | 36.6 (+155%) | 28.9 (+101%) | 19.1 (+33%) | 19.7 (+37%) | 9.9 (−31%) |
| Random churn | Yes | 12.1 | 15.3 (+26%) | 8.0 (−34%) | 42.4 (+250%) | 25.3 (+108%) | 20.7 (+71%) | 10.1 (−17%) | 5.8 (−53%) |

Collapsing alone releases as much memory as collapsing with a shift did. For random churn it retains 235 to 259 MB
when groups do not return and 178 to 203 MB when they do, against 234 to 254 MB and 175 to 197 MB with a shift. An
earlier collapse that combined only adjacent sparse blocks retained 322 to 338 MB and 255 to 277 MB: a released block
between two sparse blocks kept them apart, until a shift removed it. The collapse now reaches across released blocks
instead.

### Reading block counts from the row set

`OutputPositionBlockTracker` stores a live count for every block, and bitsets of the released and the sparse blocks. A
prototype kept none of that. Every block's live count is already in the result's row set, as the difference of two
ranks, and a closed block with no live states is a released one. Each cycle it examined only the blocks the cycle's
removals touched and the blocks that closed: a block that had become empty was released, and one that had become sparse
seeded a run, found by stepping to the previous and next live block across released ones. The only state it kept
between cycles was the number of closed blocks and the sparse blocks whose run the budget of states to move had left
for a later cycle.

It made the same decisions. Two aggregations of the same random churn at a collapse fraction of 0.5, one with each
tracker, had identical result row sets and identical shifts downstream in all of 600 cycles, 426 of which collapsed.

It saved little memory:

| Positions assigned | Blocks | Counting tracker | Row-set tracker |
| --- | --- | --- | --- |
| 10 million | 4,883 | about 17 KB: 2 byte counts for up to 7,824 blocks, and two bitsets | a few hundred bytes |
| 2^31 - 1 | 1,048,576 | about 2.3 MB | a few hundred bytes |

Even at the limit, the counts take 2 bytes a block, where every result column's array source already keeps an 8 byte
reference a block.

And it cost more time. The tracker's own work per cycle, timed around its calls, for 1,000,000 live states with 10,000
removed at random and 10,000 added each cycle, over 2,000 cycles after 1,000 of warmup; two runs agreed within 3%:

| Collapse fraction | Tracker | Update | Collapse | Total |
| --- | --- | --- | --- | --- |
| 1 | counting | 67 µs | — | 67 µs |
| 1 | row set | 174 µs | — | 174 µs |
| 0.5 | counting | 61 µs | 56 µs | 117 µs |
| 0.5 | row set | 101 µs | 238 µs | 340 µs |

A cycle of the random churn benchmark takes about 2.6 ms at 0.5, so the counting tracker is about 4.5% of it and the
row-set tracker about 13%.

Random churn touches nearly every block each cycle, and a block's count costs a search of the row set where the
counting tracker adjusts an array element. At 0.5 each cycle touched 793 blocks, counted in one forward pass of an
iterator over the live states, and seeded 99 runs, whose neighbors took about 191 searches for the previous or next
live block. Those searches, `RspArray.get` and `find` in a profile, were the prototype's cost; a cache kept any block
from being counted twice. Taking the next live block from the forward pass would remove about half of the neighbor
searches, but not the previous live block's, since a block between two touched ones can be sparse, nor the counts of
the touched blocks that are not sparse, 694 of the 793. It would likely remain 1.5 to 2 times the counting tracker.

### Compaction

Compaction shifted the states after removed ones down into their positions, cell by cell where the positions did not
line up with blocks, and gave the positions past the new end back. It released no blocks in the middle of the result.
Each cycle could move no more live states than its input rows, and each cycle started again from the first free
position.

Compaction falls behind whenever removals are concentrated at the front. Keeping the result dense then means moving
every live state each cycle, and the budget covers only the cycle's changes. The sequence below is a sliding window of
6 live groups that removes the 2 oldest and adds 2 new groups each cycle, so each cycle may move 4 states:

| Cycle | States moved | Positions after | Gap | Last row key |
| --- | --- | --- | --- | --- |
| start | — | 0–5 | 0 | 5 |
| 1 | 4 | 0–3, 6–7 | 2 | 7 |
| 2 | 4 | 0–3, 8–9 | 4 | 9 |
| 3 | 4 | 0–3, 10–11 | 6 | 11 |
| 4 | 4 | 0–3, 12–13 | 8 | 13 |
| 5 | 4 | 0–3, 14–15 | 10 | 15 |

Every cycle spends its budget moving the same front states, the gap after them grows by the churn, and the last row
key grows exactly as it would without reclaiming. Compaction falls behind like this whenever the live groups exceed
twice the churn, which is the common case of a large table with small changes.

It also moved cells into holes one value at a time, where the other modes move cells only to free a whole block, and it
needed a `clear` hook on every operator, a range `setNull` on the column sources, and free-position tracking in the
state manager, none of which any other mode uses.

Measured with the benchmark described in [Benchmark results](#benchmark-results), compaction was the weakest mode:

| Workload | Groups return | Time, ms | Heap, MB | Positions, millions |
| --- | --- | --- | --- | --- |
| Add only | — | 865 ± 179 | 567 | 10.00 |
| Sliding window | No | 1053 ± 65 | 481 | 10.00 |
| Sliding window | Yes | 928 ± 105 | 423 | 10.00 |
| Random churn | No | 2750 ± 125 | 525 | 10.00 |
| Random churn | Yes | 2915 ± 153 | 425 | 8.71 |

It never lowered the positions assigned, it was the slowest mode wherever groups return, and for the sliding window it
retained 2.5 to 3.4 times the heap of the modes that collapse. These numbers come from an earlier run than the
tables above, before block shifting had a bulk variant.

### Combining any two blocks that fit

An earlier credit mode, before the bulk shift became a parameter of release blocks, combined blocks in pairs rather
than collapsing runs of sparse blocks: two closed blocks, consecutive among the blocks not released, were combined
whenever their live states fit in one block, releasing the upper one, whatever their fractions free. It shifted blocks
in bulk (see [Block shifting](#block-shifting)).

Collapsing at 0.5 with the bulk shift does nearly as well, and uses one combining mechanism rather than two. Measured in
an earlier run, the pairwise combine assigned 2.44 million positions for random churn with one row per group and
retained 229 MB, against 2.57 million and 234 MB for collapsing at 0.5 with the bulk shift in the tables of
[Block shifting](#block-shifting); with
returning groups, 2.35 million and 170 MB against 2.40 million and 175 MB. Their times were within the error of each
other.

A variant combined the pairs of blocks with the fewest live states first, wherever they were, since every combination
frees one block and the cheapest free the most blocks for the credit. It assigned the same number of positions as
combining from the first block on, because at the benchmark's churn every pair that fits is combined either way. Over
30 measured batches of random churn its longest cycle had a median of 18.2 ms against 16.9 ms, and its batches took
about 1% longer, from sorting the candidate pairs each cycle.

### Assigning new states to released positions

Assigning new states to released positions in the middle of the result would bound the positions assigned without
moving any state. It would give new groups lower row keys than older ones, so the result would no longer list groups
in the order they were first seen, even among groups that never empty. New states therefore always go after every
existing state, and only moves that keep the states in order give positions back.
