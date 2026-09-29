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
  in the cycle has run, so previous values stay readable for the whole cycle that removed the states.

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

The columns:

- **Main:** main at c091e0578e, which predates reclaiming. Every percentage is the change against this column.
- **`none`:** this branch without reclaiming, which isolates its other improvements to the update cycle.
- **Blocks:** `releaseBlocks(1)`, the default, which releases blocks and never moves a state.
- **Collapse 0.75** and **Collapse 0.5:** `releaseBlocks(0.75)` and `releaseBlocks(0.5)`, which also collapse runs of
  sparse blocks.
- **Main again:** main once more, after every other configuration, to show any drift over the run.

The branch was measured at e631d807bb, before the block tracker walked sparse runs incrementally and applied a
collapse to each row set once for all runs; those changes affect only the collapse columns.

The numbers come from an Intel i9-14900KS (hybrid performance and efficiency cores) in an LXC container with 24 logical
CPUs and 128 GB, with the performance governor and turbo boost enabled, and otherwise idle. Each configuration ran two
JVM forks of three warmup and five measured batches; the error bounds are JMH's 99.9% intervals over the ten measured
batches. Main and Main again are within the error of each other in every workload, so the run did not drift.

### Time

Milliseconds per batch of 900 cycles; lower is better.

| Workload | Rows per group | Groups return | Main | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Main again |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 1334 ± 39 | 1350 ± 69 (+1%) | 1287 ± 56 (−4%) | 1292 ± 152 (−3%) | 1291 ± 91 (−3%) | 1386 ± 90 (+4%) |
| Sliding window | 1 | No | 2024 ± 97 | 2014 ± 155 (−0%) | 1309 ± 44 (−35%) | 1383 ± 78 (−32%) | 1358 ± 87 (−33%) | 1989 ± 79 (−2%) |
| Sliding window | 1 | Yes | 1072 ± 61 | 1083 ± 60 (+1%) | 1179 ± 54 (+10%) | 1254 ± 60 (+17%) | 1258 ± 73 (+17%) | 1057 ± 35 (−1%) |
| Random churn | 1 | No | 4537 ± 124 | 4228 ± 66 (−7%) | 3968 ± 103 (−13%) | 3468 ± 59 (−24%) | 3500 ± 85 (−23%) | 4517 ± 88 (−0%) |
| Random churn | 1 | Yes | 2966 ± 84 | 2595 ± 76 (−13%) | 4134 ± 106 (+39%) | 3458 ± 100 (+17%) | 3459 ± 93 (+17%) | 2946 ± 74 (−1%) |
| Add only | 100 | — | 170 ± 28 | 176 ± 23 (+3%) | 191 ± 26 (+12%) | 186 ± 17 (+9%) | 185 ± 38 (+9%) | 173 ± 20 (+2%) |
| Sliding window | 100 | No | 286 ± 34 | 314 ± 64 (+10%) | 301 ± 32 (+5%) | 290 ± 15 (+1%) | 310 ± 32 (+8%) | 298 ± 24 (+4%) |
| Sliding window | 100 | Yes | 306 ± 22 | 310 ± 24 (+1%) | 308 ± 20 (+1%) | 329 ± 25 (+7%) | 329 ± 36 (+7%) | 297 ± 27 (−3%) |
| Random churn | 100 | No | 849 ± 75 | 836 ± 22 (−2%) | 789 ± 38 (−7%) | 857 ± 56 (+1%) | 827 ± 69 (−3%) | 808 ± 59 (−5%) |
| Random churn | 100 | Yes | 791 ± 52 | 784 ± 72 (−1%) | 789 ± 74 (−0%) | 781 ± 49 (−1%) | 790 ± 80 (−0%) | 768 ± 65 (−3%) |

### Memory

The heap retained at the end of a batch, in megabytes, after a garbage collection, averaged over the measured
batches. It includes the source table and the benchmark's own data, so compare the modes with each other rather than
reading the values as the aggregation's size.

| Workload | Rows per group | Groups return | Main | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Main again |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 675 | 674 (−0%) | 572 (−15%) | 578 (−14%) | 577 (−15%) | 675 (−0%) |
| Sliding window | 1 | No | 680 | 684 (+1%) | 186 (−73%) | 186 (−73%) | 186 (−73%) | 679 (−0%) |
| Sliding window | 1 | Yes | 181 | 182 (+0%) | 130 (−28%) | 130 (−28%) | 130 (−28%) | 181 (+0%) |
| Random churn | 1 | No | 716 | 721 (+1%) | 472 (−34%) | 264 (−63%) | 239 (−67%) | 716 (−0%) |
| Random churn | 1 | Yes | 219 | 220 (+0%) | 437 (+99%) | 208 (−5%) | 182 (−17%) | 219 (−0%) |
| Add only | 100 | — | 29 | 29 (+0%) | 28 (−3%) | 32 (+10%) | 32 (+10%) | 29 (+0%) |
| Sliding window | 100 | No | 30 | 29 (−2%) | 26 (−15%) | 26 (−13%) | 26 (−15%) | 30 (−1%) |
| Sliding window | 100 | Yes | 21 | 21 (−1%) | 25 (+18%) | 25 (+18%) | 25 (+18%) | 21 (−1%) |
| Random churn | 100 | No | 52 | 53 (+1%) | 48 (−9%) | 52 (−1%) | 52 (−2%) | 52 (−0%) |
| Random churn | 100 | Yes | 42 | 43 (+1%) | 43 (+0%) | 47 (+11%) | 47 (+11%) | 42 (+0%) |

### Longest cycle

The longest single cycle of a batch, in milliseconds, the worst over the ten measured batches. A single worst sample is
noisy, as the difference between Main and Main again shows, so read the percentages for the modes whose longest cycles
are consistently high rather than for small differences. With 100 rows per group every longest cycle is a few
milliseconds, where one slow sample swings the percentage widely.

| Workload | Rows per group | Groups return | Main | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Main again |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 62.5 | 62.5 (+0%) | 63.2 (+1%) | 65.4 (+5%) | 63.8 (+2%) | 64.9 (+4%) |
| Sliding window | 1 | No | 73.4 | 64.5 (−12%) | 12.0 (−84%) | 18.2 (−75%) | 14.3 (−80%) | 70.7 (−4%) |
| Sliding window | 1 | Yes | 10.9 | 13.2 (+21%) | 12.9 (+18%) | 15.5 (+42%) | 14.5 (+33%) | 20.4 (+87%) |
| Random churn | 1 | No | 67.3 | 65.2 (−3%) | 17.6 (−74%) | 14.0 (−79%) | 16.9 (−75%) | 28.8 (−57%) |
| Random churn | 1 | Yes | 11.5 | 10.5 (−8%) | 14.5 (+27%) | 14.2 (+24%) | 15.3 (+33%) | 11.0 (−4%) |
| Add only | 100 | — | 2.6 | 1.4 (−45%) | 1.7 (−36%) | 1.0 (−61%) | 1.0 (−62%) | 1.0 (−62%) |
| Sliding window | 100 | No | 1.3 | 2.6 (+98%) | 1.2 (−6%) | 1.1 (−17%) | 2.5 (+93%) | 2.0 (+56%) |
| Sliding window | 100 | Yes | 1.3 | 2.4 (+81%) | 1.1 (−15%) | 1.1 (−14%) | 1.2 (−6%) | 1.0 (−21%) |
| Random churn | 100 | No | 3.7 | 3.5 (−4%) | 3.4 (−9%) | 3.2 (−12%) | 2.6 (−29%) | 3.0 (−19%) |
| Random churn | 100 | Yes | 3.2 | 3.8 (+19%) | 2.6 (−18%) | 3.1 (−4%) | 3.2 (+0%) | 3.3 (+4%) |

### Ten million rows

The same workloads with one row per group over a table of 10,000,000 rows, with 100,000 rows added and removed each
cycle, run with a 48 GB heap.

Milliseconds per batch of 900 cycles:

| Workload | Groups return | Main | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Main again |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 15750 ± 232 | 15387 ± 349 (−2%) | 13888 ± 252 (−12%) | 14294 ± 204 (−9%) | 14349 ± 214 (−9%) | 15423 ± 342 (−2%) |
| Sliding window | No | 21787 ± 721 | 21806 ± 202 (+0%) | 16175 ± 142 (−26%) | 16230 ± 224 (−26%) | 16092 ± 236 (−26%) | 22098 ± 560 (+1%) |
| Sliding window | Yes | 12081 ± 124 | 12269 ± 88 (+2%) | 13512 ± 144 (+12%) | 13769 ± 117 (+14%) | 13889 ± 164 (+15%) | 12146 ± 153 (+1%) |
| Random churn | No | 56480 ± 661 | 53407 ± 565 (−5%) | 49386 ± 468 (−13%) | 44943 ± 459 (−20%) | 43002 ± 583 (−24%) | 56393 ± 552 (−0%) |
| Random churn | Yes | 38650 ± 382 | 34928 ± 343 (−10%) | 50184 ± 894 (+30%) | 42989 ± 471 (+11%) | 40911 ± 401 (+6%) | 38752 ± 595 (+0%) |

Retained heap, in megabytes:

| Workload | Groups return | Main | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Main again |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 5623 | 5622 (−0%) | 5083 (−10%) | 5091 (−9%) | 5090 (−9%) | 5624 (+0%) |
| Sliding window | No | 5658 | 5691 (+1%) | 897 (−84%) | 900 (−84%) | 899 (−84%) | 5659 (+0%) |
| Sliding window | Yes | 1426 | 1434 (+1%) | 897 (−37%) | 900 (−37%) | 899 (−37%) | 1427 (+0%) |
| Random churn | No | 5896 | 5929 (+1%) | 4058 (−31%) | 1966 (−67%) | 1747 (−70%) | 5894 (−0%) |
| Random churn | Yes | 1662 | 1672 (+1%) | 3810 (+129%) | 1526 (−8%) | 1300 (−22%) | 1663 (+0%) |

Longest cycle, in milliseconds:

| Workload | Groups return | Main | `none` | Blocks | Collapse 0.75 | Collapse 0.5 | Main again |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 489.3 | 500.9 (+2%) | 494.1 (+1%) | 533.4 (+9%) | 498.9 (+2%) | 505.3 (+3%) |
| Sliding window | No | 498.3 | 497.1 (−0%) | 147.4 (−70%) | 147.1 (−70%) | 147.6 (−70%) | 498.1 (−0%) |
| Sliding window | Yes | 137.6 | 110.4 (−20%) | 115.1 (−16%) | 149.6 (+9%) | 149.2 (+8%) | 142.6 (+4%) |
| Random churn | No | 309.1 | 537.4 (+74%) | 155.0 (−50%) | 138.0 (−55%) | 145.4 (−53%) | 533.7 (+73%) |
| Random churn | Yes | 137.3 | 84.5 (−38%) | 107.6 (−22%) | 162.9 (+19%) | 138.6 (+1%) | 108.6 (−21%) |

### Findings

- This branch without reclaiming (`none`) is 7% faster than main for random churn with one row per group when groups
  do not return, and 13% faster when they do, from its other improvements to the update cycle; at ten million rows,
  5% and 10%. Elsewhere it is within the error.
- When groups do not return, every reclaiming mode is faster than main for the sliding window, by 32% to 35%, and
  retains about a quarter of its heap; at ten million rows, 26% faster with a sixth of the heap. For random churn,
  releasing blocks alone is 13% faster and retains a third less heap; collapsing is 23% to 24% faster and retains a
  third of the heap. At ten million rows, collapsing is 20% to 24% faster.
- When groups return, reclaiming is slower than main, since returning groups are new states rather than reused ones:
  17% collapsing for the sliding window, where releasing blocks alone is within the error, and for random churn 39%
  releasing blocks alone and 17% collapsing.
  For random churn, releasing blocks alone retains twice the heap of main, and collapsing 5% to 17% less. At ten million
  rows, random churn with returning groups costs 30% releasing blocks alone and 6% to 11% collapsing.
- Collapsing is faster than releasing blocks alone for random churn: by 12% to 13% when groups do not return and 16%
  when they do, and at ten million rows by 9% to 13% and 14% to 18%.
- No mode gives output positions back, so every reclaiming mode assigns the same positions: with one row per group,
  10 million when groups do not return, and 8.71 to 10 million when they do, where main reuses 2 million; with 100 rows
  per group, 100,000, or 20,000 for random churn with returning groups.
- With 100 rows per group, the modes are within the error of each other for time.
- Reclaiming shortens the longest cycles when groups do not return: 12 to 18 ms against main's 67 to 73 ms for the
  sliding window and random churn, and at ten million rows 138 to 155 ms against 309 to 534 ms for main and main
  again. When groups return, the longest cycles are 10 to 16 ms for every mode and for main, whose rerun reached 20 ms
  once.

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
