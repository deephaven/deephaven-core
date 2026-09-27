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

`StateReclaimMode.releaseBlocks(collapseFreeFraction)` frees the storage of each block once every state in it has been
removed. With the default parameter, no state ever moves:

- The result columns' storage tracks the groups that have rows, but only at block granularity. A block that holds even
  one long-lived state is never released, so random churn with some long-lived groups releases few blocks. The hash
  table does not shrink (see [Costs and limits](#costs-and-limits)), so it stays sized for the most groups the
  aggregation has had at once.
- Output positions are never given back, so the positions assigned grow with every group ever created.

The **collapse** (`collapseFreeFraction` below 1) releases blocks that random churn would otherwise leave nearly empty.
A closed block at least this fraction free is sparse. Runs of adjacent sparse blocks are collapsed: their live states
move to the start of the run, keeping their order, and the blocks this empties are released. A run collapses only if
that frees at least one block. Each cycle moves no more live states than its input rows added, modified, and removed;
a run that does not fit waits for a later cycle. The collapse frees memory but does not give output positions back.

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
  by default). Group-by, partition-by, formula, and rollup operators cannot.
- The aggregation does not preserve empty groups.
- The aggregation has no initial groups.

What happens when they do not hold depends on where the mode came from:

- **The configured mode** (`StateReclaimMode.configured()`, which the public `aggBy` family uses) degrades silently: the
  aggregation behaves as with `none`.
- **A mode chosen explicitly** (`releaseBlocks(...)` rather than `configured()`) fails the
  aggregation with an `IllegalArgumentException` naming the reason — for example, the result columns whose operators
  cannot reclaim states. `StateReclaimMode.isConfigured()` tells the two apart.

A static input table never reclaims states, since no state is ever removed, so any mode is accepted for it.

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

- **Main:** main, which predates reclaiming.
- **Main + #8676:** main with [#8676](https://github.com/deephaven/deephaven-core/pull/8676), which reuses the
  aggregation's modified-states bitmap. Every percentage is the change against this column, the fair baseline for
  this branch.
- **`none`:** this branch without reclaiming, which isolates its other improvements to the update cycle.
- **Blocks:** `releaseBlocks(1)`, the default, which releases blocks and never moves a state.
- **Collapse 0.75** and **Collapse 0.5:** `releaseBlocks(0.75)` and `releaseBlocks(0.5)`, which also collapse runs of
  sparse blocks.

The Blocks and collapse columns come from a later run than the others. Blocks measured within the error bounds of the
earlier run in every workload.

### Time

Milliseconds per batch of 900 cycles; lower is better.

| Workload | Rows per group | Groups return | Main | Main + #8676 | `none` | Blocks | Collapse 0.75 | Collapse 0.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 967 ± 141 (+2%) | 949 ± 116 | 954 ± 152 (+1%) | 857 ± 92 (−10%) | 861 ± 45 (−9%) | 895 ± 103 (−6%) |
| Sliding window | 1 | No | 1457 ± 178 (+7%) | 1367 ± 87 | 1435 ± 61 (+5%) | 867 ± 31 (−37%) | 841 ± 46 (−38%) | 907 ± 231 (−34%) |
| Sliding window | 1 | Yes | 618 ± 50 (+4%) | 595 ± 30 | 754 ± 561 (+27%) | 819 ± 66 (+38%) | 824 ± 65 (+38%) | 800 ± 105 (+34%) |
| Random churn | 1 | No | 3131 ± 156 (+11%) | 2827 ± 127 | 2624 ± 73 (−7%) | 2545 ± 219 (−10%) | 2302 ± 298 (−19%) | 2271 ± 173 (−20%) |
| Random churn | 1 | Yes | 2067 ± 122 (+10%) | 1873 ± 49 | 1589 ± 68 (−15%) | 2374 ± 81 (+27%) | 2208 ± 120 (+18%) | 2469 ± 284 (+32%) |
| Add only | 100 | — | 116 ± 14 (−4%) | 120 ± 19 | 115 ± 18 (−4%) | 125 ± 39 (+4%) | 125 ± 12 (+4%) | 124 ± 11 (+3%) |
| Sliding window | 100 | No | 203 ± 36 (+2%) | 200 ± 17 | 197 ± 17 (−2%) | 181 ± 15 (−9%) | 192 ± 32 (−4%) | 204 ± 20 (+2%) |
| Sliding window | 100 | Yes | 200 ± 15 (+7%) | 187 ± 23 | 188 ± 10 (+0%) | 200 ± 13 (+7%) | 196 ± 15 (+5%) | 199 ± 17 (+6%) |
| Random churn | 100 | No | 674 ± 21 (+6%) | 637 ± 40 | 635 ± 45 (−0%) | 632 ± 42 (−1%) | 637 ± 24 (−0%) | 627 ± 59 (−2%) |
| Random churn | 100 | Yes | 608 ± 33 (+3%) | 591 ± 11 | 592 ± 24 (+0%) | 588 ± 23 (−0%) | 589 ± 53 (−0%) | 581 ± 10 (−2%) |

### Memory

The heap retained at the end of a batch, in megabytes, after a garbage collection. It includes the source table and
the benchmark's own data, so compare the modes with each other rather than reading the values as the aggregation's
size.

| Workload | Rows per group | Groups return | Main | Main + #8676 | `none` | Blocks | Collapse 0.75 | Collapse 0.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 673 (−0%) | 675 | 673 (−0%) | 571 (−15%) | 567 (−16%) | 569 (−16%) |
| Sliding window | 1 | No | 670 (−1%) | 676 | 681 (+1%) | 175 (−74%) | 183 (−73%) | 183 (−73%) |
| Sliding window | 1 | Yes | 178 (−1%) | 179 | 180 (+0%) | 119 (−33%) | 127 (−29%) | 127 (−29%) |
| Random churn | 1 | No | 710 (−1%) | 715 | 717 (+0%) | 468 (−35%) | 338 (−53%) | 322 (−55%) |
| Random churn | 1 | Yes | 217 (−0%) | 217 | 217 (+0%) | 432 (+99%) | 277 (+28%) | 255 (+18%) |
| Add only | 100 | — | 28 (+0%) | 28 | 28 (+0%) | 27 (−4%) | 26 (−7%) | 26 (−7%) |
| Sliding window | 100 | No | 28 (+0%) | 28 | 28 (+0%) | 23 (−18%) | 23 (−18%) | 23 (−18%) |
| Sliding window | 100 | Yes | 20 (+0%) | 20 | 20 (+0%) | 23 (+15%) | 23 (+15%) | 23 (+15%) |
| Random churn | 100 | No | 52 (+2%) | 51 | 51 (+0%) | 46 (−10%) | 50 (−2%) | 49 (−4%) |
| Random churn | 100 | Yes | 42 (+2%) | 41 | 42 (+2%) | 41 (+0%) | 45 (+10%) | 44 (+7%) |

### Output positions assigned

The positions assigned at the end of a batch. Over a batch, one row per group creates 10 million groups, or cycles
through 2 million keys when groups return; 100 rows per group creates 100,000 groups, or cycles through 20,000 keys.

| Workload | Rows per group | Groups return | Main | Main + #8676 | `none` | Blocks | Collapse 0.75 | Collapse 0.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 10.00M (+0%) | 10.00M | 10.00M (+0%) | 10.00M (+0%) | 10.00M (+0%) | 10.00M (+0%) |
| Sliding window | 1 | No | 10.00M (+0%) | 10.00M | 10.00M (+0%) | 10.00M (+0%) | 10.00M (+0%) | 10.00M (+0%) |
| Sliding window | 1 | Yes | 2.00M (+0%) | 2.00M | 2.00M (+0%) | 10.00M (+400%) | 10.00M (+400%) | 10.00M (+400%) |
| Random churn | 1 | No | 10.00M (+0%) | 10.00M | 10.00M (+0%) | 10.00M (+0%) | 10.00M (+0%) | 10.00M (+0%) |
| Random churn | 1 | Yes | 2.00M (+0%) | 2.00M | 2.00M (+0%) | 8.71M (+335%) | 8.71M (+335%) | 8.71M (+335%) |
| Add only | 100 | — | 100.0k (+0%) | 100.0k | 100.0k (+0%) | 100.0k (+0%) | 100.0k (+0%) | 100.0k (+0%) |
| Sliding window | 100 | No | 100.0k (+0%) | 100.0k | 100.0k (+0%) | 100.0k (+0%) | 100.0k (+0%) | 100.0k (+0%) |
| Sliding window | 100 | Yes | 20.0k (+0%) | 20.0k | 20.0k (+0%) | 100.0k (+400%) | 100.0k (+400%) | 100.0k (+400%) |
| Random churn | 100 | No | 100.0k (+0%) | 100.0k | 100.0k (+0%) | 100.0k (+0%) | 100.0k (+0%) | 100.0k (+0%) |
| Random churn | 100 | Yes | 20.0k (+0%) | 20.0k | 20.0k (+0%) | 20.0k (+0%) | 20.0k (+0%) | 20.0k (+0%) |

### Longest cycle

The longest single cycle of a batch, in milliseconds, the worst over five batches. A single worst sample is noisy —
the same configuration varied by a factor of two between runs — so this table has no percentages; read it for the
modes whose longest cycles are consistently high.

| Workload | Rows per group | Groups return | Main | Main + #8676 | `none` | Blocks | Collapse 0.75 | Collapse 0.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 16.4 | 19.3 | 26.8 | 14.4 | 16.6 | 13.4 |
| Sliding window | 1 | No | 25.0 | 46.4 | 33.1 | 6.4 | 10.4 | 8.9 |
| Sliding window | 1 | Yes | 4.8 | 5.9 | 8.8 | 6.0 | 11.2 | 6.0 |
| Random churn | 1 | No | 11.1 | 14.4 | 11.0 | 11.8 | 8.5 | 7.3 |
| Random churn | 1 | Yes | 12.5 | 12.1 | 10.9 | 10.6 | 7.5 | 7.8 |
| Add only | 100 | — | 0.4 | 0.4 | 0.5 | 0.6 | 0.5 | 0.4 |
| Sliding window | 100 | No | 2.7 | 0.6 | 0.5 | 0.5 | 1.0 | 0.6 |
| Sliding window | 100 | Yes | 0.6 | 0.5 | 0.6 | 0.5 | 0.5 | 0.5 |
| Random churn | 100 | No | 1.4 | 1.5 | 1.5 | 2.1 | 1.6 | 1.4 |
| Random churn | 100 | Yes | 1.9 | 1.1 | 1.4 | 1.2 | 1.3 | 1.1 |

### Findings

- #8676 makes random churn with one row per group about 10% faster: main takes 10% to 11% longer. Elsewhere main and
  main with #8676 are within the error of each other, and they retain the same heap.
- This branch without reclaiming (`none`) is 7% faster than main with #8676 for random churn when groups do not return,
  and 15% faster when they do, from its other improvements to the update cycle. Elsewhere it is within the error.
- When groups do not return, every reclaiming mode is faster than main with #8676 for the sliding window, by 34% to
  38%, and retains about a quarter of its heap. For random churn, releasing blocks alone is 10% faster and retains
  35% less heap; collapsing is 19% to 20% faster and retains 53% to 55% less.
- When groups return, every reclaiming mode is 18% to 38% slower than main with #8676, since returning groups are new
  states rather than reused ones. Every mode retains less heap for the sliding window. For random churn, releasing
  blocks alone retains twice the heap of main with #8676, and collapsing 18% to 28% more.
- No mode gives output positions back, so one row per group assigns 10 million positions when groups do not return,
  and 8.71 to 10 million when they do, where main reuses 2 million.
- With 100 rows per group, the modes are within the error of each other for time.
- No mode has long cycles: the longest were 7 to 12 ms for random churn with one row per group.

The numbers come from one machine and one JVM fork per configuration, with five measured batches each; treat
differences within the error bounds as noise.

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

**The complete conversion, without reclaiming.** Only the downstream positions are wider, and nothing measurable
changes:

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

## Related documentation

- `StateReclaimMode` (`engine/table/src/main/java/io/deephaven/engine/table/impl/by/StateReclaimMode.java`)
- `OutputPositionBlockTracker` (`engine/table/src/main/java/io/deephaven/engine/table/impl/by/OutputPositionBlockTracker.java`)
- `ChunkedOperatorAggregationHelper` (`engine/table/src/main/java/io/deephaven/engine/table/impl/by/ChunkedOperatorAggregationHelper.java`)
- `IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBaseWithTombstones` (`engine/table/src/main/java/io/deephaven/engine/table/impl/by/IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBaseWithTombstones.java`)
- `AggregationIncrementalBenchmark` (`engine/benchmark/src/main/java/io/deephaven/benchmark/engine/AggregationIncrementalBenchmark.java`)

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

Its benefits were lower output positions and, for random churn, more memory released by the collapse (see below). Its
costs:

- **Latency.** The bulk shift had the longest cycles for random churn, 19 to 42 ms against 7 to 15 ms without it.
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

Measured with the benchmark described in [Benchmark results](#benchmark-results), one row per group, with percentages
against main with #8676 as there. Blocks releases blocks with no moves. The shifting modes were
`releaseBlocks(0.75, 0, false)`, which collapsed and swept (Collapse, sweep); `releaseBlocks(1, 0, true)`, which
shifted in bulk without collapsing (Bulk); and `releaseBlocks(0.75, 0, true)` and `releaseBlocks(0.5, 0, true)`, which
collapsed and shifted in bulk. The last two columns collapse without shifting, from the later run in
[Benchmark results](#benchmark-results).

Time, in milliseconds per batch of 900 cycles:

| Workload | Rows per group | Groups return | Main + #8676 | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk | Collapse 0.75 | Collapse 0.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 949 ± 116 | 873 ± 83 (−8%) | 922 ± 137 (−3%) | 950 ± 32 (+0%) | 917 ± 65 (−3%) | 922 ± 43 (−3%) | 861 ± 45 (−9%) | 895 ± 103 (−6%) |
| Sliding window | 1 | No | 1367 ± 87 | 822 ± 43 (−40%) | 907 ± 77 (−34%) | 968 ± 128 (−29%) | 906 ± 16 (−34%) | 926 ± 70 (−32%) | 841 ± 46 (−38%) | 907 ± 231 (−34%) |
| Sliding window | 1 | Yes | 595 ± 30 | 726 ± 90 (+22%) | 890 ± 51 (+50%) | 784 ± 56 (+32%) | 810 ± 47 (+36%) | 871 ± 22 (+46%) | 824 ± 65 (+38%) | 800 ± 105 (+34%) |
| Random churn | 1 | No | 2827 ± 127 | 2318 ± 70 (−18%) | 2537 ± 121 (−10%) | 2818 ± 89 (−0%) | 2374 ± 92 (−16%) | 2420 ± 79 (−14%) | 2302 ± 298 (−19%) | 2271 ± 173 (−20%) |
| Random churn | 1 | Yes | 1873 ± 49 | 2460 ± 211 (+31%) | 2470 ± 175 (+32%) | 2868 ± 392 (+53%) | 2393 ± 155 (+28%) | 2274 ± 210 (+21%) | 2208 ± 120 (+18%) | 2469 ± 284 (+32%) |

Retained heap, in megabytes:

| Workload | Rows per group | Groups return | Main + #8676 | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk | Collapse 0.75 | Collapse 0.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 675 | 568 (−16%) | 569 (−16%) | 568 (−16%) | 567 (−16%) | 568 (−16%) | 567 (−16%) | 569 (−16%) |
| Sliding window | 1 | No | 676 | 175 (−74%) | 180 (−73%) | 191 (−72%) | 190 (−72%) | 190 (−72%) | 183 (−73%) | 183 (−73%) |
| Sliding window | 1 | Yes | 179 | 119 (−34%) | 124 (−31%) | 135 (−25%) | 135 (−25%) | 135 (−25%) | 127 (−29%) | 127 (−29%) |
| Random churn | 1 | No | 715 | 468 (−35%) | 254 (−64%) | 469 (−34%) | 253 (−65%) | 234 (−67%) | 338 (−53%) | 322 (−55%) |
| Random churn | 1 | Yes | 217 | 433 (+100%) | 197 (−9%) | 432 (+99%) | 196 (−10%) | 175 (−19%) | 277 (+28%) | 255 (+18%) |

Output positions assigned:

| Workload | Rows per group | Groups return | Main + #8676 | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk | Collapse 0.75 | Collapse 0.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 10.00M | 10.00M (+0%) | 10.00M (+0%) | 10.00M (+0%) | 10.00M (+0%) | 10.00M (+0%) | 10.00M (+0%) | 10.00M (+0%) |
| Sliding window | 1 | No | 10.00M | 10.00M (+0%) | 2.96M (−70%) | 1.49M (−85%) | 1.49M (−85%) | 1.49M (−85%) | 10.00M (+0%) | 10.00M (+0%) |
| Sliding window | 1 | Yes | 2.00M | 10.00M (+400%) | 2.96M (+48%) | 1.49M (−25%) | 1.49M (−25%) | 1.49M (−25%) | 10.00M (+400%) | 10.00M (+400%) |
| Random churn | 1 | No | 10.00M | 10.00M (+0%) | 4.02M (−60%) | 8.59M (−14%) | 2.76M (−72%) | 2.57M (−74%) | 10.00M (+0%) | 10.00M (+0%) |
| Random churn | 1 | Yes | 2.00M | 8.71M (+335%) | 3.25M (+63%) | 8.65M (+333%) | 2.62M (+31%) | 2.40M (+20%) | 8.71M (+335%) | 8.71M (+335%) |

Longest cycle, in milliseconds:

| Workload | Rows per group | Groups return | Main + #8676 | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk | Collapse 0.75 | Collapse 0.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 19.3 | 25.8 | 11.4 | 13.5 | 14.9 | 14.0 | 16.6 | 13.4 |
| Sliding window | 1 | No | 46.4 | 8.7 | 6.8 | 10.3 | 10.0 | 10.0 | 10.4 | 8.9 |
| Sliding window | 1 | Yes | 5.9 | 6.8 | 6.2 | 6.1 | 6.1 | 6.7 | 11.2 | 6.0 |
| Random churn | 1 | No | 14.4 | 9.5 | 8.5 | 36.6 | 28.9 | 19.1 | 8.5 | 7.3 |
| Random churn | 1 | Yes | 12.1 | 15.3 | 8.0 | 42.4 | 25.3 | 20.7 | 7.5 | 7.8 |

Shifting also helped the collapse. For random churn, collapsing with a shift retained 234 to 254 MB when groups do not
return and 175 to 197 MB when they do, against 322 to 338 MB and 255 to 277 MB for collapsing alone. A run to collapse
must be adjacent sparse blocks, and a released block between two sparse blocks separates them; shifting removed the
released blocks from between the live ones, which is the likely reason.

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
