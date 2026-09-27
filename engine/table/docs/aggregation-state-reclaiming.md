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
- **Moves keep the order of the states.** When a mode moves states to give output positions back, the states keep
  their relative order. Downstream listeners see the moves as the shifts of a `TableUpdate`.
- **A group keeps its row key while it has rows,** unless the mode moves states. A group that empties and returns is a
  new row, at the end of the result. This differs from the encounter-order promise of an aggregation that does not
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

`StateReclaimMode.releaseBlocks(collapseFreeFraction, blockShiftFraction, bulkShift)` frees the storage of each block
once every state in it has been removed. With the default parameters, no state ever moves:

- The result columns' storage tracks the groups that have rows, but only at block granularity. A block that holds even
  one long-lived state is never released, so random churn with some long-lived groups releases few blocks. The hash
  table does not shrink (see [Costs and limits](#costs-and-limits)), so it stays sized for the most groups the
  aggregation has had at once.
- Output positions are never given back, so the positions assigned grow with every group ever created.

The three parameters control the two moves that reduce both limits:

- **Collapse** (`collapseFreeFraction` below 1) combines adjacent runs of blocks. A closed block at least this fraction
  free is sparse. Runs of adjacent sparse blocks are collapsed: their live states move to the start of the run, and the
  blocks this empties are released. A run collapses only if that frees at least one block. Collapse frees memory but
  does not give output positions back.
- **Block shift** (`blockShiftFraction` zero or more) moves blocks down over released ones, as whole blocks, so that
  output positions are given back. It runs only once the released blocks are at least this fraction of the positions
  assigned, so that many blocks do not shift to fill very small holes.
- **Bulk shift** (`bulkShift`) chooses how the block shift proceeds:
  - Without it, the shift sweeps toward the end over several cycles and resumes where it stopped. Each cycle may move
    no more live states than its input rows added, modified, and removed, shared with the collapse; unspent budget of
    up to one block carries to the next cycle. When the sweep reaches the end, the released blocks are given back and
    the next output position moves down.
  - With it, the shift waits until it can shift every block after the first released one in one cycle, so that it
    always frees space at the end, and gives every released block back. Moves are paid for by a credit: each cycle
    earns the number of states it added and removed, and unspent credit carries to later cycles, up to the number of
    positions assigned. The collapse spends from the same credit first.

The block shift moves whole blocks, so the array-backed sources never move its states one value at a time. A source
that does not track previous values moves each block by reference. The result columns track previous values, so a
block whose array must also hold this cycle's previous values is copied into a new current block, one array copy per
block. Each moved state also costs the hash table's update of its output position.

A sweeping shift chases a tail that grows with each cycle's new groups, and it moves blocks near the front that empty
soon after, so for a sliding window with `L` live groups the positions assigned peak near `3 L`. A bulk shift for the
same window runs whenever the credit reaches `L`, when about `L / 2` positions have been added past the live states,
so the positions assigned stay near `1.5 L`. On average the bulk shift moves no more states per cycle than the cycle
added and removed, but it concentrates that work: every live state after the first released block moves in one cycle.

## Parameters

The configuration properties set the mode that `StateReclaimMode.configured()` returns. It reads them as each
aggregation is created, so changing them affects only aggregations created afterward. Tests change them through the
matching public static fields of `ChunkedOperatorAggregationHelper`.

| Property | Field | Default | Meaning |
| --- | --- | --- | --- |
| `ChunkedOperatorAggregationHelper.reclaimStates` | `RECLAIM_STATES` | `true` | Whether states are reclaimed at all. `false` selects `none`. |
| `ChunkedOperatorAggregationHelper.collapseFreeFraction` | `COLLAPSE_FREE_FRACTION` | `1.0` | The fraction free at which a closed block is sparse and may be collapsed. 1 or more never collapses. |
| `ChunkedOperatorAggregationHelper.blockShiftFraction` | `BLOCK_SHIFT_FRACTION` | `-1.0` | The fraction of the positions assigned that released blocks must reach before blocks shift down. 0 shifts for any released block; negative never shifts. |
| `ChunkedOperatorAggregationHelper.bulkShift` | `BULK_SHIFT` | `false` | Whether the block shift waits until it can reach the end in one cycle, paid for by credit carried across cycles, rather than sweeping over several cycles. |

The defaults select `releaseBlocks(1, -1, false)`: blocks are released as they empty and no state moves. A fraction
must not be `NaN`; `releaseBlocks` rejects one.

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

- Every operator of the aggregation can reclaim states. Group-by, partition-by, formula, and rollup operators cannot.
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
- **Output positions are ints.** A mode that never gives positions back — release blocks without the block shift —
  fails with `Aggregation output positions exhausted` once 2^31 - 1 states have been created over the life of the
  aggregation. At 10,000 new groups per second that is about 2.5 days.
- **The hash table does not shrink.** Tombstones keep their keys until a rehash drops them. Growth is driven by the live
  states: when tombstones alone cross the load factor, the table rehashes at the same size.
- **Key columns track previous values only when states can move.** The copied key columns start tracking previous
  values for collapse and block shift, but not for release blocks without moves.
- **The bulk shift has a latency cost.** The cycle that shifts moves every live state after the first released block.

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
- **Blocks:** `releaseBlocks(1, -1, false)`, the default, which releases blocks and never moves a state.
- **Collapse, sweep:** `releaseBlocks(0.75, 0, false)`, which collapses sparse blocks and sweeps blocks down over
  several cycles.
- **Bulk:** `releaseBlocks(1, 0, true)`, which shifts blocks in bulk without collapsing.
- **Collapse 0.75, bulk** and **Collapse 0.5, bulk:** `releaseBlocks(0.75, 0, true)` and `releaseBlocks(0.5, 0, true)`.

### Time

Milliseconds per batch of 900 cycles; lower is better.

| Workload | Rows per group | Groups return | Main | Main + #8676 | `none` | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 967 ± 141 (+2%) | 949 ± 116 | 954 ± 152 (+1%) | 873 ± 83 (−8%) | 922 ± 137 (−3%) | 950 ± 32 (+0%) | 917 ± 65 (−3%) | 922 ± 43 (−3%) |
| Sliding window | 1 | No | 1457 ± 178 (+7%) | 1367 ± 87 | 1435 ± 61 (+5%) | 822 ± 43 (−40%) | 907 ± 77 (−34%) | 968 ± 128 (−29%) | 906 ± 16 (−34%) | 926 ± 70 (−32%) |
| Sliding window | 1 | Yes | 618 ± 50 (+4%) | 595 ± 30 | 754 ± 561 (+27%) | 726 ± 90 (+22%) | 890 ± 51 (+50%) | 784 ± 56 (+32%) | 810 ± 47 (+36%) | 871 ± 22 (+46%) |
| Random churn | 1 | No | 3131 ± 156 (+11%) | 2827 ± 127 | 2624 ± 73 (−7%) | 2318 ± 70 (−18%) | 2537 ± 121 (−10%) | 2818 ± 89 (−0%) | 2374 ± 92 (−16%) | 2420 ± 79 (−14%) |
| Random churn | 1 | Yes | 2067 ± 122 (+10%) | 1873 ± 49 | 1589 ± 68 (−15%) | 2460 ± 211 (+31%) | 2470 ± 175 (+32%) | 2868 ± 392 (+53%) | 2393 ± 155 (+28%) | 2274 ± 210 (+21%) |
| Add only | 100 | — | 116 ± 14 (−4%) | 120 ± 19 | 115 ± 18 (−4%) | 126 ± 13 (+5%) | 130 ± 17 (+8%) | 130 ± 18 (+8%) | 138 ± 47 (+15%) | 129 ± 11 (+7%) |
| Sliding window | 100 | No | 203 ± 36 (+2%) | 200 ± 17 | 197 ± 17 (−2%) | 187 ± 12 (−6%) | 200 ± 18 (−0%) | 203 ± 21 (+1%) | 205 ± 16 (+2%) | 187 ± 13 (−7%) |
| Sliding window | 100 | Yes | 200 ± 15 (+7%) | 187 ± 23 | 188 ± 10 (+0%) | 200 ± 16 (+7%) | 205 ± 14 (+10%) | 191 ± 14 (+2%) | 206 ± 8 (+10%) | 193 ± 11 (+3%) |
| Random churn | 100 | No | 674 ± 21 (+6%) | 637 ± 40 | 635 ± 45 (−0%) | 637 ± 30 (−0%) | 662 ± 44 (+4%) | 641 ± 45 (+1%) | 649 ± 48 (+2%) | 628 ± 10 (−1%) |
| Random churn | 100 | Yes | 608 ± 33 (+3%) | 591 ± 11 | 592 ± 24 (+0%) | 596 ± 14 (+1%) | 600 ± 50 (+2%) | 590 ± 33 (−0%) | 631 ± 110 (+7%) | 601 ± 29 (+2%) |

### Memory

The heap retained at the end of a batch, in megabytes, after a garbage collection. It includes the source table and
the benchmark's own data, so compare the modes with each other rather than reading the values as the aggregation's
size.

| Workload | Rows per group | Groups return | Main | Main + #8676 | `none` | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 673 (−0%) | 675 | 673 (−0%) | 568 (−16%) | 569 (−16%) | 568 (−16%) | 567 (−16%) | 568 (−16%) |
| Sliding window | 1 | No | 670 (−1%) | 676 | 681 (+1%) | 175 (−74%) | 180 (−73%) | 191 (−72%) | 190 (−72%) | 190 (−72%) |
| Sliding window | 1 | Yes | 178 (−1%) | 179 | 180 (+0%) | 119 (−34%) | 124 (−31%) | 135 (−25%) | 135 (−25%) | 135 (−25%) |
| Random churn | 1 | No | 710 (−1%) | 715 | 717 (+0%) | 468 (−35%) | 254 (−64%) | 469 (−34%) | 253 (−65%) | 234 (−67%) |
| Random churn | 1 | Yes | 217 (−0%) | 217 | 217 (+0%) | 433 (+100%) | 197 (−9%) | 432 (+99%) | 196 (−10%) | 175 (−19%) |
| Add only | 100 | — | 28 (+0%) | 28 | 28 (+0%) | 27 (−4%) | 26 (−7%) | 26 (−7%) | 26 (−6%) | 26 (−7%) |
| Sliding window | 100 | No | 28 (+0%) | 28 | 28 (+0%) | 23 (−18%) | 27 (−2%) | 23 (−18%) | 23 (−18%) | 23 (−18%) |
| Sliding window | 100 | Yes | 20 (+0%) | 20 | 20 (+0%) | 23 (+15%) | 27 (+33%) | 23 (+15%) | 23 (+15%) | 23 (+15%) |
| Random churn | 100 | No | 52 (+2%) | 51 | 51 (+0%) | 46 (−9%) | 52 (+3%) | 45 (−12%) | 51 (+0%) | 51 (−1%) |
| Random churn | 100 | Yes | 42 (+2%) | 41 | 42 (+2%) | 41 (+0%) | 45 (+9%) | 44 (+7%) | 44 (+7%) | 45 (+10%) |

### Output positions assigned

The positions assigned at the end of a batch. Over a batch, one row per group creates 10 million groups, or cycles
through 2 million keys when groups return; 100 rows per group creates 100,000 groups, or cycles through 20,000 keys.

| Workload | Rows per group | Groups return | Main | Main + #8676 | `none` | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 10.00M (+0%) | 10.00M | 10.00M (+0%) | 10.00M (+0%) | 10.00M (+0%) | 10.00M (+0%) | 10.00M (+0%) | 10.00M (+0%) |
| Sliding window | 1 | No | 10.00M (+0%) | 10.00M | 10.00M (+0%) | 10.00M (+0%) | 2.96M (−70%) | 1.49M (−85%) | 1.49M (−85%) | 1.49M (−85%) |
| Sliding window | 1 | Yes | 2.00M (+0%) | 2.00M | 2.00M (+0%) | 10.00M (+400%) | 2.96M (+48%) | 1.49M (−25%) | 1.49M (−25%) | 1.49M (−25%) |
| Random churn | 1 | No | 10.00M (+0%) | 10.00M | 10.00M (+0%) | 10.00M (+0%) | 4.02M (−60%) | 8.59M (−14%) | 2.76M (−72%) | 2.57M (−74%) |
| Random churn | 1 | Yes | 2.00M (+0%) | 2.00M | 2.00M (+0%) | 8.71M (+335%) | 3.25M (+63%) | 8.65M (+333%) | 2.62M (+31%) | 2.40M (+20%) |
| Add only | 100 | — | 100.0k (+0%) | 100.0k | 100.0k (+0%) | 100.0k (+0%) | 100.0k (+0%) | 100.0k (+0%) | 100.0k (+0%) | 100.0k (+0%) |
| Sliding window | 100 | No | 100.0k (+0%) | 100.0k | 100.0k (+0%) | 100.0k (+0%) | 12.0k (−88%) | 16.9k (−83%) | 16.9k (−83%) | 16.9k (−83%) |
| Sliding window | 100 | Yes | 20.0k (+0%) | 20.0k | 20.0k (+0%) | 100.0k (+400%) | 12.0k (−40%) | 16.9k (−16%) | 16.9k (−16%) | 16.9k (−16%) |
| Random churn | 100 | No | 100.0k (+0%) | 100.0k | 100.0k (+0%) | 100.0k (+0%) | 69.4k (−31%) | 100.0k (+0%) | 86.3k (−14%) | 90.6k (−9%) |
| Random churn | 100 | Yes | 20.0k (+0%) | 20.0k | 20.0k (+0%) | 20.0k (+0%) | 20.0k (+0%) | 20.0k (+0%) | 20.0k (+0%) | 20.0k (+0%) |

### Longest cycle

The longest single cycle of a batch, in milliseconds, the worst over five batches. A single worst sample is noisy —
the same configuration varied by a factor of two between runs — so this table has no percentages; read it for the
modes whose longest cycles are consistently high.

| Workload | Rows per group | Groups return | Main | Main + #8676 | `none` | Blocks | Collapse, sweep | Bulk | Collapse 0.75, bulk | Collapse 0.5, bulk |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Add only | 1 | — | 16.4 | 19.3 | 26.8 | 25.8 | 11.4 | 13.5 | 14.9 | 14.0 |
| Sliding window | 1 | No | 25.0 | 46.4 | 33.1 | 8.7 | 6.8 | 10.3 | 10.0 | 10.0 |
| Sliding window | 1 | Yes | 4.8 | 5.9 | 8.8 | 6.8 | 6.2 | 6.1 | 6.1 | 6.7 |
| Random churn | 1 | No | 11.1 | 14.4 | 11.0 | 9.5 | 8.5 | 36.6 | 28.9 | 19.1 |
| Random churn | 1 | Yes | 12.5 | 12.1 | 10.9 | 15.3 | 8.0 | 42.4 | 25.3 | 20.7 |
| Add only | 100 | — | 0.4 | 0.4 | 0.5 | 0.5 | 0.5 | 0.5 | 1.0 | 0.4 |
| Sliding window | 100 | No | 2.7 | 0.6 | 0.5 | 0.6 | 0.5 | 1.5 | 0.8 | 0.4 |
| Sliding window | 100 | Yes | 0.6 | 0.5 | 0.6 | 1.4 | 0.5 | 0.4 | 0.5 | 0.4 |
| Random churn | 100 | No | 1.4 | 1.5 | 1.5 | 1.4 | 8.8 | 2.0 | 2.5 | 2.9 |
| Random churn | 100 | Yes | 1.9 | 1.1 | 1.4 | 1.1 | 2.5 | 1.7 | 2.3 | 1.3 |

### Findings

- #8676 makes random churn with one row per group about 10% faster: main takes 10% to 11% longer. Elsewhere main and
  main with #8676 are within the error of each other, and they retain the same heap.
- This branch without reclaiming (`none`) is 7% faster than main with #8676 for random churn when groups do not return,
  and 15% faster when they do, from its other improvements to the update cycle. Elsewhere it is within the error.
- When groups do not return, every mode that releases blocks is faster than main with #8676 for the sliding window, by
  29% to 40%, and retains about a quarter of its heap. For random churn, the modes that collapse retain a third of the
  heap, and releasing blocks alone two thirds. The bulk shift without collapsing gains little there — the same time and
  heap as releasing blocks alone, and 14% fewer positions — because random removals seldom empty a whole block for it
  to give back.
- The bulk shift keeps the positions assigned lowest: 1.49 million for the sliding window, against 2.96 million for the
  sweeping shift and 10 million without a shift. For random churn it needs the collapse to release blocks: collapsing
  at 0.5 with the bulk shift assigns 2.57 million positions, at 0.75 2.76 million, and the sweeping shift 4.02 million.
- When groups return, every reclaiming mode is 21% to 53% slower than main with #8676, since returning groups are new
  states rather than reused ones. Every mode retains less heap for the sliding window. For random churn the modes that
  collapse retain 9% to 19% less, while releasing blocks alone, with or without the bulk shift, retains twice as much,
  since few blocks empty while no state is reused.
- With 100 rows per group, the modes are within the error of each other for time.
- The bulk shift has the longest cycles for random churn, 19 to 42 ms against 8 to 15 ms without it, because it moves
  every live state after the first released block in one cycle.

The numbers come from one machine and one JVM fork per configuration, with five measured batches each; treat
differences within the error bounds as noise.

## Related documentation

- `StateReclaimMode` (`engine/table/src/main/java/io/deephaven/engine/table/impl/by/StateReclaimMode.java`)
- `OutputPositionBlockTracker` (`engine/table/src/main/java/io/deephaven/engine/table/impl/by/OutputPositionBlockTracker.java`)
- `ChunkedOperatorAggregationHelper` (`engine/table/src/main/java/io/deephaven/engine/table/impl/by/ChunkedOperatorAggregationHelper.java`)
- `IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBaseWithTombstones` (`engine/table/src/main/java/io/deephaven/engine/table/impl/by/IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBaseWithTombstones.java`)
- `AggregationIncrementalBenchmark` (`engine/benchmark/src/main/java/io/deephaven/benchmark/engine/AggregationIncrementalBenchmark.java`)

## Rejected Alternatives

These designs were implemented or measured and then rejected.

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
tables above, without the `bulkShift` parameter.

### Combining any two blocks that fit

An earlier credit mode, before `bulkShift` became a parameter of release mode, combined blocks in pairs rather than
collapsing runs of sparse blocks: two closed blocks, consecutive among the blocks not released, were combined whenever
their live states fit in one block, releasing the upper one, whatever their fractions free. It shifted in bulk as
`bulkShift` does.

Collapsing at 0.5 with the bulk shift does nearly as well, and uses one combining mechanism rather than two. Measured in
an earlier run, the pairwise combine assigned 2.44 million positions for random churn with one row per group and
retained 229 MB, against 2.57 million and 234 MB for collapsing at 0.5 with the bulk shift in the tables above; with
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
