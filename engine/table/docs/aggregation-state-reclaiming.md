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

`StateReclaimMode.releaseBlocks(collapseFreeFraction, blockShiftFraction)` frees the storage of each block once every
state in it has been removed. With the default parameters, no state ever moves:

- Memory tracks the groups that have rows, but only at block granularity. A block that holds even one long-lived state
  is never released, so random churn with some long-lived groups releases few blocks.
- Output positions are never given back, so the positions assigned grow with every group ever created.

Two optional moves reduce both limits. Each cycle may move no more live states than the cycle's input rows added,
modified, and removed.

- **Collapse** (`collapseFreeFraction` below 1): a closed block at least this fraction free is sparse. Runs of adjacent
  sparse blocks are collapsed: their live states move to the start of the run, and the blocks this empties are
  released. A run collapses only if that frees at least one block. Collapse frees memory but does not give output
  positions back.
- **Block shift** (`blockShiftFraction` zero or more): once the released blocks are at least this fraction of the
  positions assigned, the blocks after released ones shift down over them as whole blocks. The shift sweeps toward the
  end over several cycles and resumes where it stopped. Unspent budget of up to one block carries to the next cycle.
  When the sweep reaches the end, the released blocks are given back and the next output position moves down.

### Credit

`StateReclaimMode.credit()` releases blocks as they empty and moves states only when a move frees a whole block. Moves
are paid for by a credit that carries across cycles:

- **Earning credit:** each cycle earns the number of states it added and removed. Unspent credit carries to later
  cycles, up to the number of positions assigned, which is enough for any move.
- **Combining blocks:** two closed blocks that are consecutive among the blocks not released, and whose live states fit
  in one block, are combined. The states of both move, in order, to the start of the lower block, and the upper block
  is released. Released blocks may lie between the two. Pairs are combined from the first block on, each only if the
  credit covers the states of both blocks. A combined block may take in the next block too.
- **Shifting in bulk:** once the credit covers every live state after the first released block, those blocks shift
  down over the released blocks as whole blocks, all in one cycle, and every released block is given back at the end.
  The shift never stops part of the way.

The block shift moves whole blocks by reference in the array-backed sources, so its cost per state is the hash table's
update of the state's output position. On average the credit mode moves no more states per cycle than the cycle added
and removed. The bulk shift concentrates that work: every live state moves in one cycle, roughly once per cycle count
that it takes to earn the live state count.

The credit mode bounds the positions assigned for both front-concentrated and random removals. For a sliding window
with `L` live groups, the shift runs whenever the credit reaches `L`, when about `L / 2` positions have been added past
the live states, so the positions assigned stay near `1.5 L`.

## Parameters

The configuration properties set the mode that `StateReclaimMode.configured()` returns. It reads them as each
aggregation is created, so changing them affects only aggregations created afterward. Tests change them through the
matching public static fields of `ChunkedOperatorAggregationHelper`.

| Property | Field | Default | Meaning |
| --- | --- | --- | --- |
| `ChunkedOperatorAggregationHelper.reclaimStates` | `RECLAIM_STATES` | `true` | Whether states are reclaimed at all. `false` selects `none`. |
| `ChunkedOperatorAggregationHelper.collapseFreeFraction` | `COLLAPSE_FREE_FRACTION` | `1.0` | With released blocks, the fraction free at which a closed block is sparse and may be collapsed. 1 or more never collapses. |
| `ChunkedOperatorAggregationHelper.blockShiftFraction` | `BLOCK_SHIFT_FRACTION` | `-1.0` | With released blocks, the fraction of the positions assigned that released blocks must reach before blocks shift down. 0 shifts for any released block; negative never shifts. |
| `ChunkedOperatorAggregationHelper.creditReclaim` | `CREDIT_RECLAIM` | `false` | Whether to use the credit mode. It takes precedence over the collapse and block shift settings. |

The defaults select `releaseBlocks(1, -1)`: blocks are released as they empty and no state moves. A fraction must not
be `NaN`; `releaseBlocks` rejects one.

## Choosing a mode per call

Engine code that builds an aggregation can choose a mode for that aggregation alone:

- `ChunkedOperatorAggregationHelper.aggregation(control, factory, input, preserveEmpty, initialKeys, reclaimMode,
  groupByColumns)`
- `QueryTable.aggNoMemo(factory, preserveEmpty, initialGroups, groupByColumns, reclaimMode)`

A consumer that looks up a group's current row key and then reads previous values at that row needs a mode that never
moves states. The tree table's source row lookup uses `none` for this reason, and the table-backed data index uses
`none` as well.

## When a mode applies

A mode other than `none` applies only when all of the following hold. Otherwise the aggregation behaves as with `none`.

- The input table is refreshing.
- Every operator of the aggregation can reclaim states. Group-by, partition-by, formula, and rollup operators cannot,
  so aggregations that use them never reclaim.
- The aggregation does not preserve empty groups.
- The aggregation has no initial groups.

## Costs and limits

- **Returning groups cost more.** A group that empties and returns is a new state, and a new row at the end of the
  result, where `none` would reuse its state. Workloads whose groups keep returning run slower with reclaiming.
- **Output positions are ints.** A mode that never gives positions back — release blocks without the block shift —
  fails with `Aggregation output positions exhausted` once 2^31 - 1 states have been created over the life of the
  aggregation. At 10,000 new groups per second that is about 2.5 days.
- **The hash table does not shrink.** Tombstones keep their keys until a rehash drops them. Growth is driven by the live
  states: when tombstones alone cross the load factor, the table rehashes at the same size.
- **Key columns track previous values only when states can move.** The copied key columns start tracking previous
  values for collapse, block shift, and credit, but not for release blocks without moves.
- **The bulk shift of the credit mode has a latency cost.** The cycle that shifts moves every live state after the first
  released block.

## Benchmark results

`AggregationIncrementalBenchmark` in `engine/benchmark` measures batches of 900 update cycles of a keyed `sumBy` over a
table of 1,000,000 rows, one row per group, with 10,000 rows added and removed each cycle:

- **Add only:** rows are only added, each with a new group.
- **Sliding window:** each cycle removes the oldest rows, so groups empty in the order they were created.
- **Random churn:** each cycle removes rows chosen at random, so blocks thin out unevenly.

A group "returns" when its key comes back after its state has been removed; with returning groups, the keys cycle
through a space twice the size of the table.

The columns are main, which predates reclaiming, and this branch's modes. "Blocks" is `releaseBlocks(1, -1)`, the
default, and "Blocks with moves" is `releaseBlocks(0.75, 0)`, which collapses and shifts blocks.

### Time

Milliseconds per batch of 900 cycles; lower is better.

| Workload | Groups return | Main | `none` | Blocks | Blocks with moves | `credit` |
| --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 911 ± 130 | 862 ± 130 | 898 ± 87 | 876 ± 78 | 907 ± 58 |
| Sliding window | No | 1382 ± 68 | 1354 ± 68 | 800 ± 39 | 861 ± 71 | 898 ± 66 |
| Sliding window | Yes | 601 ± 23 | 617 ± 55 | 693 ± 43 | 782 ± 60 | 772 ± 45 |
| Random churn | No | 3137 ± 170 | 2512 ± 92 | 2392 ± 92 | 2389 ± 81 | 2440 ± 188 |
| Random churn | Yes | 2044 ± 57 | 1608 ± 83 | 2355 ± 48 | 2344 ± 48 | 2201 ± 45 |

### Memory

The heap retained at the end of a batch, in megabytes, after a garbage collection. It includes the source table and
the benchmark's own data, so compare the modes with each other rather than reading the values as the aggregation's
size.

| Workload | Groups return | Main | `none` | Blocks | Blocks with moves | `credit` |
| --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 673 | 674 | 571 | 568 | 569 |
| Sliding window | No | 670 | 682 | 175 | 180 | 191 |
| Sliding window | Yes | 178 | 179 | 119 | 124 | 135 |
| Random churn | No | 709 | 720 | 468 | 254 | 229 |
| Random churn | Yes | 216 | 218 | 432 | 197 | 170 |

### Output positions assigned

The positions assigned at the end of a batch, in millions. Every workload creates 10 million groups over the batch,
or cycles through 2 million keys when groups return.

| Workload | Groups return | Main | `none` | Blocks | Blocks with moves | `credit` |
| --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 10.00 | 10.00 | 10.00 | 10.00 | 10.00 |
| Sliding window | No | 10.00 | 10.00 | 10.00 | 2.96 | 1.49 |
| Sliding window | Yes | 2.00 | 2.00 | 10.00 | 2.96 | 1.49 |
| Random churn | No | 10.00 | 10.00 | 10.00 | 4.02 | 2.44 |
| Random churn | Yes | 2.00 | 2.00 | 8.71 | 3.25 | 2.35 |

### Longest cycle

The longest single cycle of a batch, in milliseconds, the worst over five batches.

| Workload | Groups return | Main | `none` | Blocks | Blocks with moves | `credit` |
| --- | --- | --- | --- | --- | --- | --- |
| Add only | — | 19.8 | 14.0 | 14.8 | 13.7 | 13.7 |
| Sliding window | No | 25.8 | 20.1 | 6.2 | 10.0 | 10.4 |
| Sliding window | Yes | 5.6 | 6.2 | 5.6 | 5.3 | 6.0 |
| Random churn | No | 13.0 | 13.4 | 7.0 | 7.0 | 18.3 |
| Random churn | Yes | 11.8 | 10.2 | 12.6 | 8.8 | 21.5 |

### Findings

- When groups do not return, every mode that releases blocks runs faster than main and retains less heap: about a
  quarter of main's for the sliding window, and for random churn a third less with blocks alone, or about two thirds
  less with blocks with moves or `credit`. The credit mode also keeps the positions assigned closest to the live
  groups: 1.5 times them for the sliding window and 2.4 times for random churn, against 10 times without reclaiming.
- When groups return, `none` is the fastest, since returning groups reuse their states. Blocks with moves and `credit`
  still use less heap than main, and `credit` assigns the fewest positions of the reclaiming modes.
- Adding rows only costs nothing with any mode.
- The credit mode's longest cycle is the highest for random churn, because its bulk shift moves every live state after
  the first released block in one cycle.
- With 100 rows per group, earlier runs placed every mode within the measurement noise of each other.

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
retained 2.5 to 3.4 times the heap of blocks with moves or `credit`.

### Combining the cheapest pairs first

A variant of the credit mode combined the pairs of blocks with the fewest live states first, wherever they were, since
every combination frees one block and the cheapest free the most blocks for the credit. It assigned the same number of
positions as combining from the first block on, because at the benchmark's churn every pair that fits is combined
either way. Over 30 measured batches of random churn its longest cycle had a median of 18.2 ms against 16.9 ms, and its
batches took about 1% longer, from sorting the candidate pairs each cycle.

### Assigning new states to released positions

Assigning new states to released positions in the middle of the result would bound the positions assigned without
moving any state. It would give new groups lower row keys than older ones, so the result would no longer list groups
in the order they were first seen, even among groups that never empty. New states therefore always go after every
existing state, and only moves that keep the states in order give positions back.
