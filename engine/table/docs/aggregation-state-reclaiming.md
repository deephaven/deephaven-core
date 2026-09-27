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

### Compact

`StateReclaimMode.compact()` shifts the states after removed ones down into their positions and gives the positions
past the new end back, releasing their storage after the cycle. It does not release blocks in the middle of the result
as they empty.

Compaction is budgeted by the cycle's input rows, and it starts from the first free position every cycle. When
removals are concentrated at the front — a sliding window, for example — keeping the result dense means moving every
live state each cycle. Once the live states exceed twice the cycle's changes, the budget cannot keep up: compaction
moves the same front states every cycle, and the positions assigned grow as fast as without reclaiming.

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
| `ChunkedOperatorAggregationHelper.releaseBlocks` | `RELEASE_BLOCKS` | `true` | Whether to release blocks (`true`) or compact (`false`). |
| `ChunkedOperatorAggregationHelper.collapseFreeFraction` | `COLLAPSE_FREE_FRACTION` | `1.0` | With released blocks, the fraction free at which a closed block is sparse and may be collapsed. 1 or more never collapses. |
| `ChunkedOperatorAggregationHelper.blockShiftFraction` | `BLOCK_SHIFT_FRACTION` | `-1.0` | With released blocks, the fraction of the positions assigned that released blocks must reach before blocks shift down. 0 shifts for any released block; negative never shifts. |
| `ChunkedOperatorAggregationHelper.creditReclaim` | `CREDIT_RECLAIM` | `false` | Whether to use the credit mode. It takes precedence over the release, collapse, and block shift settings. |

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
  values for compaction, collapse, block shift, and credit, but not for release blocks without moves.
- **The bulk shift of the credit mode has a latency cost.** The cycle that shifts moves every live state after the first
  released block.

## Benchmark results

`AggregationIncrementalBenchmark` in `engine/benchmark` measures batches of 900 update cycles of a keyed `sumBy` over a
table of 1,000,000 rows, with 10,000 rows added and removed each cycle. The table compares the credit mode and `none`
against main, which predates reclaiming, on one row per group. Times are milliseconds per batch; lower is better.

| Workload | Groups return | Main | `none` | `credit` | Positions assigned, main / `credit` |
| --- | --- | --- | --- | --- | --- |
| Add only | — | 911 ± 130 | 950 ± 85 | 868 ± 128 | 10.0M / 10.0M |
| Sliding window | No | 1382 ± 68 | 1338 ± 65 | 913 ± 42 | 10.0M / 1.49M |
| Sliding window | Yes | 601 ± 23 | 643 ± 63 | 785 ± 60 | 2.0M / 1.49M |
| Random churn | No | 3137 ± 170 | 2558 ± 73 | 2268 ± 28 | 10.0M / 2.44M |
| Random churn | Yes | 2044 ± 57 | 1526 ± 125 | 2227 ± 188 | 2.0M / 2.35M |

- When groups do not return, the credit mode is the fastest and keeps the positions assigned bounded. Its retained
  heap is about a third of main's: 190 MB against 670 MB for the sliding window, and 228 MB against 709 MB for random
  churn.
- When groups return, `none` is the fastest, because returning groups reuse their states.
- With 100 rows per group, all three are within the measurement noise of each other.
- The credit mode's longest cycle is about 17 to 19 ms for random churn, against 9 to 13 ms without it, because of the
  bulk shift.

The numbers come from one machine and one JVM fork per configuration; treat differences within the error bounds as
noise.

## Related documentation

- `StateReclaimMode` (`engine/table/src/main/java/io/deephaven/engine/table/impl/by/StateReclaimMode.java`)
- `OutputPositionBlockTracker` (`engine/table/src/main/java/io/deephaven/engine/table/impl/by/OutputPositionBlockTracker.java`)
- `ChunkedOperatorAggregationHelper` (`engine/table/src/main/java/io/deephaven/engine/table/impl/by/ChunkedOperatorAggregationHelper.java`)
- `IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBaseWithTombstones` (`engine/table/src/main/java/io/deephaven/engine/table/impl/by/IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBaseWithTombstones.java`)
- `AggregationIncrementalBenchmark` (`engine/benchmark/src/main/java/io/deephaven/benchmark/engine/AggregationIncrementalBenchmark.java`)
