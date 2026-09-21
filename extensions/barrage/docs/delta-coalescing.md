# Barrage delta coalescing

How `BarrageMessageProducer` keeps its queue of pending deltas small, how a run of deltas is
coalesced into one, and the measurements that chose each part of the design.

Code: `server/src/main/java/io/deephaven/server/barrage/BarrageMessageProducer.java`,
`BarrageMessageDelta.java`, and
`extensions/barrage/src/main/java/io/deephaven/extensions/barrage/chunk/*BarrageCopyKernel.java`.

## The problem

A producer records one delta per update-graph cycle and drains the queue when the subscribers'
update interval elapses. A subscriber served less often than the table ticks therefore holds every
cycle's chunk data at once: memory grows linearly in cycles per flush, while the message finally
sent is the size of the *coalesced* change, not the sum of the changes.

On the original reproduction — 100 cycles each re-modifying the same 10k rows of eight `long`
columns, one flush at the end — that is **100 MiB** of pooled chunk storage (62.5 MiB of data, the
rest pool rounding) to send **626 KiB**. The fix is to coalesce the queue *before* the flush, off
the update-graph thread.

## Design

### Recording and triggering

Each cycle appends a `BarrageMessageDelta` under the producer's monitor. After appending,
`shouldCompact` decides whether to schedule compaction.

Compaction costs processor time and buys memory, so a producer pays for one only when there is
memory to reclaim. The policy compares what the queue holds against what it would hold coalesced:

- `A` is `pendingDeltaBytes`, the chunk storage the pending deltas occupy.
- `N` is `coalescedChunkBytes()`, an estimate of what they would occupy coalesced.
- Compact when `A − N ≥ max(COMPACTION_FLOOR_BYTES, COMPACTION_MIN_FREED_FRACTION × A)`.

Read the second term as a fraction of memory released: at the default of 0.5, a compaction has to
free half of what the queue holds to be worth running. Both conditions must hold, and each answers a
different question. The fraction asks whether compacting is worthwhile at all; the floor, 4 MiB by
default, asks whether the saving is large enough to be worth a job.

The floor is what stops a stream of tiny deltas from compacting on every cycle. A delta that records
a single row still occupies the pool's smallest chunk in each subscribed column, so from the second
such delta on almost all of the queue is chunk rounding rather than data and the freed fraction sits
above 90% forever. Compacting there copies the whole queue to release one chunk per column, while
the job's fixed costs — scheduling, the run lock, the row-set passes, the splice — are the same as
for a compaction that frees gigabytes. With the floor in place, a stream of single-row appends
compacts about every 7,000 cycles at 20 bytes per row, and about every 450 at 300 bytes per row.

**Both bounds hold throughout the interval, not only just after a compaction.** Because each
compaction leaves behind exactly the storage the estimate predicted, the next cannot fire until a
further fraction `f` of the queue has been superseded. Writing `raw` for the bytes recorded between
two compactions, the trigger gives `N_i ≤ (1 − f)(N_{i−1} + raw_i)`, and summing:

```
Σ N_i ≤ ((1 − f) / f) × Σ raw_i
```

Total copying is therefore linear in the data recorded — at `f = 0.5`, compaction copies at most one
byte for every byte recorded, so a byte is written twice over its lifetime, amortized across the
stream rather than bounded row by row — and the queue holds at most about `1 / (1 − f)` times its
coalesced footprint, plus the delta that triggered the compaction and the transient of the copy
itself. Raising `f` to 0.9 cuts the copying to about a ninth but lets the queue reach ten times that
footprint, which is the memory the feature exists to reclaim; raise it only where processor time is
scarcer than heap.

#### Estimating the coalesced size

`N` needs no scan of the queue. The producer keeps two row sets, `netAddedRows` and
`netModifiedRows`, and a `netModifiedColumns` bitset, updated in `updateNetRowSets` as each delta is
appended, as coalescing would: rows removed upstream drop out, the cycle's shifts are applied, the
cycle's recorded rows are inserted, and the two sets are kept disjoint because a row the queue adds
is sent as an add. The added side is exactly what `RunSummary` computes when a compaction runs; the
modified side approximates what `ColumnMapping` does, in the ways below. `N` is then their sizes
times the width of the columns concerned.

Three properties of that estimate matter:

- **It counts rows where `pendingDeltaBytes` counts capacity.** A delta pays for the last chunk of
  every column at whatever the pool rounded it up to, and the estimate does not, so a compaction
  leaves behind somewhat more than `N` predicts — at most one chunk per column per side. Since `N`
  is subtracted from `A`, that credits a compaction with rounding it cannot free. The floor is what
  keeps it harmless: rounding up to a power of two never accounts for half a chunk, so at `f = 0.5`
  the phantom saving cannot meet the fraction by itself, and a much smaller `f` wants a floor that
  covers `DELTA_CHUNK_SIZE` rows of the subscription's width.
- **It charges one row set per side, not one per column.** Exact figures would need a row set per
  modified column, which is a row-set pass per column per cycle on the update-graph thread for a
  wide table that ticks every column. The union instead overstates `N` when columns tick on their
  own rows, which delays a compaction and can never provoke one that frees less than the policy
  asks for.
- **It subtracts the recorded adds from the modified side, where compaction subtracts the coalesced
  update's adds.** A row scoped into a viewport is recorded as an add without being an upstream add,
  and a compacted delta keeps such a row on both sides, because one subscriber needs it as an add
  while another needs it as a modification. The estimate drops it from the modified side, so it
  understates `N` by those rows — the one way it errs toward compacting early rather than late. It
  is bounded by the rows both scoped in and modified within one window, and a viewport change forces
  a snapshot, which splits the run.

Compaction leaves these sets alone. The compacted delta describes the same rows they name, so they
describe the new queue as faithfully as the old one, including whatever the update-graph thread
appended while the coalescing ran. Only a flush clears them.

**Blink tables are never compacted.** Their pending deltas are coalesced by concatenation rather
than by the algorithm compaction runs, because every row that blinked during the interval has to
reach the subscriber. Nothing is ever superseded, so there is nothing to reclaim — and the estimate
above does not describe such a queue at all, since each cycle removes the rows the last one added,
which would read as waste.

### Scheduling

`CompactionJob` runs on a scheduler thread and takes `updatePropagationJob.runLock` with `tryLock`.
Failing to take it means propagation is already flushing, which makes the compaction moot, so the
job gives up rather than hold a scheduler thread; the next enqueue re-evaluates. The update-graph
thread is never blocked — it keeps appending under the monitor, which the job takes only to copy out
the run and later to splice the result in. Coalescing is therefore **single threaded**, across both
columns and rows.

`compactLeadingRun` takes the leading run of one `subscriptionGeneration`, but that is a guard by
construction rather than a restriction it works around. The propagation job splits the queue by
*step*, not by generation — a delta belongs to the pre-snapshot half when its `lastStep` is at or
before the snapshot's first sequence — and it asserts that no delta straddles that step. What a
compacted delta must never span, then, is a snapshot step.

It never can. Both promotions happen inside a propagation run: the snapshot one in
`snapshotCompletedConsistently`, the removal-only one in `updateSubscriptionsSnapshotAndPropagate`.
That run holds the lock the compaction job could not take, and it ends by flushing the whole queue,
so outside such a run every pending delta shares one generation and the loop takes all of them. The
generation test costs one comparison per delta and would stop a future change that let the two run
together, rather than letting the step assertion fire during a flush.

Coalescing across a generation boundary is not in itself unsound, and propagation does it: when
subscriptions are only removed, the producer promotes the narrower column set at once and
`RunSummary` intersects the columns each delta recorded, which loses nothing the remaining
subscribers still want. Compaction simply never meets such a boundary.

A failure anywhere in the job fails every subscription, because the pending queue is the
subscribers' only record of what changed.

### Coalescing a run

`BarrageMessageDelta.coalesce` walks the run latest-first so each surviving row takes its value from
the last delta that recorded it, and produces one synthetic delta. Propagation uses the same entry
point, so there is one algorithm and one set of invariants.

The per-column work is shared. `ColumnMappingCache` keys a `ColumnMapping` on `MappingKey.forColumn`
— which deltas modified the column, plus the identity of their recorded modification row sets — so
columns that tick together compute their mapping once. Adds are common to every column, so
`addedRuns` is shared whenever the key matches.

### Runs

A `ColumnMapping` holds two `BarrageCopyKernel.Runs`, one for the added rows and one for the
modified. A run is a stretch contiguous in both the output and its source chunk, recorded as the
first output position, the encoded origin of its first row, and the length. Row sets are
range-compressed and updates arrive in ranges, so building this costs proportionally to ranges
rather than rows, and a run usually covers many of them.

No column data lives in `Runs`, only the positions that say where to find it. The caller appends in
output order and the kernel reads; nothing consumes or rewrites them, which is what lets columns
sharing a `MappingKey` share one.

### The copy kernel

`BarrageCopyKernel` fills one column's output chunks from the runs. Each run is split wherever it
crosses a chunk boundary in either the output or its origin, and every resulting stretch moves with
one `copyFromTypedChunk` call. That is the only copy path.

`copy` takes the runs, the output chunks and the column's per-delta chunks as the generic
`WritableChunk` arrays the producer holds, and casts them to its own chunk type once at the top of
the call. Casting up front is what keeps the copy call inside the loop monomorphic; there is nothing
to keep between calls, so there is no context object and nothing to close.

The kernel is replicated per chunk type from `CharBarrageCopyKernel`, the Object variant included
(`./gradlew replicateBarrageUtils`). `charToObject` leaves one thing for
`ReplicateBarrageUtils.fixupObjectBarrageCopyKernel` to correct: a `WritableObjectChunk` takes two
type parameters where the primitive chunks take one.

Every chunk a delta records holds `DELTA_CHUNK_SIZE` rows except the last of a column, which is what
lets an encoded position locate its chunk. `DELTA_CHUNK_SIZE` is a power of two — the producer rounds
a configured size up to one — so the kernel finds a position's chunk with a shift and its offset with
a mask rather than with a division per row, and asserts that.

## Measurements

JMH `AverageTime`, ms/op, lower is better; `±` is the 99.9% confidence interval over all iterations
of all forks.

**Hardware**: Apple M1 Max, 10 cores, 64 GiB, macOS 26.6.2 (arm64). **JVM**: Zulu 21.0.1
(`21.0.1+12-LTS`), G1, `-Xmx8g`. JMH 1.37. **Settings**: 3 forks, 3×2s warmup, 10×2s measurement,
`-gc true`. **Benchmark**: `extensions/barrage/benchmark/.../BarrageCopyKernelBenchmark.java`, two
deltas, runs at random origins, at 1M rows (2^20 in 64Ki-row chunks) and 98,304 rows (24 chunks of
4096).

Three things are required to get a usable number out of this benchmark:

- **Three forks, not one.** JMH's interval describes the spread *within* a fork; two forks of
  identical code can each be tight and still disagree, and on short-run cells the sign of a small
  difference has flipped between them.
- **`-gc true`.** Both arms allocate megabytes per operation. Without a forced collection between
  iterations, `Double` at average run 2 came back `31.605 ± 45.164` — unusable.
- **A quiet machine.** Check `uptime` first; a browser and IDE language servers can put this machine
  at load 200+, and a measurement taken at load 218 gave a conclusion that a repeat at load 3
  reversed.

Both arms cast their chunk arrays to the typed chunk inside the timed method, as each does once per
column in production. Hoisting that into trial setup for one arm only charges the casts to the other
and tilts the comparison.

### Copying by runs against gathering per row

Coalescing originally expanded the surviving rows into a mapping holding one encoded origin per
output row and gathered cell by cell from it, ignoring run structure. That is the `mappingOnly` arm,
its expansion timed with its gather because that algorithm cannot copy without one; `rangeAware` is
`BarrageCopyKernel.copy`. The tables below therefore measure what this design replaces, not a
hypothetical alternative.

Speed-up of the run copy, gather ÷ runs, so above 1 favours the run copy:

| type / rows | run 1 | run 2 | run 3 | run 5 | run 10 | run 50 |
| --- | --- | --- | --- | --- | --- | --- |
| `int`, 98k | 0.74x | 1.06x | 1.42x | 2.09x | 3.19x | 11.2x |
| `int`, 1M | 0.60x | 0.99x | 1.33x | 1.83x | 2.58x | 7.2x |
| `double`, 98k | 0.73x | 1.09x | 1.45x | 2.15x | 3.15x | 7.0x |
| `double`, 1M | 0.57x | 0.98x | 1.36x | 1.95x | 2.76x | 5.0x |
| `String`, 98k | 2.7x | 1.3x | 4.8x | 3.0x | 8.0x | 65x |
| `String`, 1M | 2.4x | 5.1x | 6.3x | 8.8x | 14x | 77x |

At 1M rows, gather → runs:

| avg run | `int` | `double` | `String` |
| --- | --- | --- | --- |
| 1 | 10.68 → 17.86 | 13.10 → 23.11 | 58.22 → **24.19** |
| 2 | 9.34 → 9.39 | 11.88 → 12.09 | 72.01 → **14.13** |
| 3 | 8.73 → **6.56** | 10.91 → **8.00** | 67.46 → **10.64** |
| 5 | 7.61 → **4.15** | 9.33 → **4.78** | 71.43 → **8.10** |
| 10 | 6.41 → **2.48** | 7.32 → **2.66** | 72.56 → **5.09** |
| 50 | 4.61 → **0.64** | 4.83 → **0.97** | 72.80 → **0.94** |

At 98,304 rows:

| avg run | `int` | `double` | `String` |
| --- | --- | --- | --- |
| 1 | 0.737 → 0.990 | 0.755 → 1.037 | 3.529 → **1.302** |
| 2 | 0.551 → 0.519 | 0.580 → 0.530 | 0.988 → **0.747** |
| 3 | 0.497 → **0.351** | 0.515 → **0.355** | 2.659 → **0.558** |
| 5 | 0.447 → **0.214** | 0.465 → **0.216** | 1.319 → **0.443** |
| 10 | 0.402 → **0.126** | 0.406 → **0.129** | 2.809 → **0.350** |
| 50 | 0.359 → **0.032** | 0.358 → **0.051** | 3.552 → **0.055** |

The gather's cost is per row and barely moves with run structure — at 1M the `String` column sits
between 58 and 73 ms at every run length — while the run copy falls with run length, because a
longer run is the same number of `copyFromTypedChunk` calls moving more rows each.

Note that a stretch is a call, not necessarily a bulk copy: `copyFromTypedChunk` uses
`System.arraycopy` only from `Chunk.SYSTEM_ARRAYCOPY_THRESHOLD` (16) elements up and assigns element
by element below it. So the figures at average runs of 1 to 10 compare two per-element loops, and
what the run copy saves there is the addressing the gather repeats for every row, not the copy
itself.

**Reference columns improve everywhere**, by 1.3–2.7x at 98k and 2.4–5.1x at 1M even where runs are
shortest.

**Primitives improve from an average run of three upward**, are level at two, and lose at one:
1.35x at 98k and 1.7–1.8x at 1M. A one-row run costs the copy path a call and the arithmetic to
split a stretch, to move a single element, which a flat gather loop does not pay. That is the
accepted cost of a single copy path; §"Open questions" records what reopening it would take.

**Read the `String` gather figures as orders of magnitude, not measurements.** That arm is bimodal
per fork: one fork in three runs several times slower than the others, at every run length, which is
why its rows are not monotonic. Two full runs of identical code put the 98k, runs-of-50 cell at
0.887 ms and at 3.552 ms. The run copy has no such mode — across the same two runs every one of its
cells reproduced within 14%, and every primitive cell within 6% — so the primitive figures and all
six run-copy columns can be read as measured, and only the `String` gather column carries that
uncertainty.

### Why the kernel is replicated per type

A single generic implementation sees every chunk class, goes megamorphic and stops the typed copy
inlining: measured roughly a third slower at short run lengths, tapering to under 10% at very long
ones, where one bulk copy amortizes the dispatch. Typing only the destination array and leaving the
sources generic ties at short runs and loses 5–8% at long ones. The eight kernels are generated from
one template, so the duplication costs one file of maintenance.

## Decisions recorded

| Decision | Why |
| --- | --- |
| Trigger on memory freed, measured against the whole queue | Compaction costs processor time and buys memory, so it should run only when there is memory to reclaim; measuring against the queue rather than the last compaction's output makes the copying bound hold throughout the interval |
| An absolute floor as well as a fraction | A one-row delta fills a minimum pool chunk per column, so a tiny-delta stream looks over 90% wasted forever; the floor makes it wait until a job is worth running |
| One estimated row set per side, not per column | Exact figures would cost a row-set pass per column per cycle on the update-graph thread; the union only ever delays a compaction |
| The estimate counts rows, not chunk capacity | Keeps it to two multiplications; it understates what a compaction leaves by at most one chunk per column per side, which the floor covers |
| `tryLock`, never block the scheduler thread | A held lock means a flush is imminent, which makes compaction moot |
| Delta chunks stay pooled; last chunk asks for the exact remainder | Pooling dominates; the rounding on one chunk per column is acceptable |
| Blink tables are never compacted | Their deltas are coalesced by concatenation, so nothing is ever superseded |
| Uniform chunk layout (no ragged chunks) | An encoded position locates its chunk by shifting, which a ragged layout breaks |
| `deltaChunkSize` rounded up to a power of two | Lets the kernel turn a position into a chunk index and an offset with a shift and a mask, rather than a division per row |
| One copy path, by runs | Wins from an average run of three up and everywhere for references; the loss at a run per row buys the removal of a second path, its threshold and its mapping |
| Kernel replicated per chunk type, casting inside `copy` | Keeps the copy call monomorphic so the typed copy inlines, with nothing to keep between calls |

## Open questions

- Primitive columns at an average run of one pay 1.3–1.8x what a per-row gather would. Real updates
  arrive as ranges, so a coalesce whose surviving rows are that scattered means a table modified
  in a genuinely scattered pattern; whether that happens often enough to matter needs run-length
  distributions from real workloads. If it does, the gather can return for primitive columns alone,
  which is where its whole margin lies.
- Sharing a `ColumnMapping` saves every column after the first in a group the pass that builds its
  runs. The copy kernel is indifferent to sharing, so the saving is in `BarrageMessageDelta`'s
  mapping pass and is not covered by the copy-kernel benchmark.
- `Runs` live until the `ColumnMappingCache` closes, so N distinct keys hold N of them at once. They
  cost three longs per run against the output's one row, so this is cheap where runs are long and
  closest to the output's own size where they are short.
- Coalescing is serial across columns. Columns are nearly embarrassingly parallel — separate
  destinations, read-only shared `Runs` — but `ColumnMappingCache.getOrCompute` computes lazily and
  would need pre-computation or synchronization first.
- The trigger's estimate charges every modified column for the union of the modified rows, so a
  table whose columns tick on their own rows compacts later than its real waste warrants — by a
  factor of roughly the number of distinct modification patterns. Whether that delay costs enough
  memory to be worth per-column row sets on the update-graph thread needs a workload that shows it.

## Re-running

```bash
./gradlew :extensions-barrage-benchmark:jmhRunBarrageCopyKernelAlgorithms      # 1M rows
./gradlew :extensions-barrage-benchmark:jmhRunBarrageCopyKernelAlgorithms100k  # 98,304 rows
```

Size is overridable without editing code: `-Dbench.totalRows=`, `-Dbench.deltaChunkSize=`. Each task
runs three forks over six run lengths and three column types, about 35 minutes. Check `uptime`
before trusting anything, and never run Gradle against `:extensions-barrage-benchmark` while a JMH
run of it is live — it rewrites the fork's classpath.
