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
`shouldCompact` decides whether to schedule compaction:

- **Byte trigger** — bytes recorded since the last compaction exceed `max(COMPACTION_FLOOR_BYTES,
  COMPACTION_GROWTH_FACTOR × compacted head size)`. Defaults: floor 4 MiB, factor 1.0. The floor
  keeps producers whose subscribers are served often enough from ever paying; the geometric term
  bounds total copying to linear in data recorded, and the queue to about `(1 + factor)` times the
  compacted footprint.
- **Count trigger** — `COMPACTION_MAX_PENDING_DELTAS` (default 32) deltas since the last compaction,
  whatever their size, bounding the per-delta overhead (row sets, update descriptions) that the byte
  policy cannot see.

A run in which no delta supersedes anything is declined rather than compacted, since copying it
would move every row and drop none. The producer tracks that as `pendingNonAddOnlyDeltas`, so a
queue of nothing but add-only deltas never schedules a job, and a run found to be add-only once the
job holds the lock is declined there. Blink tables are never compacted, for the same reason.

### Scheduling

`CompactionJob` runs on a scheduler thread and takes `updatePropagationJob.runLock` with `tryLock`.
Failing to take it means propagation is already flushing, which makes the compaction moot, so the
job gives up rather than hold a scheduler thread; the next enqueue re-evaluates. The update-graph
thread is never blocked — it keeps appending under the monitor, which the job takes only to copy out
the run and later to splice the result in. Coalescing is therefore **single threaded**, across both
columns and rows.

Compaction is **generation-scoped**: `compactLeadingRun` replaces only the leading run of
same-generation deltas. A generation is bumped by `promoteSnapshotToActive`, and deltas either side
of one describe different subscription bases, so coalescing across that boundary would break the
pre/post-snapshot split a late joiner depends on. A failure anywhere in the job fails every
subscription, because the pending queue is the subscribers' only record of what changed.

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

`Runs` is data only: the caller appends in output order, the kernel reads. Nothing consumes or
rewrites it, which is what lets columns sharing a `MappingKey` share one.

### The copy kernel

`BarrageCopyKernel` fills one column's output chunks from the runs. Each run is split wherever it
crosses a chunk boundary in either the output or its origin, and every resulting stretch moves with
one typed array copy. That is the only copy path.

The kernel is replicated per chunk type from `CharBarrageCopyKernel`, the Object variant included
(`./gradlew replicateBarrageUtils`). `charToObject` leaves two things for
`ReplicateBarrageUtils.fixupObjectBarrageCopyKernel` to correct: a `WritableObjectChunk` takes two
type parameters where the primitive chunks take one, and the `charContext` local comes out
capitalized along with the type name.

Every chunk a delta records holds `DELTA_CHUNK_SIZE` rows except the last of a column, which is what
lets an encoded position locate its chunk by division. The kernel needs only that uniformity. The
producer additionally rounds a configured size up to a power of two, because chunks come from a pool
that serves powers of two: an unrounded size would leave the tail of every pooled chunk unused.

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

### Copying by runs against gathering per row

Coalescing originally expanded the surviving rows into a mapping holding one encoded origin per
output row and gathered cell by cell from it, ignoring run structure. That is the `mappingOnly` arm,
its expansion timed with its gather because that algorithm cannot copy without one; `rangeAware` is
`BarrageCopyKernel.copy`. The tables below therefore measure what this design replaces, not a
hypothetical alternative.

Speed-up of the run copy, gather ÷ runs, so above 1 favours the run copy:

| type / rows | run 1 | run 2 | run 3 | run 5 | run 10 | run 50 |
| --- | --- | --- | --- | --- | --- | --- |
| `int`, 98k | 0.75x | 1.10x | 1.39x | 2.03x | 3.04x | 11.4x |
| `int`, 1M | 0.60x | 1.01x | 1.35x | 1.90x | 2.51x | 7.3x |
| `double`, 98k | 0.73x | 1.13x | 1.42x | 2.10x | 3.08x | 7.0x |
| `double`, 1M | 0.55x | 0.96x | 1.26x | 2.05x | 2.83x | 5.1x |
| `String`, 98k | 1.80x | 1.71x | 5.2x | 4.8x | 4.4x | 15.6x |
| `String`, 1M | 2.76x | 4.17x | 6.1x | 7.6x | 12.7x | 65x |

At 1M rows, gather → runs:

| avg run | `int` | `double` | `String` |
| --- | --- | --- | --- |
| 1 | 10.62 → 17.77 | 13.06 → 23.84 | 67.67 → **24.52** |
| 2 | 9.56 → 9.43 | 12.19 → 12.69 | 62.87 → **15.08** |
| 3 | 8.90 → **6.59** | 11.12 → **8.80** | 65.00 → **10.69** |
| 5 | 7.92 → **4.17** | 9.74 → **4.76** | 61.32 → **8.07** |
| 10 | 6.46 → **2.57** | 7.62 → **2.70** | 65.35 → **5.15** |
| 50 | 4.64 → **0.64** | 5.02 → **0.98** | 60.22 → **0.93** |

At 98,304 rows:

| avg run | `int` | `double` | `String` |
| --- | --- | --- | --- |
| 1 | 0.724 → 0.961 | 0.731 → 0.996 | 2.395 → **1.328** |
| 2 | 0.553 → 0.504 | 0.583 → 0.518 | 1.402 → **0.821** |
| 3 | 0.498 → **0.359** | 0.520 → **0.365** | 3.407 → **0.651** |
| 5 | 0.451 → **0.222** | 0.469 → **0.223** | 2.298 → **0.476** |
| 10 | 0.407 → **0.134** | 0.413 → **0.134** | 1.598 → **0.361** |
| 50 | 0.364 → **0.032** | 0.364 → **0.052** | 0.887 → **0.057** |

The gather's cost is per row and barely moves with run structure — at 1M the `String` column sits
between 60 and 68 ms at every run length — while the run copy falls with run length, because a run
becomes one `System.arraycopy`.

**Reference columns improve everywhere**, by 1.7–1.8x at 98k and 2.8–4.2x at 1M even where runs are
shortest, because a reference gather pays a write barrier per row that a bulk copy pays once per
stretch.

**Primitives improve from an average run of three upward**, are level at two, and lose at one: 1.3x
at 98k and 1.7–1.8x at 1M. A one-row run costs the copy path four divisions and a call to move a
single element, which a flat gather loop does not pay. That is the accepted cost of a single copy
path; §"Open questions" records what reopening it would take.

Two cautions on the numbers above. The `mappingOnly` `String` cells at 98k are bimodal per fork —
one fork in three runs 2.5–4x slower than the others, which is why that row is not monotonic in run
length — so read those ratios as approximate. Every `rangeAware` cell is stable, its three forks
agreeing within 10% and usually within 3%.

### Why the kernel is replicated per type

A single generic implementation sees every chunk class, goes megamorphic and stops the typed copy
inlining: measured roughly a third slower at short run lengths, tapering to under 10% at very long
ones, where one `arraycopy` amortizes the dispatch. Typing only the destination array and dropping
the context ties at short runs and loses 5–8% at long ones. The eight kernels are generated from one
template, so the duplication costs one file of maintenance.

## Decisions recorded

| Decision | Why |
| --- | --- |
| Geometric byte trigger plus a count cap | Total copying linear in data recorded; the count cap bounds per-delta overhead the byte policy cannot see |
| `tryLock`, never block the scheduler thread | A held lock means a flush is imminent, which makes compaction moot |
| Delta chunks stay pooled; last chunk asks for the exact remainder | Pooling dominates; the rounding on one chunk per column is acceptable |
| Add-only runs are declined, not coalesced | Nothing is superseded, so copying moves every row and drops none |
| Uniform chunk layout (no ragged chunks) | An encoded position locates its chunk by division, which a ragged layout breaks |
| `deltaChunkSize` rounded up to a power of two | Chunks come from a pool that serves powers of two; an unrounded size wastes every chunk's tail |
| One copy path, by runs | Wins from an average run of three up and everywhere for references; the loss at a run per row buys the removal of a second path, its threshold and its mapping |
| Kernel replicated per chunk type | Keeps the copy call monomorphic so the typed copy inlines |

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

## Re-running

```bash
./gradlew :extensions-barrage-benchmark:jmhRunBarrageCopyKernelAlgorithms      # 1M rows
./gradlew :extensions-barrage-benchmark:jmhRunBarrageCopyKernelAlgorithms100k  # 98,304 rows
```

Size is overridable without editing code: `-Dbench.totalRows=`, `-Dbench.deltaChunkSize=`. Each task
runs three forks over six run lengths and three column types, about 35 minutes. Check `uptime`
before trusting anything, and never run Gradle against `:extensions-barrage-benchmark` while a JMH
run of it is live — it rewrites the fork's classpath.
