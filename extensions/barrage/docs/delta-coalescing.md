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

Measured on the original reproduction — 100 cycles each re-modifying the same 10k rows of eight
`long` columns, one flush at the end: **100 MiB** of pooled chunk storage (62.5 MiB of actual data,
the rest power-of-two pool rounding) to send **626 KiB**. (From the producer benchmark written to
size the problem; it is not in the tree.) The fix is to coalesce the queue *before* the flush, off
the update-graph thread.

## Design

### Recording and triggering

Each cycle appends a `BarrageMessageDelta` under the producer's monitor. After appending,
`shouldCompact()` decides whether to schedule compaction:

- **Byte trigger** — bytes recorded since the last compaction exceed `max(COMPACTION_FLOOR_BYTES,
  COMPACTION_GROWTH_FACTOR × compacted head size)`. Defaults: floor 4 MiB, factor 1.0. The floor
  keeps producers whose subscribers are served often enough from ever paying; the geometric term
  bounds total copying to linear in data recorded, and the queue to about `(1 + factor)` times the
  compacted footprint.
- **Count trigger** — `COMPACTION_MAX_PENDING_DELTAS` (default 32) deltas since the last compaction,
  whatever their size, bounding the per-delta overhead (row sets, update descriptions) that the byte
  policy cannot see.

### Scheduling

`CompactionJob` runs on a scheduler thread and takes `updatePropagationJob.runLock` with `tryLock`.
Failing to take it means propagation is already flushing, which makes the compaction moot, so the
job gives up rather than hold a scheduler thread; the next enqueue re-evaluates. The update-graph
thread is never blocked — it keeps appending under the monitor, which the job takes only to copy out
the run and later to splice the result in. Coalescing is therefore **single threaded**, across both
columns and rows, as it was before this change.

Compaction is **generation-scoped**: `compactLeadingRun` replaces only the leading run of
same-generation deltas. A generation is bumped by `promoteSnapshotToActive`, and deltas either side
of one describe different subscription bases, so coalescing across that boundary would break the
pre/post-snapshot split a late joiner depends on. A failure anywhere in the job fails every
subscription, because the pending queue is the subscribers' only record of what changed.

### Coalescing a run

`BarrageMessageDelta.coalesce` walks the run latest-first so each surviving row takes its value from
the last delta that recorded it, and produces one synthetic delta.

The per-column work is shared. `ColumnMappingCache` keys a `ColumnMapping` on `MappingKey.forColumn`
— which deltas modified the column, plus the identity of their recorded modification row sets — so
columns that tick together compute their mapping once. Adds are common to every column, so
`addedRuns` is shared whenever the key matches.

### Runs, and the element mapping

A `ColumnMapping` holds `BarrageCopyKernel.Runs`: for each stretch contiguous in both the output and
its source chunk, the first output position, the encoded origin of its first row, and the length.
Row sets are range-compressed and updates arrive in ranges, so building this costs proportionally to
ranges rather than rows.

Below the array-copy threshold the copy kernel (`BarrageCopyKernel`, described next) does not read
the runs directly; it calls `Runs.convertRunsToElementMapping`, which converts them to one encoded
origin per output row and **drops the run arrays**. Runs cost three longs per run against the
mapping's one long per row, so at a run per row they are three times the mapping — and up to six
times it, because `add` grows its three arrays by doubling and the last doubling can leave almost
half of each unused. The conversion is idempotent and unsynchronized — safe because coalescing is
single threaded — and `BarrageCopyKernel.copy` tests `elementMapping != null` *before* the
run-length test, which makes `copyByRuns` structurally unreachable once a `Runs` has been converted.
Columns sharing a `MappingKey` share the mapping too, restoring the reuse that existed before this
change, when one mapping array was passed to every column with the same modification pattern.

### The copy kernel

`BarrageCopyKernel` is replicated per chunk type from `CharBarrageCopyKernel`, the Object variant
included (`./gradlew replicateBarrageUtils`). `charToObject` leaves two things for
`ReplicateBarrageUtils.fixupObjectBarrageCopyKernel` to correct: a `WritableObjectChunk` takes two
type parameters where the primitive chunks take one, and the `charContext` local comes out
capitalized along with the type name. Per column the kernel chooses once:

- **`copyByRuns`** — average run length ≥ `MIN_AVERAGE_RUN_LENGTH_FOR_ARRAY_COPY`, defaulting to 3:
  split each run at origin and destination chunk boundaries, move each stretch with one typed array
  copy.
- **`copyByElements`** — below it: fill the destination in order from the element mapping.

`DELTA_CHUNK_SIZE` is rounded up to a power of two so a position locates its chunk by a shift and
its offset by a mask.

## Measurements

All numbers below are JMH `AverageTime`, ms/op, lower is better, `±` is the 99.9% confidence
interval JMH reports.

**Hardware**: Apple M1 Max, 10 cores, 64 GiB, macOS 26.6.2 (arm64). **JVM**: Zulu 21.0.1
(`21.0.1+12-LTS`), G1, `-Xmx8g`. JMH 1.37. **Settings** unless stated: 1 fork, 3×2s warmup, 8×2s
measurement, `-gc true`. **Benchmark**:
`extensions/barrage/benchmark/.../BarrageCopyKernelBenchmark.java`, 1M rows (2^20), 64Ki-row delta
chunks, two deltas, runs at random origins.

Two methodology points that matter:

- **`-gc true` is required.** Both arms allocate megabytes per operation; without forcing GC between
  iterations, `Double` at average run 2 came back `31.605 ± 45.164` — unusable. With it, the same
  cell was `12.530 ± 0.211`.
- **The machine must be quiet.** Check `uptime` first; a browser and IDE language servers can put
  this machine at load 200+, and an early measurement taken at load 218 gave a conclusion that a
  repeat at load 3 reversed.

### 1. The algorithm: before and after

`mappingOnly` is the algorithm as it stood before this change — expand every surviving row into a
mapping, gather cell by cell, whatever the run structure (the expansion is timed with the gather
because that algorithm cannot copy without it). `rangeAware` is `BarrageCopyKernel.copy`.

At 1M rows:

| avg run | Int before → after | Double before → after | String before → after |
| --- | --- | --- | --- |
| 1 | 11.48 → 10.61 | 13.44 → 12.57 | 55.07 → 60.95 |
| 2 | 11.19 → 10.19 | 13.48 → 11.66 | 61.89 → 44.16 |
| 5 | 9.62 → **5.67** | 11.61 → **6.89** | 52.03 → **11.94** |
| 16 | 7.76 → **1.46** | 8.14 → **2.04** | 58.93 → **2.36** |
| 50 | 4.71 → **0.68** | 5.12 → **1.06** | 57.85 → **1.15** |

At 100K rows (98,304 rows, 24 chunks of 4096, geometry held comparable):

| avg run | Int before → after | Double before → after | String before → after |
| --- | --- | --- | --- |
| 1 | 0.771 → 0.729 | 0.810 → 0.782 | 1.931 → 3.485 |
| 2 | 0.605 → 0.582 | 0.704 → 0.644 | 1.062 → 2.391 |
| 5 | 0.487 → **0.262** | 0.550 → **0.278** | 2.109 → **0.521** |
| 16 | 0.396 → **0.072** | 0.391 → **0.103** | 1.621 → **0.128** |
| 50 | 0.366 → **0.032** | 0.362 → **0.052** | 0.703 → **0.060** |

The old cost is per row and barely moves with run structure — at 1M the `String` column sits between
52 and 62 ms at every run length — while the new cost falls with run length, because a run becomes
one `System.arraycopy`. By runs of 50 that is 6.9x for `int`, 4.8x for `double` and 50x for `String`
at 1M, and 11.4x, 7.0x and 11.7x at 100K.

**At runs of 1 and 2 the two arms are the same algorithm**: both expand every row into a mapping and
gather from it, so what is left is implementation detail — where the chunk arrays are cast, whether
the destination arithmetic folds. For the primitives that shows up as a 1.04–1.16x spread either
way, which is noise rather than a result.

`String` at those two lengths is not noise, and the two sizes disagree: level at 1M (1.11x one way
at a run per row, 1.40x the other at two, both inside ±6–7) but **1.8–2.3x slower after the change
at 100K**, with intervals tight enough to be real. Both arms build the same mapping and gather from
it there, so the gap is in the kernel's path rather than the algorithm. Unexplained, and related to
the reference-path oddity in the open questions below.

#### Fable 5.1 Best Guess why String is slower with the same algorithm

The two loops are the same algorithm, and with an 8 GB heap compressed oops make a reference the
same four bytes as an int, so they move identical bytes; the whole String-versus-Int gap, 12 ns per
row in the old arm and 28 in the new, is the work a reference store adds that an int store does not.
That work is the G1 post-write barrier, which on a cross-region store to a clean card takes a slow
path of a StoreLoad fence plus a card enqueue, and the aastore type check, which C2 removes only if
the shared profile of WritableObjectChunk.set lets it prove the array is exactly Object[]. Neither
is decided by the loop's source: the barrier slow path fires as often as cards get re-cleaned by
concurrent refinement and the forced collection between iterations, and the two arms allocate
differently, the kernel nulling its run arrays and building its mapping inside the timed window, so
refinement pacing and region placement differ between forks. At 100K everything sits in cache and a
16 ns per-row difference in barrier cost is visible; at 1M every gathered reference is a DRAM miss
at about 55 ns and the same difference disappears under it. The old arm's non-monotonic 1M result,
more expensive at runs of two than one, fits the same explanation. One run of the two String arms
under -XX:+UseParallelGC, whose barrier is a bare card mark, would confirm or rule it out.

### 2. Copying straight from the runs, with no mapping

The mapping looks like pure overhead: it is an array the size of the output, written once and read
once, to hold information the runs already carry. So the element path was tried without it — walk
the runs, and for each row compute its source and destination chunk and offset and assign the
element there. No allocation, no expansion pass.

It was clearly worse (1M, ms/op, average runs below the array-copy threshold):

| ms/op | Int run 1 | Int run 2 | Double run 1 | Double run 2 |
| --- | --- | --- | --- | --- |
| via the mapping | **11.20** | **9.71** | **14.03** | **12.89** |
| straight from the runs | 16.91 | 12.04 | 25.50 | 16.26 |

and at 100K the same ordering, with the margin shrinking as the working set does: Int 0.731 against
0.872 at a run per row, Double 0.770 against 0.921, the two paths level by runs of two.

The reason is the shape of the loop rather than the arithmetic. Copying from the runs assigns
elements *inside* a per-run loop, so every row carries that run's bookkeeping — decode the encoded
origin, select the delta's chunk array, establish the inner bounds — and at an average run of one or
two the inner loop body runs once or twice before all of that is paid again. The destination is
written in an order that the loop structure obscures, so neither the bounds checks nor the chunk
lookups can be hoisted. Building the mapping first costs one flat pass, and buys a gather loop whose
variable *is* the output row index: no run bookkeeping, no destination arithmetic, and a linear
sweep the JIT and the prefetcher can both follow.

The same experiment on reference columns did not show the same margin, which is part of why the
reference element path is still an open question below. The arm itself is no longer in the
benchmark; these figures were measured on the same machine and settings as the rest.

### 3. Table size: is 1M rows necessary?

Reading §1's two tables against each other. "Speed-up" is before ÷ after; "scaling" is how much each
arm's own cost grew from 100K to 1M, where the row count grew 10.67x.

| type / avg run | speed-up at 1M | speed-up at 100K | before, 100K → 1M | after, 100K → 1M |
| --- | --- | --- | --- | --- |
| Int 1 / 5 / 50 | 1.08x / 1.70x / 6.93x | 1.06x / 1.86x / 11.44x | 15x / 20x / 13x | 15x / 22x / 21x |
| Double 1 / 5 / 50 | 1.07x / 1.68x / 4.85x | 1.04x / 1.98x / 6.96x | 17x / 21x / 14x | 16x / 25x / 20x |
| String 1 / 5 / 50 | 0.90x / 4.36x / 50.1x | 0.55x / 4.05x / 11.7x | **29x / 25x / 82x** | 17x / 23x / 19x |

For primitives 1M is overkill. The speed-ups agree at both sizes, and the 100K intervals are several
times tighter, so the smaller table answers the question better and faster. Everything scales at
13–25x for a 10.67x row increase — superlinear, as a working set that outgrows cache should be, but
evenly across both arms.

For references it is **not** overkill. The old arm's `String` cost grows 82x at runs of 50 while its
own `int` cost grows 13x, and the size changes the *sign* of the short-run comparison (§1). The
mapping is 8 MB at 1M rows, landing in a heap holding a million strings; at 100K it is 786 KB
against 100K strings and the GC component all but disappears. Any conclusion about reference columns
has to be taken at the size it was measured.

### 4. The array-copy threshold

Shipped kernel against the same kernel with the threshold forced to 1 (array copy everywhere).
Separate JVM runs, so read the run-5 row — identical code in both — as the calibration:

| ms/op | run 1 | run 2 | run 5 (control) |
| --- | --- | --- | --- |
| Int, element path | **11.73** | 10.66 | 4.14 |
| Int, forced array | 18.75 | 11.32 | 5.82 |
| Double, element path | **15.25** | 12.95 | 5.78 |
| Double, forced array | 29.06 | 14.64 | 6.48 |

A one-row run costs the array path four divisions and a call to move a single element. The crossover
sits between 2 and 3; the threshold is 3.

### 5. Does sharing runs and the mapping pay?

Two columns of one type per operation; reported time covers both copies. A fresh `Runs` is built per
invocation outside the measurement — mandatory, since conversion consumes it and JMH would otherwise
amortize one build across a whole iteration.

| avg run | 1M separate → shared | 100K separate → shared |
| --- | --- | --- |
| 1 | 20.87 → **18.70** (−10.4%) | 1.420 → **1.244** (−12.4%) |
| 2 | 20.02 → **18.51** (−7.5%) | 1.133 → **0.938** (−17.2%) |
| 5 | 13.01 → 12.81 (within error) | 0.531 → 0.521 (within error) |
| 16 | 3.33 → 3.25 (within error) | 0.149 → 0.146 (within error) |

The bottom two rows are the control: at or above the threshold no mapping is built and sharing saves
nothing, so the saving below it is exactly one mapping build. This measures the smallest possible
group; a real group of N columns sharing a key saves N−1 expansions.

### 6. Why the kernel is still replicated per type

A single generic implementation sees every chunk class, goes megamorphic and stops the typed copy
inlining: measured ~32% slower at average run 1, ~19% at 16, ~8% at 1024. A middle option — typing
only the destination array and dropping the context — tied at runs 1–8 and was 5–8% slower at ≥16.
*(These two were measured against an earlier shape of the kernel, on the same machine, with a
benchmark arm that no longer exists; the conclusion stands, the exact figures should be re-measured
before being relied on.)*

## Decisions recorded

| Decision | Why |
| --- | --- |
| Geometric byte trigger plus a count cap | Total copying linear in data recorded; the count cap bounds per-delta overhead the byte policy cannot see |
| `tryLock`, never block the scheduler thread | A held lock means a flush is imminent, which makes compaction moot |
| Delta chunks stay pooled; last chunk asks for the exact remainder | Pooling dominates; the pow2 rounding on one chunk per column is acceptable |
| Append-only runs are declined, not concatenated | Coalescing them drops no rows, so it would copy everything and save nothing |
| Uniform chunk layout (no ragged chunks) | Every delta, recorded or compacted, must keep the layout the kernel and later compactions assume |
| `deltaChunkSize` rounded up to a power of two | Shift-and-mask addressing; an arbitrary configured value still starts the server |

## Open questions

- Whether the element path is worth having at all. It exists for average runs of one or two, which
  may be rare in the wild: real updates arrive as ranges, and a coalesce whose surviving rows are
  that scattered has to be a table modified in a genuinely scattered pattern. If that turns out to
  be a corner case, the kernel could drop to one copy shape and lose the threshold, the mapping and
  the conversion with it. Deciding needs run-length distributions from real workloads or an
  executive decision.
- Failing that, the threshold is one number for every type, and probably should not be.
  Extrapolating the array path's cost curve down from the measured run lengths puts its crossover at
  2 for `String` but above 2 for `int` and `double`, so a `String` column with runs of two may be
  taking the slower path. Keeping 3 everywhere for now; a per-type threshold is the obvious future
  refinement, and needs a direct measurement rather than an extrapolation to justify it.
- The reference element path behaves oddly in two ways nobody has explained. It is not monotonic in
  run length at 1M — in §1 the old arm costs 55.07 ms at a run per row and 61.89 ms at runs of two,
  where more contiguity should not cost more — and at 100K the new kernel is 1.8–2.3x slower than
  the old one at those same lengths while the primitives are level. Both reproduce across
  independent implementations, so they are not one bad loop.
- Mappings live until the `ColumnMappingCache` closes, so N distinct keys hold N mappings at once
  where previously each was transient. Dropping the runs on conversion more than offsets it, but the
  peak is now proportional to the number of distinct modification patterns.
- Coalescing is serial across columns. Columns are nearly embarrassingly parallel — separate
  destinations, read-only shared `Runs` — but `ColumnMappingCache.getOrCompute` computes lazily and
  would need pre-computation or synchronization first.

## Re-running

```bash
./gradlew :extensions-barrage-benchmark:jmhRunBarrageCopyKernelAlgorithms   # §1
./gradlew :extensions-barrage-benchmark:jmhRunBarrageCopyKernelSharing      # §5
./gradlew :extensions-barrage-benchmark:jmhRunBarrageCopyKernelAlgorithms100k  # §3
```

Size is overridable without editing code: `-Dbench.totalRows=`, `-Dbench.deltaChunkSize=`. Check
`uptime` before trusting anything, and never run Gradle against `:extensions-barrage-benchmark`
while a JMH run of it is live — it rewrites the fork's classpath.
