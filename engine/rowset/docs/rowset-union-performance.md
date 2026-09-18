# Unioning N row sets

Notes from DH-23676, which replaced sequential row set insertion with `RowSetFactory.union`. Eight strategies were
built and measured against seven input shapes. This records what the measurements said, which intuitions were wrong,
and the traps that produced confident wrong answers, so that none of it has to be rediscovered.

| | |
|---|---|
| **Problem** | Inserting N row sets into one growing accumulator is quadratic when the inputs are disjoint and arrive out of key order. |
| **What shipped first** | `RowSetFactory.union` — sort by first row key, then merge in passes on an append-or-duplication rule. |
| **What replaced it** | The radix build below: when the inputs' `SortedRanges` entries would overflow one, bucket every range by block and build each block's container once into an `RspBitmap`, compacted afterwards if the inputs coalesced. The merge in passes remains for `RspBitmap` inputs and for inputs whose entries together fit a `SortedRanges`. |
| **Best case of the merge** | 25.7x — 1B rows, 1000 disjoint blocks in reverse order. |
| **Worst case of the merge** | 0.58x against the same insert-loop baseline — the 100K random-bucket comb bucketed `updateBy` produces every cycle, 245 ms against the loop's 142. See below. |

## Why sequential insertion falls over

A union of N row sets has an obvious implementation: copy the first, insert the rest. It is what
`RowSetFactory.unionInsert` did, and what most call sites hand-rolled. It is also fine most of the time, which is why
the failure mode went unnoticed.

Each non-appending insert is linear in the *accumulator*, not in the inserted set. When every input extends past the
accumulator's last key, inserts are splices and the whole union is linear. When they do not — because the inputs
interleave, or simply because they arrived in an order unrelated to their keys — every insert walks a structure that is
still growing, and N of them cost O(N x result).

That shape is not exotic. It is what the engine produces: per-region pushdown results, per-bucket `updateBy` affected
rows, per-key data index row sets. All of them arrive in hash or scheduling order, not key order.

## Eight strategies, in the order they were tried

The order matters: most were rejected because of what the previous one revealed.

### 1. Sequential insert (baseline)

Copy the first row set, insert the rest in the order given.

The hand-rolled loop and `unionInsert` are the same algorithm, and the numbers confirm it: within 1-2% of each other in
18 of 21 cells. Converting call sites from one to the other buys nothing on its own.

### 2. Random builder (rejected)

`RowSetFactory.builderRandom()` plus `addRowSet` per input.

5.8x to 130x slower than insert. It re-materializes every range through a priority queue instead of moving spans. It
did expose something worse, though — see pitfall 6.

### 3. Priority queue of range iterators (rejected)

A heap of `RangeIterator`s keyed on current range start; pop, append, advance, re-push.

One heap operation per range. 123-577 ms against insert's 2-33 ms on the same data. A pre-existing benchmark had
already concluded this; we re-measured and agreed.

### 4. Per-run priority queue (rejected)

Same heap, but drain each popped iterator for as long as it stays the global minimum, so one heap operation covers a
run of ranges instead of one range.

Removes the heap cost entirely — 162 ms to 41 ms on disjoint input — and is a wash where runs are length 1. Still never
approaches insert, because the floor is ~11 ns per range and insert moves whole spans. Kept in `UnionBenchmark` as
`unionIteratorRuns` for the record.

### 5. Balanced pairwise merge (superseded)

Merge element 2k+1 into 2k, halving each round: log2(N) passes instead of N inserts into one accumulator.

The first real win — 5.8x on interleaved input at n=100. But 17x *worse* on redundant input, where sequential insert's
accumulator saturates and later inserts are nearly free while pairwise dutifully does log N full passes.

### 6. Greedy append runs (superseded)

Keep absorbing into one accumulator while each next row set only *appends*; start a new accumulator when one would
overlap. Each accumulator takes at least one partner, so a pass at least halves the count and the merge terminates.

Recovers the ordered-disjoint case exactly (one pass, all appends) and degenerates to pairwise when nothing appends.
The first version also extended on *prepends* — see pitfall 2.

### 7. Sort, then greedy append runs (superseded)

Sort inputs by `firstRowKey` first, so the append path is reachable whatever order the caller supplies.

Makes the merge order-insensitive at negligible cost: sorting at most a few thousand references against a merge
measured in milliseconds. Still 20x worse than plain insert on redundant input.

### 8. Sort + greedy runs + duplication test (shipped)

As above, but an accumulator also keeps absorbing while the row set before it brought rows the accumulator already
held. Duplication means the inputs are covering each other and further insertion stays cheap, which is precisely the
regime where sequential insert wins.

Closes the redundant hole with no measurable cost elsewhere. Cardinality and endpoints are O(1) to read; an append
cannot duplicate anything, so the append path makes no size query at all.

## The shape matrix

No single strategy wins everywhere, so the only way to choose was to name the shapes that separate them. Each shape
exists because it discriminates against a specific wrong answer.

| Shape | Construction | Catches |
|---|---|---|
| `redundant` | Every set walks the same span from key 0; ~34% dense each. | Pairwise and greedy-run regressions. Sequential insert is sub-linear here. |
| `partial` | Successive sets overlap halfway into the previous one. | Regression guard — nothing should differ on this shape. |
| `blocks-ascending` | Disjoint sets laid end to end, presented in key order. | Loss of the one-pass append path. |
| `blocks-shuffled` | The same blocks, presented in random order. | A merge that does not sort. The headline quadratic case. |
| `blocks-descending` | The same blocks, reversed. | Prepend chains — every insert lands before everything already there. |
| `adjacent` | Abutting ranges dealt round robin, so ranges touch across sets but not within one. | Cardinality used as a proxy for range count. The union coalesces; no row duplicates. |
| `interleaved` | Ranges with gaps, dealt round robin: disjoint sets that all span the key space. | Falling back to sequential insert. Nothing appends, nothing duplicates. |

Set count matters independently of shape. The redundant shape only separates the strategies at n >= 100;
blocks-descending only at n >= 100. At n=10 on ordered input everything is within 2% and the absolute times are under
2 ms.

## Three patterns across every shape

Medians in milliseconds. `unionInsert (orig)` is the pre-change body; `iterative` is the hand-rolled accumulation loop;
`union` is what shipped. All three produce identical cardinality in every cell.

| Shape | Rows | n | unionInsert | iterative | union | Speedup |
|---|---:|---:|---:|---:|---:|---:|
| redundant | 100M | 10 | 28.05 | 27.80 | 28.21 | 0.99x |
| redundant | 100M | 100 | 4.89 | 4.88 | 4.77 | 1.03x |
| redundant | 100M | 1000 | 2.32 | 2.27 | 2.76 | **0.84x** |
| partial | 100M | 10 | 9.62 | 9.58 | 9.58 | 1.00x |
| partial | 100M | 100 | 10.40 | 10.39 | 10.50 | 0.99x |
| partial | 100M | 1000 | 11.95 | 12.01 | 12.03 | 0.99x |
| blocks-ascending | 1B | 10 | 0.74 | 0.73 | 0.74 | 1.00x |
| blocks-ascending | 1B | 100 | 1.16 | 1.17 | 1.18 | 0.98x |
| blocks-ascending | 1B | 1000 | 2.63 | 2.66 | 2.69 | 0.98x |
| blocks-shuffled | 1B | 10 | 0.97 | 0.95 | 0.72 | 1.35x |
| blocks-shuffled | 1B | 100 | 7.74 | 7.73 | 1.30 | **6.0x** |
| blocks-shuffled | 1B | 1000 | 53.10 | 59.19 | 3.31 | **16.0x** |
| blocks-descending | 1B | 10 | 1.52 | 1.49 | 1.22 | 1.25x |
| blocks-descending | 1B | 100 | 8.69 | 8.81 | 1.11 | **7.8x** |
| blocks-descending | 1B | 1000 | 76.87 | 88.40 | 2.99 | **25.7x** |
| adjacent | 1B | 10 | 74.58 | 74.73 | 74.38 | 1.00x |
| adjacent | 1B | 100 | 122.15 | 122.62 | 132.74 | **0.92x** |
| adjacent | 1B | 1000 | 536.19 | 546.56 | 580.77 | **0.92x** |
| interleaved | 1B | 10 | 413.72 | 418.36 | 280.74 | 1.47x |
| interleaved | 1B | 100 | 3952.67 | 3807.00 | 676.67 | **5.8x** |
| interleaved | 1B | 1000 | 4645.19 | 4561.20 | 2539.22 | 1.83x |

Round-robin harness rotating which pattern runs first each round: 41 rounds for the 1B block cells, 21 at 100M, 11 for
adjacent, 3 for interleaved.

### The adjacent loss is an artifact of presentation order

The 8% deficit above is measured on *ordered* adjacent input. Shuffle the same sets and the old path collapses, because
a random arrival order leaves the accumulator fragmented mid-merge while any monotone order keeps it coalesced. The new
merge sorts, so it is flat across all three orders.

| Order | n | unionInsert | iterative | union | Speedup |
|---|---:|---:|---:|---:|---:|
| ascending | 100 | 156.59 | 157.14 | 143.24 | 1.09x |
| shuffled | 100 | 1129.13 | 1125.24 | 145.68 | **7.75x** |
| descending | 100 | 168.32 | 167.85 | 154.09 | 1.09x |
| ascending | 1000 | 580.84 | 579.39 | 541.78 | 1.07x |
| shuffled | 1000 | 3980.69 | 3969.63 | 536.89 | **7.41x** |
| descending | 1000 | 574.70 | 575.46 | 572.50 | 1.00x |

## The call-site batcher

`RowSetUnionBatcher` is the batched merge the converted call sites all repeated, with their ownership bookkeeping
folded into one `SafeCloseable`. A caller hands it row sets and calls `build` for the union; it owns everything in
between, so a traversal that throws part way through abandons what it gathered instead of handing back half a union.

The batch size is the caller's own count — `indexRowKeys.size`, `filteredTable.size`, `keysToRefilter.size`,
`matchColumns.size` — taken as a `long` and clamped by the constructor to `[1, maxBatchSize]`, so a caller
counting rows rather than objects has nothing to narrow and no reason to name the cap. Under the cap that count merges the whole
input at once; over it, or where it is only an upper bound, it costs nothing to pass and the cap takes over. The clamp
is also what makes `2 * batchSize` the list's greatest extent rather than just its starting capacity.

It accumulates rather than writing into a row set the caller passes in. Every converted site wanted a new row set, and
the two that ultimately insert into a long-lived one — `SyncTableFilter` and `LeaderTableFilter`, whose targets are the
tracking row sets behind their results — do it with a single insert at the end rather than one per batch.

**Batches are not merged into one running result.** That would reintroduce exactly the problem this whole document is
about, one level up: one pass over a growing result per batch instead of per row set, which at `maxBatchSize` to a batch
is still `n/maxBatchSize` passes over something that keeps getting bigger. Instead the entries live in two regions of a list of
`2 * batchSize` slots. A full batch collapses into a single row set that stays where it is, so the front fills with
collapsed groups while the back gathers the next batch. The groups are allowed to fill their half of the list; the
batch that would need a slot past it folds everything into one instead. A result is merged into again once per `batchSize` batches rather than once per batch — the same tree
the multi-pass merge inside `union` builds, one level up, and for the same reason.

The list is all this holds onto: at most `2 * maxBatchSize` references. Each collapse still allocates what
`union` allocates — an array of its inputs and a groups array about half that size — but so did every batch under the
old hand-rolled loop, so that part is unchanged. What is *held* is unchanged for the
callers that produce a row set per key or per index entry — those inputs are disjoint, so the groups sum to the result.
Overlapping input is where holding groups costs more than a running result would: a running result stays the size of
one input while `batchSize` groups are each about that size. Two call sites could overlap and neither reaches it.
`WouldMatchOperation` overlaps, since a row can change in several match columns at once, but it sizes its batcher at
`matchColumns.size`, so it collapses once and never holds a second group. `DynamicWhereFilter.filterPartialIndex`
looks up a subset of the set's key columns, so many set values land on the same index row — it deduplicates the index
row keys it has already taken, which is less work as well as less held, and leaves the intersections it merges
disjoint. Reaching the bad case needs both overlap and enough inputs to fill the groups, which no converted call site
does — and the merge's own passes have the same property.

It does two things before the merge sees anything, both of which the merge would otherwise have to undo:

- **Empty row sets never take a slot.** The merge already compacts them away, but a batch that spends slots on them
  fills early and merges more often than the caller asked for.
- **A row set that appends to the last one is spliced onto it in place.** Ascending input therefore collapses to a
  single row set that `build` hands over as it stands: no sort, no group array, no copy-on-write reference per input.
  This is the same append test the merge makes, moved to where the batch is still one row set long.

The ordering caveat from pitfall 5 stands. Batching still forfeits the global sort, and the collapse only fires for
input that arrives in ascending order; it makes the good case cheaper, not the bad case good.

## The radix build

The merge in passes shipped on the measurements above and cost bucketed incremental `updateBy` half its throughput in
the next nightly. Everything below was learned finding out why, and is the reason `RowSetFactory.union` now builds
its result by a radix pass on the block bits whenever the inputs' entries would overflow a `SortedRanges`.

### What the merge got wrong

The shape `updateBy` hands over is one row set per dirty bucket per cycle: for 100K buckets, one set of ~50 single
keys per bucket that received an added row (all 100K under the round-robin layout, about 63K of them under the random
one), every set spanning the whole recently added region, no two sharing a key. `RowSetIncrementalInsertBench`
models it, and its `layout` parameter is the whole story:

| Layout | Insert loop (before) | Merge in passes | Why |
|---|---:|---:|---|
| `ROUND_ROBIN`, 100K buckets | 165 ms | **73 ms** | Sorted neighbours have adjacent keys; every pairwise merge coalesces ranges, so each pass is half the previous one. |
| `RANDOM`, 100K buckets | 190 ms | **345 ms** | Sorted neighbours rarely have keys that touch, so far less coalesces and each of the log2(100K) = 17 passes walks close to the full range count. (Measured before the benchmark was corrected to union only the buckets that received an added row, ~63K of 100K under this layout; the corrected cells are in the radix table below.) |

The 2x claim in the original change was measured on `ROUND_ROBIN` alone. The nightly benchmarks deal keys randomly,
and so does most keyed data. The result is the same dense region either way, so no measure of the result can tell
the two apart; only the relative alignment of the inputs does.

Four attempts preceded the radix build, each tried behind a runtime switch in one build and measured on the same
matrix; none is in the code.

- **A size threshold** in the timsort spirit, absorbing any input under N rows: does nothing below the per-set size,
  is 27% *worse* at the set size (some inputs absorb, the rest still merge pairwise), and above it collapses into the
  insert loop in sorted order. The knob is on the wrong quantity.
- **Per-step density estimates** from first and last keys, sizes and span counts: matched the threshold on the target
  shape, but still a greedy pairwise decision, and a rule that let a `SortedRanges` accumulator keep absorbing was
  quadratic per group on inputs of two keys (845 ms against 21 ms on `NEW_BLOCKS` below). Removed.
- **A pre-pass** summing `SortedRanges` entries to know the result is an `RspBitmap`, then starting from an empty one and
  inserting every small input: 158 ms on `RANDOM` 100K, the first result under the insert loop. It gives back the
  coalescing win on `ROUND_ROBIN` (105 ms against 73).
- **A planned span array** from the blocks the inputs touch, so no insert splices: 15 ms against 21 on `NEW_BLOCKS`
  at 100K, otherwise within noise of the pre-pass.

Two things came out of those that matter beyond this change:

- **Finishing a mutation per input is a walk of the span array per input.** `WritableRowSetImpl.insert` calls
  `finishMutations`, which rebuilds the cardinality cache from the lowest modified span to the last one. Over a long
  span array that was 90% of the time on the `NEW_BLOCKS` shape, not the splices it was blamed on. Any loop inserting
  many small sets should go through the unsafe mutators and finish once.
- **The `NEW_BLOCKS` shape** in `UnionBenchmark`: set `i` of `n` holds `i << 16` and `(1 << 30) + ((n + i) << 16)`,
  so every set brings two blocks nobody else touches and, inserted in first-key order, every low key splices into the
  middle of a span array whose tail is every earlier set's high key. It is the sequential-insert hazard in its purest form and
  belongs in any future matrix.

### What the radix build does

Once the pre-pass finds the `SingleRange` and `SortedRanges` inputs' entries exceed one `SortedRanges`'s capacity
(`RspBitmap` inputs do not count: they merge in passes either way), the result is built as an `RspBitmap`. That sum is an upper bound, since overlapping inputs coalesce,
so it is the heuristic that selects the build and not proof of the result's representation; a result the bitmap turns
out oversized for is compacted at the end. Two walks over the `SingleRange` and `SortedRanges` inputs bucket their
ranges by block: the first counts the block-local pieces each block receives and records the runs of blocks some range
covers whole as first, last pairs, the second places each piece, its two 16-bit block-local ends in one int, into its block's slice
of one array. `RspBitmap.makeFromBlockPieces` then walks the blocks in order and builds each container exactly once:
up to 64 pieces are sorted and coalesced directly, more go through an 8KB scratch bitmap whose runs are read off;
the container is whichever of run, array and bitmap is smallest for that cardinality and run count; a block that
comes out all ones joins a full block span; a single row is a singleton span. The span array is allocated once, at an
upper bound on its size since a block that fills up or lies within a full run takes no slot, and nothing is ever
inserted. `RspBitmap` inputs, whose insert is a walk of both span arrays, still merge
in passes, and the two results are combined by inserting the smaller into the larger. Blocks are indexed by offset from the first block when the
inputs' block range is at most 2^20 blocks, and through a hash of the block index when it is wider, which any union
spanning two regions of a table addressed by region is: regions sit 2^43 keys, or 2^27 blocks, apart.

Abutting pieces from different inputs meet in the scratch bitmap and become one run before any container exists,
which is what the pairwise tree achieved only through its passes; that is why the coalescing layouts come back.

### Measured, ms per union, one build

| Case | Insert loop | Merge in passes | Radix | Radix vs merge | Radix vs insert loop |
|---|---:|---:|---:|---:|---:|
| `updateBy` comb, `RANDOM`, 10K buckets (all dirty) | 15.6 | 28.1 | **4.1** | 6.9x | 3.8x |
| `updateBy` comb, `RANDOM`, 100K buckets (63K dirty) | 142 | 245 | **38.8** | 6.3x | 3.7x |
| `updateBy` comb, `ROUND_ROBIN`, 10K | 15.0 | 6.9 | **3.5** | 2.0x | 4.3x |
| `updateBy` comb, `ROUND_ROBIN`, 100K | 171 | 80 | **59** | 1.4x | 2.9x |
| `NEW_BLOCKS`, 10K sets | 77 | 1.56 | **0.37** | 4.2x | 208x |
| `NEW_BLOCKS`, 100K sets | 128 | 22.9 | **4.1** | 5.6x | 31x |
| `INTERLEAVED`, n=1000, 100M rows | 102 | 118 | **42** | 2.8x | 2.4x |
| `ADJACENT`, n=1000, 100M rows | 16 | 18.3 | 19.0 | 0.96x | 0.84x |
| `REDUNDANT`, `PARTIAL`, `BLOCKS_SHUFFLED`, n=1000 | | 2.8, 28.3, 2.4 | 2.8, 28.4, 2.4 | 1.0x | |

The last row's inputs are `RspBitmap`s and take the merge in passes under both, by construction. `ADJACENT` is the one
cell where the merge's coalescing was already as good as the radix build's.

### Costs

The piece array is four bytes a piece, about 20 MB transient for the 100K comb, plus eight bytes a block of offsets
over the block range when the blocks are indexed densely; past 2^20 blocks a hash map of the touched blocks and
their sort take over, so the cost follows the touched blocks and not the range. The scratch bitmap and run array are 8 KB and
256 KB per call.

### Pitfall 9. A counting sort's off-by-one is silent

The first radix build stored block `b`'s count at `b + 1` and then took an exclusive prefix sum over the shifted
array, so every block's slice began one block early and about a seventh of the rows were lost. Nothing threw. The
setup-time equality check both benchmarks make against the insert loop caught it before a timing was reported;
that check is not optional.

## Pitfalls

Each of these produced a confident, plausible, wrong conclusion first. They are the parts of this work least likely to
survive in a diff.

### 1. A fixed strategy order fabricates 25-30% regressions

Timing each strategy as a block of repetitions in a fixed order let heap and JIT state differ systematically between
them. Whichever ran first paid for the dirty heap left by data generation. This produced a consistent, reproducible,
entirely false 23-32% deficit that survived adding a warmup, and it pointed convincingly at a real code difference.

**Fix:** rotate which strategy runs first each round, and give millisecond-scale cells 40+ rounds. Under that harness
the deficit vanished to +/-4%.

### 2. Prepend is not the mirror of append

The greedy rule originally extended a run when the next row set lay entirely past the accumulator's end *or* entirely
before its start. The endpoint test says both are cheap. They are not: appending is amortized O(1) at the end of the
span array, while prepending shifts the whole array, so a descending run of prepends is quadratic.

**Measured:** blocks-descending at n=1000, 76 ms with prepend allowed — identical to plain sequential insert — against
6 ms append-only. Refusing the prepend forces a new accumulator, which is just the pairwise merge, and is 12x better.

### 3. Duplication credit must expire

Tracking duplication as a running total means one early overlapping pair licenses absorbing an unbounded run of
disjoint row sets afterwards, because the count never returns to zero. The quadratic case the rule exists to prevent
comes straight back for any input that begins with a slight overlap.

**Fix:** count only the most recent insertion. The first row set that brings nothing the accumulator already held ends
the group. Regression shape: two sets sharing one key, then a long disjoint interleaved tail
(`RowSetFactoryUnionTest.overlapThenDisjoint`).

### 4. Cardinality is a proxy for range count, and the proxy has a blind spot

What actually drives insert cost is the accumulator's range count, not its cardinality. Range count is O(spans) to
read — `RspArray.rangesCountUpperBound` walks the span array — far too expensive to consult per decision, so duplicate
*rows* stand in for it.

**Consequence:** disjoint sets whose ranges abut show zero row duplication, so the rule pairs them, even though their
ranges coalesce and sequential insertion would be slightly cheaper. Costs 8%, but only when the input is already
ordered, which is the case sequential insert is good at anyway.

### 5. Bounded batching is not free, and not always right

Merging in bounded batches bounds the row sets held at once, which matters where each entry is a freshly materialized
intersection or copy. Applied where entries are merely borrowed references, it trades a real speedup for a memory
saving that was never needed.

**Measured:** batching `updateBy`'s per-bucket merge gives 16.3 ms against 6.4 ms one-pass at 10K buckets, and 137 ms
against 71 ms at 100K — worse than the insert loop it replaced. The gain comes from ordering every bucket at once so
most of the merge appends; batching destroys it.

### 6. A fast path with no callers is not a fast path

`AdaptiveRowSetBuilderRandom` never overrode `addRowSet`, so every caller fell through to the interface default, which
walks the row set range by range. The whole-row-set path underneath had zero callers in main and had been dead since it
was written.

**And it was hiding a bug:** wiring it up immediately threw `ClassCastException`. `addToBuilderFromImpl` dispatches on
`SingleRange` then `SortedRanges` and casts everything else to `RspBitmap`, but an empty row set's implementation is
the shared `OrderedLongSet.EMPTY` sentinel. Adding an empty row set to a random builder would have thrown.

### 7. Do not build the test oracle out of the code under test

The first reference implementation built its expected result with `builderRandom()` plus `addRowSet` — a method the
same change modifies. A shared representation bug would have made implementation and expectation agree.

**Fix:** collect every row key into a `BitSet` and append through a sequential builder. Not a `TreeSet<Long>`: boxing
every key is both slow and a pattern with history here. `Math.toIntExact` on each key is a deliberate tripwire if
someone adds a large-key shape.

### 8. Verify the cost model before optimizing against it

The per-step decision reads `acc.size()`, and `RspArray` invalidates its cardinality cache on every mutation, which
looks like an O(spans) rebuild per insert.

**It is not:** every `RspBitmap` mutation ends in `finishMutations()`, which rebuilds the cache, so `size()` really is
O(1). A variant that avoided the query entirely measured within +/-2% everywhere, confirming it. The
plausible-sounding optimization was for a cost that did not exist.

## Numbers worth knowing

| Constant | Value | Why it matters here |
|---|---:|---|
| `SortedRanges.MAX_CAPACITY` | 8193 | Entries, so roughly 4096 ranges. Above it a set becomes an `RspBitmap` and the insert path changes character — the cause of a non-monotonic result that looked like a measurement error. |
| RSP block size | 65,536 | Keys per span. Whether an incoming range starts a new block decides whether a pre-pass can pay for itself. |
| `MixedBuilderRandom.addAsIndexThreshold` | 65,536 | Gates the builder's whole-set path on the *incoming* range count alone, ignoring the accumulator. Still open — the same class of mistake as pitfall 4. |
| `RowSetUnionBatcher.maxBatchSize` | 8192 (property `RowSetUnionBatcher.maxBatchSize`; was the constant 1024) | The most row sets gathered into one batch, whatever count a caller asks for. Callers pass their own count; this is the ceiling that keeps data-driven input from holding an unbounded number of row sets. It caps the batch, not every merge — the final `build` also hands over the collapsed groups, up to `2 * batchSize - 1` row sets. Raised for the radix build, which merges a batch of small row sets in one linear pass: see the cap sweep under the radix build. |

## Still unresolved

- **The 64K builder threshold** decides on the incoming set's range count without looking at the accumulator it is
  merging into. The queue path only pays when the accumulator is large relative to the input.
- **Redundant input at n=1000** costs ~19%, about 240 ns per input set of decision overhead on a shape where nothing
  ever appends, so every step pays the full test. A latch on first duplication would remove most of it, since the
  duplication count is monotonic within a group.
- **The nightly bucketed `updateBy` benchmarks** have not yet run against the radix build. The end-to-end reproducer
  from the regression report has: 10M rows, 90,900 random-key buckets through `AutoTuningIncrementalReleaseFilter`
  into a bucketed `RollingMax`, median of five reps, two fresh JVMs a side, measured 6.65 s and 7.38 s against 8.55 s
  before the merge shipped and 13.8 s and 14.7 s with it. The nightly is the remaining check, as it was the one that
  caught the merge's 2x in isolation turning into -54% in place.
- **Interleaved input at n=1000** remains 2.5 seconds however it is merged. Nothing tried helps meaningfully; the
  result genuinely has 38M ranges.

## Methodology

Measurements come from a single sandboxed machine on JDK 21, not from CI. The four-strategy shape sweeps and the
three-pattern tables come from standalone harnesses; the `updateBy` cycle numbers are JMH
(`RowSetIncrementalInsertBench`), as are the `UnionBenchmark` strategy comparisons. Treat sub-2x differences on cells
under 3 ms as noise.
