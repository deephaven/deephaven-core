# RowSet overlaps and subsetOf

How `RowSet.overlaps` and `RowSet.subsetOf` decide an answer between two non-trivial row sets, which of the two sides
each path walks and which it probes, and why. Written alongside DH-23694, which found that both operations degraded to
a linear merge over one side's ranges even though both representations can binary search to a key.

| | |
|---|---|
| **Entry points** | `RowSet.overlaps` → `OrderedLongSet.ixOverlaps` → `SortedRanges.overlaps(SortedRanges/RspBitmap/RangeIterator)`, `RspArray.overlaps(RspArray, RspArray)`; `RowSet.subsetOf` → `ixSubsetOf` → `RspBitmap.subsetOf(SortedRanges)` |
| **The idea** | both cursors seek; a probe that misses reports where the probed side's next candidate begins, and the walked side skips to it |
| **What decides which side is walked** | array positions for `SortedRanges` x `SortedRanges`, span count for `SortedRanges` x RSP, cardinality against array length for `RspBitmap.subsetOf(SortedRanges)` |
| **Hot path** | `BaseTable.validateUpdateOverlaps`, on by default, runs three `subsetOf` and one `overlaps` per notification of every ticking table |
| **Benchmarks** | `RowSetOverlapsBench`, `RowSetSubsetOfBench` (`jmhRunRowSetOverlaps`, `jmhRunRowSetSubsetOf`), sharing shape generators through `RowSetShapes` |
| **Tests** | `OverlapsTest`, `SubsetOfTest` (cross-checked against a linear merge **and** against `ixIntersectOnNew` / `ixMinusOnNew`, over every pair of representations and both argument orders) |

## Why this is on the hot path

These are not occasional operations. `BaseTable.validateUpdateOverlaps` runs from `notifyListeners`, on **every
notification of every ticking table**, and it is on by default (`BaseTable.validateUpdateOverlaps`, default `true`;
only `BaseTable.validateUpdateIndices` defaults to `false`). Each call makes three `subsetOf` tests, and when the
update carries no shifts, one `minus` and one `overlaps` as well:

```java
final boolean currentMissingAdds = !update.added().subsetOf(getRowSet());
final boolean currentMissingModifications = !update.modified().subsetOf(getRowSet());
final boolean previousMissingRemovals = !update.removed().subsetOf(getRowSet().prev());
...
try (final RowSet removedMinusAdded = update.removed().minus(update.added())) {
    currentContainsRemovals = removedMinusAdded.overlaps(getRowSet());
}
```

So the shapes that matter most are the ones that validation actually presents: a small, recently built added or removed
set against a large tracking row set, in whatever pair of representations the two happen to hold — which is exactly the
asymmetric case where the old code's choice of which side to walk was worst, and the `DENSE_VS_SPARSE` pattern in the
benchmarks below. Every cycle of every ticking table in the DAG pays this, so a constant factor here is not academic.

## What each representation can search

A `SortedRanges` is one sorted array of packed *entries*: a non-negative entry starts a range or stands alone, a
negative entry is the negated end of the range whose start precedes it. So the array holds one position per single key
and two per longer range, and the number of *ranges* and the number of *positions* differ by up to 2x. It searches with
`absRawBinarySearch(packedTarget, startIdx, endIdx)` over any sub-range of positions, and with
`absRawGallopingSearch` from a carried position (below).

An `RspArray` is a sorted array of *spans* keyed by block. A span is a singleton, a container of low bits within one
block, or a run of full blocks. `getSpanIndex(fromIdx, blockKey)` searches for a block from a carried index, and
reading a span's contents needs a `SpanView`, which the one-shot paths borrow from `workDataPerThread` and return on
every call.

Two consequences run through everything below. First, neither representation keeps a range count, so every
"which side is cheaper to walk" decision here is made from a proxy that is available in constant time. Second, a span
is not a ceiling on what a probe steps over: a container span can hold many disjoint ranges inside one block, and a
full block span can cover many blocks.

## The shared idea

Before DH-23694, each of these operations picked a side, walked it one range (or one span) at a time, and searched only
on the other side. That makes the cost the walked side's range count even when the two sets interleave a handful of
times over millions of keys.

Now both cursors seek. The probe returns, on a miss, the position it stopped at, which names the probed side's next key
that could still match; nothing on the walked side below that key can overlap, so the walk seeks there instead of
stepping through the ranges in between. Disjoint inputs that interleave coarsely then cost a search per *alternation*
rather than a step per range, and tightly interleaved inputs — where there is nothing to skip — must not be made worse,
which is what the guards and the galloping search are for.

## overlaps

### The dispatch

```
ixOverlaps(other)
├─ either side empty ................................ false
├─ other is a SingleRange ........................... overlapsRange
├─ SortedRanges x SortedRanges ...................... overlaps(SortedRanges): walk the side with fewer
│                                                     array positions, probe the other, both gallop
├─ SortedRanges x RspBitmap ......................... overlaps(RspBitmap):
│  ├─ other.size() < count / 2 ...................... walk other's ranges, probe our array
│  │                                                  (overlaps(RowSet.RangeIterator))
│  └─ otherwise ..................................... walk our array, probe other through one
│                                                     RspArray.OverlapProbe
├─ RspBitmap x RspBitmap ............................ RspArray.overlaps: walk the smaller array,
│                                                     probe the other, skipping with spanIndexAtOrAfter
└─ anything else .................................... overlaps(RowSet.RangeIterator)
```

### SortedRanges x SortedRanges

`overlaps(SortedRanges other)` walks whichever side has fewer array positions (`count <= other.count`) and probes the
other. Both cursors gallop from a carried position, and a miss on the probed side yields its next key, which the walked
side seeks to.

Be precise about what choosing by array length does. When the two lengths differ, the caller's argument order stops
deciding the cost — the common case, and the point of the rule. When they are *equal* the receiver is walked, so the
two orders can still cost different amounts. And array length is only a proxy for range count: `2n` singletons and `n`
longer ranges both occupy `2n` positions. Deciding this properly needs a range count neither representation keeps, so
the claim is "mostly normalized", not "always".

### SortedRanges x RspBitmap

Walking our packed array and probing the RSP goes through `RspArray.OverlapProbe`, a resumable form of
`overlapsRange` that carries both cursors a caller would otherwise re-establish per call:

- **A span view of its own.** The one-shot `overlapsRange` has nowhere to keep a view, so it takes one from the
  thread's work data and gives it back on every call. A caller testing one key per span pays that for every key, which
  is what made probing lose to simply walking the RSP's ranges. The probe holds one view, as `RspRangeIterator` does,
  and re-initializes it only on moving to a different span. It is allocated on first use, since probes answered from
  block keys alone never need one.
- **A span index.** An ascending caller searches from where the last probe stopped.
- **No reference on the array.** A probe belongs to one operation and is closed before that operation returns, so the
  array cannot be mutated while one is open — the same reasoning as `RspArray.overlaps(RspArray, RspArray)`. Cursors
  that *do* outlive their caller, such as `SpanCursorForwardImpl`, acquire instead, which marks the array shared and
  makes the next mutation copy.

Which side is walked is a guard, not a bound. A probe costs a span search and possibly a span view where a step of the
walk over the RSP's ranges amortizes both over the ranges read out of one container. Span count is the cheap proxy for
how much a probe can miss, so we walk ourselves while `other` has at least `count / 2` spans — that being the fewest
ranges our own array can hold — and walk `other` below that.

On a miss, `probe.resumeBlockKey()` gives the first key of the block the next probe will start on. The probe resumes on
the span it *stopped at*, not the one after, because that span can still hold keys above the range just tested; so our
own range end is the floor for the skip:

```java
key = Math.max(end + 1, probe.resumeBlockKey());
```

Without that floor the walk can skip past a key it has not tested.

### RspBitmap x RspBitmap

`RspArray.overlaps(RspArray, RspArray)` already picked the smaller side to walk, but it walked that side one span at a
time while only the probed side sought. It now skips with `spanIndexAtOrAfter(r, fromIndex, blockKey)`: the first span
of the walked side at or after `fromIndex` that reaches `blockKey`'s block, every span below that ending beneath it and
so unable to match.

The tie is broken deterministically and towards *not* searching:

```java
if (fromIndex >= r.size || r.getKey(fromIndex) >= blockKey) {
    return fromIndex;
}
```

Sides that alternate span by span take that branch every time, and a search there would cost more than the step it
replaces.

### The galloping search

`absRawGallopingSearch` brackets the answer by doubling steps from the carried position, then runs
`absRawBinarySearch` inside the bracket. `packedGallopingSearch` wraps it with `packedBinarySearch`'s sign contract and
sits beside it.

This is load-bearing rather than incidental. With a plain binary search over the remaining tail, seeking is a
*regression* on tightly interleaved input: the search costs `O(log n)` where the old loop stepped once. Galloping costs
a handful of reads next to the carried position for a near answer and stays logarithmic in the distance to a far one.

Two details that look like noise and are not:

- **The one-shot entry points do not gallop.** `overlapsRange(start, end)` searches the whole array once with
  `absRawBinarySearch`; a gallop there would only add probes on the way to a far answer. Galloping is for callers
  carrying a cursor across ascending ranges.
- **`packedGallopingSearchPos` exists as a method.** It is `packedGallopingSearch`'s answer normalized to a plain array
  position. Doing that inline costs `overlaps(SortedRanges)` half its speed on some shapes — the two extra statements
  are enough to push that loop past a threshold the JIT compiles it differently on either side of.

### Iterator ownership

`overlaps(RowSet.RangeIterator)` **takes ownership of the iterator and closes it**, as `subsetOf(RowSet.RangeIterator)`
does. The answer is usually reached with ranges still unread, and an iterator over a reference-counted row set holds a
reference on it until closed; leaving it open marks that row set shared for good, so every later mutation of it copies
first.

## subsetOf

`subsetOf` is not symmetric, and unlike `overlaps` it cannot skip: every range of the subject has to be found in the
superset, because any one of them can falsify the answer. The win available is therefore smaller, and it comes from not
re-establishing cursors and from walking the less fragmented side.

`RspBitmap.subsetOf(SortedRanges sr)` applies both ideas:

```
subsetOf(sr)
├─ we are empty ..................................... true
├─ sr is empty ...................................... false
├─ first() < sr.first() || sr.last() < last() ....... false
├─ isCardinalityCached() && getCardinality() < sr.count()
│   └─ walk our ranges, ask sr to cover each through containsRangeFrom, carrying a position
└─ otherwise
    └─ walk sr's ranges, probe our gaps through one OverlapProbe
```

The second walk carries one `OverlapProbe` across all the gaps rather than taking a span view per gap. The first walk
is new: `SortedRanges.containsRangeFrom(startPos, start, end)` is a resumable `containsRange` built on the galloping
search, returning the position of the covering range to pass back for the next one, or -1 if the range is not covered.
A caller whose ranges advance in step with `sr`'s then pays a constant per range rather than a search of the whole
array.

Which walk is chosen is, again, a guard over proxies. Our cardinality is an upper bound on our range count, and `sr`'s
array holds one or two positions per range, so its range count is at least half its length. Taking our side only when
our cardinality is below that length bounds how wrong the choice can be: our ranges are then at worst twice `sr`'s,
never the thousandfold gap a *span* count would allow when many of our ranges share one span. The cardinality has to be
already cached — computing it walks the spans, which is the cost being avoided.

## Measured behaviour

All timings in this document come from one machine, measured in September 2026:

| | |
|---|---|
| **CPU** | Intel Core i9-14900KS, one socket, 32 hardware threads, 36 MiB L3; `performance` frequency governor |
| **Memory** | 128 GiB |
| **OS** | Ubuntu 26.04 LTS in an LXC container on a Proxmox VE host, Linux 7.0.14-14-pve x86_64; 24 threads visible to the container |
| **JVM** | Temurin OpenJDK 21 (Gradle toolchain), 64-bit server VM, with the engine's standard benchmark options: G1, 8 GiB heap, `-DConfiguration.rootFile=dh-tests.prop`, `-DQueryTable.memoizeResults=false` |
| **Harness** | JMH 1.37, one fork, two 1-second warmup and three 1-second measurement iterations, average time per operation, one thread |
| **Benchmarks** | `RowSetOverlapsBench` and `RowSetSubsetOfBench` via `jmhRunRowSetOverlaps` / `jmhRunRowSetSubsetOf` |

The absolute numbers will move with hardware, OS and JIT; what the design rests on is the ratios between the walks and
where they cross over, which is what to reproduce with those benchmarks before re-tuning any of the guards.

### The matrix

Both benchmarks build their two sides from the same 12 `RowSetShapes.Pattern`s, so the two measure the same shapes:

| Pattern | What it is for |
|---|---|
| `INTERLEAVED_KEYS` | alternating single keys one apart — the tightest interleaving there is, nothing to skip |
| `INTERLEAVED_BLOCKS` | alternating single keys a block apart, so every step crosses an RSP span |
| `INTERLEAVED_RUNS` | alternating runs of 64 keys |
| `SEPARATED` | the whole of one side below the whole of the other |
| `CLUSTERED` | a few alternating clusters — a walk pays per range, a seek per cluster |
| `CLUSTERED_BLOCKS` | `CLUSTERED` spread a block per key, so an RSP side holds a span per key |
| `SAME_BLOCK_KEYS` | one key per block in the *same* blocks — the block search hits every time; a seek cannot help and must not hurt |
| `SAME_BLOCK_CONTAINERS` | `SAME_BLOCK_KEYS` with several keys per shared block, so spans are containers |
| `TOUCH_AT_START` / `TOUCH_IN_MIDDLE` / `TOUCH_AT_END` | interleaved a block apart, sharing one key at each position — where the answer is |
| `DENSE_VS_SPARSE` | deliberately unequal range counts between the two sides |

`RowSetOverlapsBench` runs 3 layouts x 12 patterns x 3 sizes (64, 512, 2048 ranges per side) x both argument orders =
**216 configurations**. `RowSetSubsetOfBench` names the two sides' representations separately, since subset testing is
directed, and adds where the answer is: 2 x 2 representations x 12 patterns x 3 sizes (64, 512, 1024) x 3 outcomes
(`SUBSET`, `MISSING_LATE`, `MISSING_EARLY`) = **432 configurations**.

Both tables below are one slice of their matrix — 2048 and 1024 ranges per side, and for `subsetOf` the `SUBSET`
outcome, where the walk has to read everything. Every number is a median over the argument orders, us/op, measured
against upstream main with the same benchmark code on both sides. 0.95-1.05x is run-to-run noise on this machine. The
summary rows give the range and the geometric mean — geometric because these are ratios, where an arithmetic mean would
let one 170x outweigh everything else.

### overlaps

| Layout | Pattern | Before | After | Change |
|---|---|---:|---:|---:|
| SortedRanges x SortedRanges | `CLUSTERED` | 3.909 | 0.053 | **73x** |
| SortedRanges x SortedRanges | `CLUSTERED_BLOCKS` | 3.853 | 0.053 | **73x** |
| SortedRanges x SortedRanges | `DENSE_VS_SPARSE` | 2.609 | 1.660 | 1.57x |
| SortedRanges x SortedRanges | `INTERLEAVED_BLOCKS` | 10.6 | 8.135 | 1.31x |
| SortedRanges x SortedRanges | `INTERLEAVED_KEYS` | 9.434 | 8.674 | 1.09x |
| SortedRanges x SortedRanges | `INTERLEAVED_RUNS` | 14.1 | 11.7 | 1.20x |
| SortedRanges x SortedRanges | `SAME_BLOCK_CONTAINERS` | 10.8 | 8.506 | 1.27x |
| SortedRanges x SortedRanges | `SAME_BLOCK_KEYS` | 10.7 | 8.210 | 1.31x |
| SortedRanges x SortedRanges | `SEPARATED` | 0.004 | 0.002 | 1.75x |
| SortedRanges x SortedRanges | `TOUCH_AT_END` | 10.7 | 8.224 | 1.30x |
| SortedRanges x SortedRanges | `TOUCH_AT_START` | 0.003 | 0.003 | 1.00x |
| SortedRanges x SortedRanges | `TOUCH_IN_MIDDLE` | 5.374 | 3.029 | 1.77x |
| SortedRanges x RSP | `CLUSTERED` | 6.660 | 0.073 | **91x** |
| SortedRanges x RSP | `CLUSTERED_BLOCKS` | 8.955 | 0.052 | **172x** |
| SortedRanges x RSP | `DENSE_VS_SPARSE` | 9.268 | 1.829 | **5.07x** |
| SortedRanges x RSP | `INTERLEAVED_BLOCKS` | 16.8 | 14.3 | 1.17x |
| SortedRanges x RSP | `INTERLEAVED_KEYS` | 12.2 | 10.3 | 1.18x |
| SortedRanges x RSP | `INTERLEAVED_RUNS` | 19.1 | 15.9 | 1.20x |
| SortedRanges x RSP | `SAME_BLOCK_CONTAINERS` | 14.9 | 13.1 | 1.14x |
| SortedRanges x RSP | `SAME_BLOCK_KEYS` | 16.8 | 18.8 | *0.90x* |
| SortedRanges x RSP | `SEPARATED` | 0.016 | 0.007 | 2.29x |
| SortedRanges x RSP | `TOUCH_AT_END` | 16.8 | 16.7 | 1.01x |
| SortedRanges x RSP | `TOUCH_AT_START` | 0.016 | 0.011 | 1.45x |
| SortedRanges x RSP | `TOUCH_IN_MIDDLE` | 8.921 | 8.130 | 1.10x |
| RSP x RSP | `CLUSTERED` | 0.022 | 0.024 | *0.94x* |
| RSP x RSP | `CLUSTERED_BLOCKS` | 11.9 | 0.062 | **192x** |
| RSP x RSP | `DENSE_VS_SPARSE` | 2.010 | 2.174 | *0.92x* |
| RSP x RSP | `INTERLEAVED_BLOCKS` | 14.0 | 14.6 | 0.96x |
| RSP x RSP | `INTERLEAVED_KEYS` | 6.506 | 6.544 | 0.99x |
| RSP x RSP | `INTERLEAVED_RUNS` | 146.1 | 148.7 | 0.98x |
| RSP x RSP | `SAME_BLOCK_CONTAINERS` | 5.189 | 5.499 | *0.94x* |
| RSP x RSP | `SAME_BLOCK_KEYS` | 9.681 | 9.575 | 1.01x |
| RSP x RSP | `SEPARATED` | 0.003 | 0.003 | 1.00x |
| RSP x RSP | `TOUCH_AT_END` | 15.0 | 31.1 | *0.48x* |
| RSP x RSP | `TOUCH_AT_START` | 0.008 | 0.008 | 1.00x |
| RSP x RSP | `TOUCH_IN_MIDDLE` | 8.335 | 9.633 | *0.87x* |
| **range** | | | | 0.48x to 192x |
| **geometric mean** | | | | 2.22x |

### subsetOf

| Direction | Pattern | Before | After | Change |
|---|---|---:|---:|---:|
| SortedRanges in SortedRanges | `CLUSTERED` | 3.907 | 3.908 | 1.00x |
| SortedRanges in SortedRanges | `CLUSTERED_BLOCKS` | 3.913 | 3.921 | 1.00x |
| SortedRanges in SortedRanges | `DENSE_VS_SPARSE` | 1.228 | 1.272 | 0.97x |
| SortedRanges in SortedRanges | `INTERLEAVED_BLOCKS` | 7.021 | 7.262 | 0.97x |
| SortedRanges in SortedRanges | `INTERLEAVED_KEYS` | 1.396 | 1.407 | 0.99x |
| SortedRanges in SortedRanges | `INTERLEAVED_RUNS` | 4.414 | 4.431 | 1.00x |
| SortedRanges in SortedRanges | `SAME_BLOCK_CONTAINERS` | 7.130 | 7.086 | 1.01x |
| SortedRanges in SortedRanges | `SAME_BLOCK_KEYS` | 4.098 | 4.104 | 1.00x |
| SortedRanges in SortedRanges | `SEPARATED` | 3.858 | 3.866 | 1.00x |
| SortedRanges in SortedRanges | `TOUCH_AT_END` | 7.028 | 7.035 | 1.00x |
| SortedRanges in SortedRanges | `TOUCH_AT_START` | 6.990 | 7.079 | 0.99x |
| SortedRanges in SortedRanges | `TOUCH_IN_MIDDLE` | 7.005 | 7.066 | 0.99x |
| SortedRanges in RSP | `CLUSTERED` | 5.992 | 6.004 | 1.00x |
| SortedRanges in RSP | `CLUSTERED_BLOCKS` | 7.831 | 7.749 | 1.01x |
| SortedRanges in RSP | `DENSE_VS_SPARSE` | 2.214 | 1.802 | 1.23x |
| SortedRanges in RSP | `INTERLEAVED_BLOCKS` | 16.2 | 16.6 | 0.98x |
| SortedRanges in RSP | `INTERLEAVED_KEYS` | 1.885 | 1.877 | 1.00x |
| SortedRanges in RSP | `INTERLEAVED_RUNS` | 7.084 | 7.289 | 0.97x |
| SortedRanges in RSP | `SAME_BLOCK_CONTAINERS` | 10.6 | 10.4 | 1.01x |
| SortedRanges in RSP | `SAME_BLOCK_KEYS` | 8.362 | 8.338 | 1.00x |
| SortedRanges in RSP | `SEPARATED` | 7.079 | 7.092 | 1.00x |
| SortedRanges in RSP | `TOUCH_AT_END` | 16.9 | 16.3 | 1.04x |
| SortedRanges in RSP | `TOUCH_AT_START` | 16.7 | 16.3 | 1.03x |
| SortedRanges in RSP | `TOUCH_IN_MIDDLE` | 16.1 | 16.1 | 1.00x |
| RSP in SortedRanges | `CLUSTERED` | 19.7 | 5.009 | **3.93x** |
| RSP in SortedRanges | `CLUSTERED_BLOCKS` | 15.4 | 6.851 | **2.25x** |
| RSP in SortedRanges | `DENSE_VS_SPARSE` | 4.525 | 4.070 | 1.11x |
| RSP in SortedRanges | `INTERLEAVED_BLOCKS` | 12.8 | 8.043 | **1.59x** |
| RSP in SortedRanges | `INTERLEAVED_KEYS` | 0.009 | 0.010 | *0.90x* |
| RSP in SortedRanges | `INTERLEAVED_RUNS` | 11.6 | 10.2 | 1.13x |
| RSP in SortedRanges | `SAME_BLOCK_CONTAINERS` | 19.8 | 6.287 | **3.14x** |
| RSP in SortedRanges | `SAME_BLOCK_KEYS` | 8.873 | 8.326 | 1.07x |
| RSP in SortedRanges | `SEPARATED` | 11.9 | 6.540 | **1.83x** |
| RSP in SortedRanges | `TOUCH_AT_END` | 12.5 | 8.081 | **1.55x** |
| RSP in SortedRanges | `TOUCH_AT_START` | 12.7 | 7.931 | **1.60x** |
| RSP in SortedRanges | `TOUCH_IN_MIDDLE` | 12.9 | 8.053 | **1.61x** |
| RSP in RSP | `CLUSTERED` | 1.573 | 1.574 | 1.00x |
| RSP in RSP | `CLUSTERED_BLOCKS` | 9.356 | 9.457 | 0.99x |
| RSP in RSP | `DENSE_VS_SPARSE` | 1.254 | 1.255 | 1.00x |
| RSP in RSP | `INTERLEAVED_BLOCKS` | 11.9 | 12.1 | 0.98x |
| RSP in RSP | `INTERLEAVED_KEYS` | 0.009 | 0.009 | 1.00x |
| RSP in RSP | `INTERLEAVED_RUNS` | 104.6 | 104.8 | 1.00x |
| RSP in RSP | `SAME_BLOCK_CONTAINERS` | 2.782 | 2.522 | 1.10x |
| RSP in RSP | `SAME_BLOCK_KEYS` | 5.463 | 5.477 | 1.00x |
| RSP in RSP | `SEPARATED` | 4.704 | 4.797 | 0.98x |
| RSP in RSP | `TOUCH_AT_END` | 11.9 | 12.3 | 0.97x |
| RSP in RSP | `TOUCH_AT_START` | 11.8 | 12.0 | 0.99x |
| RSP in RSP | `TOUCH_IN_MIDDLE` | 11.7 | 11.9 | 0.98x |
| **range** | | | | 0.90x to 3.93x |
| **geometric mean** | | | | 1.14x |

Bold marks 10x or better for `overlaps` and 1.5x or better for `subsetOf`, which is the smaller win the subset walk was
ever going to produce: it cannot skip, because every range of the subject has to be found. Italics mark below 0.95x.

Over the whole matrices rather than these slices: `overlaps` runs 0.48x to 214x with a geometric mean of **1.75x**
across its 216 configurations, and `subsetOf` runs 0.57x to 3.9x with a geometric mean of **1.06x** across its 432.

### The configurations below 0.95x

`subsetOf` has 46: 24 `MISSING_EARLY`, 12 `SUBSET`, 10 `MISSING_LATE`. Most are trivial in absolute terms — the worst
*ratio*, 0.57x, is a `MISSING_EARLY` case going from 0.013 us to 0.023 us, about +10 nanoseconds of iterator allocation
on a path that exits almost at once. The largest *time* difference, `SortedRanges in RSP` `TOUCH_AT_START` at 1024
`MISSING_LATE`, 13.3 us to 16.0 us, is single-fork noise rather than a regression: re-measured at `@Fork(4)` on both
sides it is 16.4 us before and 16.8 us after (0.97x), and the fork-to-fork spread within each leg — 15.9-18.1 before,
16.0-18.2 after — covers that difference entirely. The 13.3 us was the outlier fork, not a baseline the branch fell
off. The two neighbouring patterns re-measured the same way land at 1.02x and 0.98x-1.05x.

`overlaps` has 54. All but one pattern are at 64 ranges per side, where the absolutes are tens of nanoseconds; several
`SortedRanges x SortedRanges` patterns run 0.75-0.90x there, from the galloping search's overhead on short arrays, with
absolutes of 0.22 us against 0.18 us. Two results deserve more than a number.

**`SortedRanges x RSP` `SAME_BLOCK_KEYS` (0.90x).** Both sides hold one key per block in the *same* blocks, so the
block search hits every time. `RspArray.overlapsRange` returns before it reaches `workDataPerThread.get()` when the
blocks alternate; when they coincide every probe goes past that line, and because each probe also lands in a different
block it defeats `keySearch`'s "the next key is often in the span we last found" fast path as well. `OverlapProbe` took
this from 0.72x to 0.95x. `INTERLEAVED_BLOCKS` and `SAME_BLOCK_KEYS` agree on every cheap static signal and differ only
in block collision, so no static guard separates them.

**`RSP x RSP` `TOUCH_AT_END` (0.48x).** A JIT effect that resisted removal. The configuration is bimodal: forks of the
**unchanged** code land at either ~15 us or ~31 us, and after the change every fork lands at ~31. It is not the
algorithm (these shapes never execute the skip, since the adjacency fast path returns first), not the extra read, not
the write to the loop variable, and not an inlining budget — `-XX:FreqInlineSize=800 -XX:MaxInlineSize=120` leaves it
unchanged. A JFR recording says what the slow mode is: `unsignedBinarySearch`, `highBits`, `getSpanIndex` and
`borrowSpanView` all appear as separate leaf frames, so the hot loop's callees are not inlined in that mode. The fast
mode could not be captured under JFR — attaching the profiler puts `main` into the slow mode too, which is itself a
sign of how fragile that mode is — so this is what the slow mode looks like, not a contrast between the two.

## Traps this code has already fallen into

Each of these produced a wrong answer, a crash or a hang at least once while these paths were built; the tests named
cover them.

- **`~i` from an RSP probe means "no keys in the range just tested", not "no keys left".** The span it stopped on can
  still hold keys above that range, so the probe resumes on that span (`spanIdx = i - 1`) and the caller floors its
  skip at its own range end.
- **A probe on an empty array.** `OverlapProbe` stored `spanIdx = -1`, which a second probe passed to `getSpanIndex`,
  indexing `spans[-1]`. Removing the empty-array guard still fails a test with an `ArrayIndexOutOfBoundsException`.
- **A `for`-to-`while` conversion is not mechanical.** The same conversion applied to `subsetOf` as well as `overlaps`
  would have spun forever: `overlaps` advances the index inside the loop body, `subsetOf` does not.
- **Seeking without galloping is a regression.** With a plain binary search over the remaining tail, the seek costs
  `O(log n)` on tightly interleaved input where the old loop stepped once. Any change here has to be measured on
  `INTERLEAVED_KEYS` and `SAME_BLOCK_KEYS`, not only on `CLUSTERED`.
- **A one-shot search is not a resumable one.** `overlapsRange` keeps a plain binary search deliberately; galloping
  from position 0 only adds probes on the way to a far answer.
- **The walked side must not skip past its own untested keys.** `key = Math.max(end + 1, probe.resumeBlockKey())`.
- **An unclosed range iterator marks its row set shared for good.** `overlaps(RowSet.RangeIterator)` and
  `subsetOf(RowSet.RangeIterator)` take ownership and close in a `finally`; the answer is usually reached with ranges
  still unread.
- **`containsRangeFrom` needs the end check.** Dropping it fails `SubsetOfTest` — but an earlier draft of that test did
  **not** catch it, which is why it now includes ranges that start inside one of the superset's ranges and run past its
  end.
- **Two walks need two sets of coverage.** `SubsetOfTest` asserts that its shapes reach **both** subset walks, so the
  coverage it looks like it gives is the coverage it gives.
- **Extra statements in a hot loop are not free.** Normalizing the galloping search's sign inline rather than through
  `packedGallopingSearchPos` cost `overlaps(SortedRanges)` half its speed on some shapes.

`OverlapsTest` and `SubsetOfTest` cross-check against a linear merge of the same ranges **and** against
`ixIntersectOnNew` / `ixMinusOnNew`, over every pair of representations and both argument orders: hand-built shapes,
interleaved combs, single-key overlaps at each interesting position, random trials, shapes large enough to leave the
packed array, multi-block full spans, clusters spread a block per key, and shapes that share blocks but never a key.
Breaking the RSP skip by one position fails three cases.
