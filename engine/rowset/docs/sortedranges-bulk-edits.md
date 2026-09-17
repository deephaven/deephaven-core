# SortedRanges bulk insert and remove

How `SortedRanges` applies a whole `RowSet` of additions or removals, which of its five strategies each shape of input
takes, and why. Written alongside DH-23690, which added the planned strategies after the merge-based one turned out to
be 40-68x slower than key-by-key insertion for the small arguments incremental `naturalJoin` produces.

| | |
|---|---|
| **Entry points** | `WritableRowSet.insert(RowSet)` / `remove(RowSet)` → `SortedRanges.ixInsert` / `ixRemove` → `insertImpl` / `remove(OrderedLongSet)` |
| **Strategies** | append, individual edits, planned edits, merge, convert to `RspBitmap` |
| **What decides** | whether the argument lies past our last key, how many ranges it has, how big it is relative to us, and whether the result still fits our packing and capacity |
| **Benchmarks** | `RowSetSmallInsertBench`, `RowSetSmallRemoveBench` (`jmhRunRowSetSmallInsert`, `jmhRunRowSetSmallRemove`) |
| **Tests** | `SortedRangesBulkInsertTest` (randomized against range-by-range `insertRange` / `removeRange` on an independent copy, plus the named corner cases below) |

## The representation the strategies edit

A `SortedRanges` is one sorted array of *entries*. A non-negative entry is a key that starts a range or stands alone
as a single; a negative entry is the (negated) end of the range whose start precedes it. So `{3, 10..12, 20}` is stored
as `3, 10, -12, 20`: four entries for three ranges. Ranges are never adjacent or overlapping; `{10} {11..12}` is not a
valid state, it is `10..12`. The array is packed as shorts or ints relative to an offset (`SortedRangesShort`,
`SortedRangesInt`, via `SortedRangesPacked`) or as raw longs with no offset (`SortedRangesLong`). Each packing has a
capacity. The merge-based paths choose the result's packing afresh, by span, density and capacity, as described under
the merge below. The append path keeps our packing, repacking only into a narrower one when our capacity runs out, and
converts straight to an `RspBitmap` when the result lies outside a short or int packing's span from our offset or when
neither our packing nor a narrower one can hold it; it never tries a wider packing. In the merge, a result becomes an
`RspBitmap` when the packing its span selects cannot hold it, or when the merge judges it dense; it does not try a wider
packing for room.

Two consequences shape everything below. First, the number of *ranges* and the number of *entries* differ by up to
2x, and the code is explicit about which one it is counting. Second, an edit that changes no entry count can still
change the encoding: inserting `11` into `{10} {12}` leaves two entries but turns two singles into the range `10..12`.

A set may be shared copy-on-write (`RefCountedCow`). Every strategy either writes in place because it holds the only
reference, or produces a new set and leaves the shared one untouched. Shared sets are read concurrently by `UpdateGraph`
threads, so no strategy keeps scratch state on the set itself.

## The decision tree

For `insert(other)`, in the order the code asks the questions:

```
ixInsert(added)
├─ added is empty ................................... return this (or EMPTY if we are empty too)
├─ we are empty ..................................... return a shared reference to added, no copy
├─ added is a SingleRange ........................... addRange: one in-place open/coalesce
│                                                     (null → convert to RspBitmap)
├─ added is an RspBitmap ............................ convert to RspBitmap, or the two together
└─ added is a SortedRanges .......................... insertImpl(added):
   ├─ our last key < added.first .................... APPEND: mergeAppend copies added onto our tail
   │                                                  (null when added.last is outside a short or int
   │                                                  packing's range from our offset, or no capacity of our
   │                                                  type holds the result and it cannot be repacked
   │                                                  smaller → convert to RspBitmap directly; the merge
   │                                                  below is not tried)
   └─ otherwise
      ├─ added does not fit our packing ............. MERGE (see below)
      ├─ editIndividually(added) .................... INDIVIDUAL: addRangeInternal per range
      ├─ planEdits(added) ........................... PLANNED: insertPlanned
      └─ otherwise, or INDIVIDUAL/PLANNED returned null
         └─ MERGE: union into the long work buffer, then pack by span, density and
            capacity: shorts when the span fits a short; otherwise a bitmap when the
            result is dense; otherwise ints or longs by span; a bitmap when the chosen
            packing has no capacity for the result
```

For `remove(removed)`:

```
ixRemove(removed)
├─ we are empty ..................................... return EMPTY
├─ removed is empty ................................. return this
├─ removed is a SingleRange ......................... removeRange: one in-place cut
│                                                     (null → convert to RspBitmap)
└─ otherwise ........................................ remove(removed):
   ├─ removed is a SortedRanges
   │  ├─ editIndividually(removed) ................. INDIVIDUAL: removeRange per range
   │  ├─ planEdits(removed) ........................ PLANNED: removePlanned
   │  └─ otherwise, or either returned null ......... fall through
   └─ MERGE: intersect with the complement into the long work buffer, repack
      (null on capacity → convert to RspBitmap)
```

A result that comes back empty is normalized to `OrderedLongSet.EMPTY`.

### The two predicates

**`editIndividually(other)`**: true for one or two ranges at any size; beyond that, with `r` the number of ranges in
`other` and `n` our entry count, true when `(r - 2)^2 * n <= 400`, where 400 is the default of
`SortedRanges.individualEditThreshold`, which admits a handful of ranges on a tiny set. Ranges are counted from their
start entries, stopping as soon as the inequality fails, so the count is cheap. This is where the planned
strategy's fixed cost of a few tens of nanoseconds does not pay: measured, individual edits win for one or two ranges
everywhere and for up to about six ranges on a 20-entry set.

**`planEdits(other)`**: `other.count * 8 <= count`, on entries, where 8 is the default of
`SortedRanges.plannedEditMaxSizeRatio`. A planned edit costs a binary search and a small block
move, about 20 ns; the merge costs about 2 ns per entry of either set. Measured, planning wins while the argument has
up to about a twelfth as many entries as we do and loses from about a quarter, so the boundary sits at an eighth.
Counting entries rather than ranges only sends a range-heavy argument to the merge a little early.

The two predicates are shared by insert and remove; the remove benchmark showed the same crossovers within noise.

## The strategies

### Append

Our last key is below the argument's first key, so the argument goes on the end. `mergeAppend` copies its entries
after ours, coalescing when its first key is adjacent to our last. It fails (`ensureCanAppend` returns null) in two
cases: for a short or int packing, the argument's last key lies outside what the packing can represent from our offset;
or no capacity of our type holds the result and it cannot be repacked smaller. Either way `insertImpl` converts to an
`RspBitmap` at once; the
append and the merge are alternatives, so a failed append does not fall through to the merge's repack. This is the path
every append-only workload takes, including the release phase of an incremental join, and it was never the problem.

### Individual edits

`insertRangesIndividually` walks the argument's ranges and calls `addRangeInternal` for each;
`removeRangesIndividually` calls `removeRange`, the same range-level code a single `insertRange` or `removeRange` runs.
In the case the benchmarks measure, a private set and a range that lands between two entries, each call is a binary
search and one `System.arraycopy` to open or close a gap, moving about half the entries. That block move is cheap, about
0.02-0.05 ns per entry moved, which is why two such moves beat one planned pass with its fixed setup. A contained
range, a coalescing one, a growing array or a shared set each do different work in those calls.

Copy-on-write: `removeRange` handles a shared receiver itself. `addRangeInternal(..., writeCheck)` does too, but
returns the receiver *unchanged and still shared* when the range is already contained, so the loop keeps `writeCheck`
on until a call returns a different object, which is the private copy. Clearing it after the first call let a later
range write into a shared set; `containedFirstRangeKeepsSharedCopyIsolated` covers the sequence.

A null from either loop means the result outgrew the capacity of the set's current packing: an insert added more
entries than it can hold, or a removal split ranges into more entries than before. The loop may already have applied
earlier ranges in place; the caller falls through to the merge, which can repack into another packing, and for which
re-inserting or re-removing them is idempotent.

### Planned edits

Two passes. The first plans every edit without moving anything; the second moves each untouched stretch of entries at
most once, and not at all when the edits before it net to no shift. All scratch state lives in a thread-local
`EditPlan`.

**Plan pass, insert (`insertPlanned`).** For each range `[s, e]` of the argument, in order:

1. Find the first of our entries whose key is at least `s - 1` (`absRawBinarySearch`, from a cursor that never moves
   backward). That entry is one of three things:
   - the *end* of a range that reaches `s - 1` or beyond: it started before `s - 1`, so it touches `[s, e]`;
   - the *start* of a range or single within `e + 1`: it touches `[s, e]`;
   - the start of a range beyond `e + 1`, or nothing at all: `[s, e]` touches none of ours and goes in before it.
2. Absorb every following range of ours that starts within `e + 1` (`absorbTouching`), extending the group's end and
   summing the cardinality the absorbed ranges held.
3. The group is now the old entries `[groupStart, groupEnd)` and the single range they become. A following argument
   range that starts within one key of the group's end joins the same group and absorbs further.
4. When the group closes (`recordInsertGroup`), it is dropped if the old entries already encode exactly its range: a
   contained range changes nothing. The encoding test is exact, not an entry count: two singles bridged by a new key
   keep two entries but become a start and a negative end. Otherwise the edit is recorded: replace
   `[groupStart, groupEnd)` with one piece `[first, last]`.

**Plan pass, remove (`removePlanned`).** For each range `[s, e]` of the argument:

1. Find the first of our entries whose key is at least `s`.
   - If it is a range *end*, `s` falls inside that range, which started before `s`; a left remainder
     `[start, s - 1]` always survives and is the edit's first piece.
   - If it is a range *start* beyond `e`, the argument range holds none of our keys; nothing is recorded.
   - Otherwise it starts a range or single at or beyond `s` that the cut reaches.
2. Absorb every range of ours that starts at or before `e` (`absorbCut`). If the last absorbed range reaches past `e`,
   its right remainder `[e + 1, end]` becomes a piece.
3. A following argument range that starts inside that right remainder carves it: the remainder is shortened to end at
   `s - 1`, and either a new right remainder is added or, when `e` reaches past it, absorption continues. The argument's
   ranges are neither overlapping nor adjacent, so `s` is at least one key past the remainder's first key and a left
   part always survives; the code asserts this rather than handling a case that cannot occur.
4. When the group closes, the edit replaces `[groupStart, groupEnd)` with zero, one or two pieces (or more, when several
   argument ranges carve one of ours).

Both plan passes guard the `+ 1` arithmetic for a range ending at `Long.MAX_VALUE`, which otherwise wraps negative
and stops absorption early.

**Apply pass (`applyPlan`).** The plan knows the net change in entries and in cardinality, and whether any edit grows
or shrinks its entry count.

- In place, when we hold the only reference, the result fits our array, and no edit grows while another shrinks:
  - all edits grow or hold: `applyPlanBackward`, from the last edit, so each stretch moves right before anything is
    written over it;
  - all edits shrink or hold: `applyPlanForward`, from the first edit, so each stretch moves left into vacated space.
- Otherwise into a new set of the same type and offset (`applyPlanToNew`), sized by `capacityForLastIndex`; our own
  array is returned to the array pool when arrays are pooled (`SortedRanges.poolArrays`, off by default) and we owned
  it. Mixed grow-and-shrink edits, a common shape for removals that
  split some ranges and delete others, take this path. If no capacity of our type can hold the result, the strategy
  returns null and the caller falls through to the merge.

Each untouched stretch is moved by one `System.arraycopy` at most, so the pass costs O(n) entries moved plus O(k) edits,
against the O(k log n) searches of the plan pass.

### Merge

The original strategy and still the right one for large arguments and for arguments that do not fit our packing.
`union` (or `intersect` with the complement, for removal) walks both sets through range iterators into a thread-local
long work buffer, then `makeOrderedLongSetFromLongRangesArray` packs the result by span, density and capacity: shorts
when the span from first to last key fits a short and the entry count is within the short capacity; otherwise an
`RspBitmap` when the result is dense (`isDenseLong`: enough entries per 64K block that a bitmap is the better
representation); otherwise ints when the span fits an int, or longs, each subject to its own capacity, beyond which
the result is an `RspBitmap` too. Our own array is reused when we own it and the chosen packing is ours. The merge
visits every entry of both sets, at about 2 ns each, which is what made
it 40-68x slower than key-by-key insertion at two keys into 2000 entries, and it is also what makes it the fastest
choice once the argument is a sizeable fraction of the set: at 500 keys into 2000 the planned pass was 1.6x slower,
at 2000 into 6000 2.3x slower.

The merge is also the fallback for the individual and planned strategies' capacity failures, because it can change the
packing: a dense `SortedRangesLong` caps at 256 entries by default (`SortedRanges.longDenseMaxCapacity`) where the same
content, when its span fits a short, repacked as shorts holds thousands (`SortedRanges.shortMaxCapacity`, 4090 by
default); with a wider span the density branch produces a bitmap instead. The result becomes an `RspBitmap` only when
the merge's packing rules above say so: dense, or beyond the span-selected packing's capacity.

### Convert to RspBitmap

When the merge's packing rules produce a bitmap, or an append's result lies outside our packing's span or fits neither
our packing nor a narrower one, the set becomes an `RspBitmap` and the argument is applied there. This is the terminal
case of every branch; it is not a performance strategy.

## Measured behaviour

Microseconds per bulk insert into a target of scattered single keys; `forAll` is the comparison baseline, the same keys
inserted one at a time through `forAllRowKeys`. The aim was for the bulk call to be no slower than that baseline beyond
the benchmark's own run-to-run noise. Before is the merge for every argument; after is the tree above.

```
 target    k   before    after   forAll
     20    2    0.080    0.040    0.042
     20   20    0.142    0.137    0.265
    200   20    0.458    0.463    0.380
   2000    2    4.233    0.105    0.103
   2000    5    4.531    0.208    0.265
   2000   20    4.555    0.522    0.969
   2000  100    5.329    2.792    5.149
   6000    2   13.006    0.149    0.193
   6000   20   15.151    2.174    4.204
   6000  100   15.381    4.226   12.283
   6000 2000   23.222   23.044      n/a   (merge, via planEdits)
```

Removal shows the same shape. The 20- and 200-entry rows are sub-microsecond operations whose cells moved by up to
about 80 ns between otherwise identical runs; the `200 / 20` cell, for instance, measured between 0.32 and 0.46 µs for
bulk against 0.38 to 0.39 µs for `forAll` across runs of the same build. Differences of that size in those rows are not
evidence either way; the larger rows are well outside it. For the join, accumulation of a slot's row keys per cycle went
from losing to per-key insertion by 84% on 2000-key slots to beating it by 13%.

## Traps this code has already fallen into

Each of these produced a wrong answer or a failing test at least once while the strategies were built; the tests
named cover them.

- **Scratch state on the set.** A shared set is read by several `UpdateGraph` threads at once. Returning two values
  from a helper through instance fields raced and produced `Index -1` failures in the join tests while every rowset
  unit test passed. Scratch lives on the thread-local plan.
- **Copy-on-write through a no-op.** See individual edits above; `containedFirstRangeKeepsSharedCopyIsolated`.
- **Testing "private" sets that are shared.** `RowSetFactory.empty().insert(set)` takes a shared reference to `set`
  rather than copying it, so a test that meant to exercise in-place edits exercised only the copy-to-new path. Private
  copies in tests are rebuilt range by range through a fresh sequential builder.
- **Entry count as a proxy for "unchanged".** Bridging two singles keeps the entry count; compare the encoding.
- **`e + 1` at `Long.MAX_VALUE`.** Wraps negative; every adjacency test guards it.
- **Capacity is per type.** A planned result that outgrows a dense `SortedRangesLong` must fall back to the merge,
  which repacks, rather than to the bitmap.
- **Removing from a set an earlier removal emptied.** `removeRange` on an empty set fails; the individual loop stops
  when the result is empty.
- **The argument is a normalized set.** Its ranges never touch, which is what makes the carve invariant hold and what
  made a proposed test case (`{10} {11..12}`) impossible rather than uncovered.
- **Small arguments are not the only arguments.** The first cut of the planned strategy had no upper bound and was
  up to 2.3x slower than the merge for arguments near the set's own size; `planEdits` is the result. Any change to
  these strategies should be measured across the whole `k` axis of both benchmarks, not only the small end.
- **The argument may be the receiver.** `WritableRowSetImpl` checks `removed == this` for `rowSet.remove(rowSet)`
  itself, but two row sets sharing one inner set copy-on-write reach `SortedRanges.remove` with the receiver as the
  argument. The merge handled that; the individual loop would cut the set it is reading its ranges from. `remove`
  returns the empty set for its own receiver before choosing a strategy; inserting a set into itself is a no-op on
  the individual and merge paths, which are the ones an argument of the set's own size selects.
