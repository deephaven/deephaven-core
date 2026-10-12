# Join hashing: key ids, key values and slot tags

The cross join, the natural join, the as-of join and the symbol table combiner find each distinct key's state through
one family of generated, open-addressed hash tables in `io.deephaven.engine.table.impl.join`. This note describes that
design as it stands: dense key ids that callers index their own state by, the one hasher that keeps a value in the slot
instead, and the one-byte tag that every key id slot carries. It also records the measurements that shaped it. DH-23922
introduced the key id hashers for the cross join and the symbol table combiner. DH-24013 moved the natural join and the
as-of join onto them, and added the key-to-long hasher and the tag. Every benchmark here compares against upstream main
after DH-23922, where the cross join already uses key ids.

| | |
|---|---|
| **Problem** | The natural and as-of joins each had their own generated hash table and kept per-key state in arrays indexed by hash slot. Every rehash moved that state, anything that referred to a key by slot had to follow the key when it moved, and every state array paid for the empty slots. |
| **Design** | A hasher gives each distinct key a dense `int` id that never changes. Callers keep per-key state in their own arrays indexed by id. |
| **Hashers** | A static key id hasher (built once, add-only), an incremental key id hasher (removes keys, reuses their ids, grows incrementally), and a static key-to-`long` hasher for the one caller whose only per-key state is a `long` that every probe reads. |
| **Slot layout** | The key columns, a one-byte tag and the id. A search scans the tags and compares a slot's key only when its tag matches. |
| **Result** | Against upstream main before DH-24013, at 25% and 75% hit rates from 100K to 10M keys: 58 of 72 natural and as-of join cases at least 5% faster, none slower. The cross join, already on key ids, is within 3.5% of upstream in seven of eight cases and 7% faster in the eighth. |

## Why ids

In upstream main before DH-24013, the natural and as-of joins each generated their own open-addressed hash table, 27
generated classes for the natural join alone, and kept the join's per-key state in arrays indexed by hash slot. For the
right-ticking natural join, that was a right row key, a left row set and a tracker cookie beside every key. That layout
has three costs.

- **A rehash moves all of it.** The ticking natural joins moved 28 bytes per entry at every doubling, and the
  right-ticking as-of join 25, two of them object references.
- **Whatever refers to a key by slot must follow it.** The natural join's modified-slot tracker had to follow every key
  that a rehash moved, through `moveTableLocation`.
- **State arrays are as long as the table, not the key count.** At the 0.75 maximum load factor, an array indexed by
  slot is at least a third longer than the number of keys and, just after a doubling, up to 2.7 times as long. At 10M
  keys in 2^24 slots it is 68% longer.

A dense id fixes all three. The hasher moves only keys, tags and ids when it grows; nothing outside it ever refers to a
slot; and per-key state lives in arrays as long as the number of keys. The cross join and the symbol table combiner
already worked this way after DH-23922.

## Key ids

`KeyIdHasher` gives each distinct key an `int` id. Ids start at zero and are dense, and a rehash moves keys, tags and
ids between slots without changing any id. Keys are built or probed a chunk at a time:

- `build` finds or inserts the key of every row and reports each row's id and a status, `ADDED` for a key the build
  inserted and `FOUND` for one already present.
- `probe` reports only ids, with `NULL_ID` for a key that is not in the table. It never changes the table.
- Both take either a `RowSequence` and key column sources, reading the keys a chunk at a time and handing the results to
  a consumer, or key chunks the caller already holds, with a reusable `Context` that holds the result chunks.
- `idCapacity()` bounds the ids handed out, so a caller can size its arrays.

A caller keeps per-key state in arrays indexed by id. Almost all are block-based array sources, such as
`ObjectArraySource` and `LongArraySource`, which grow by adding blocks of 2,048 entries, so their data is never copied
as keys arrive. Where a caller needs to find a row's key again, it records the row's id, never its slot.

### Static and incremental

| | `StaticKeyIdHasherTypedBase` | `IncrementalKeyIdHasherTypedBase` |
|---|---|---|
| **Keys** | Added only. | Added and removed. |
| **Ids** | Handed out in insertion order. | A removed key's id goes on a free list, and later insertions take freed ids first, so ids stay below the peak number of live keys. |
| **Growth** | Doubles by a full rehash as soon as an insertion takes it past its load factor, so a table sized for its keys never grows. | Grows into a new main table, while the old one becomes an alternate that each later build drains, examining `REHASH_SLOTS_PER_ENTRY` (3) alternate slots per entry it builds; no single build pays for the whole rehash. `buildWithFullRehash` rehashes all at once, for an initial build that nothing waits on. |
| **Removal** | None. | `remove(ids)` turns each key's slot into a tombstone, leaving the key in place so that a probe for it stops there, and frees the id. `idToSlot` records where each id's key lives, in the main or the alternate table. |
| **Freezing** | `complete()` is optional; after it, a build that is given any rows throws. | None. |

`buildWithFullRehash` refuses to run once any key has been removed: a full rehash copies tombstones in slot order, so it
could place a key's tombstone ahead of a live entry for the same key.

### Key to value

`StaticKeyLongValueHasherTypedBase` is the exception. It maps each distinct key to a `long` stored in the key's slot,
for a static set of keys, and a probe reads the value from the slot where it finds the key, with no further load. A
build gives a new key the value of its row; what it does with a key that is already present is set by its `RepeatedKey`:
keep the first value, keep the last, or replace it with a mark value and remember that a key was marked (`anyMarked()`).
The slot's value doubles as its empty marker (`NULL_LONG`), so this hasher has no tag.

It exists for the natural join built from a static right table, whose only per-key state is the right row key, read by
every left probe. With a key id, each probe found the id and then read the row key from a second array, a dependent load
that usually missed the cache. Before this hasher, that made left-ticking builds and re-probes 7 to 12% slower than
upstream, which kept the row key in the slot too. With it, re-probes and left-ticking builds are even with upstream or
up to 7% faster. The `MARK` mode is how that join notices duplicate right keys: the duplicate check runs only if a key
was marked.

Ids remain the right choice wherever state is larger than one `long`, changes after the build, or is referred to from
elsewhere, as per-row records and redirections are.

## Slot layout and the tag

A key id hasher keeps three kinds of array, all indexed by slot: one per key column, a `byte` tag and an `int` id. The
incremental hasher has a second set for its alternate table while one is draining.

| Tag | Meaning |
|---|---|
| `EMPTY_TAG`, the null byte (−128) | No key. A new tag array is filled with it. |
| `TOMBSTONE_TAG` (−1) | A removed key, still in place. Incremental hasher only. |
| 0 to 127 | A key, tagged with bits 24 to 30 of its hash. |

A search scans the tags. It compares a slot's key only when the slot's tag is the key's tag, or the slot is a tombstone,
and it reads the id only once it has found the key. In the generated incremental hasher for a `long` key:

```java
while (!isStateEmpty(tagValue = mainTag.getUnsafe(tableLocation))) {
    if ((tagValue == tagFor(hash) || isStateDeleted(tagValue)) && eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
        if (isStateDeleted(tagValue)) {
            searchAlternate = false;
            break;
        }
        ids.set(chunkPosition, mainId.getUnsafe(tableLocation));
        found = true;
        break;
    }
    tableLocation = nextTableLocation(tableLocation);
}
```

The tag pays off on misses, which dominate a probe at a low hit rate. Before it, the id itself was the slot's state, so
a miss scanned an `int` array four times the size of the one-byte state upstream's right-ticking as-of join scanned, and
compared the key of every occupied slot it passed. Now a miss scans bytes, and an occupied slot's key is compared only
when its tag matches, which happens by chance about once in 128. A hit reads three arrays instead of two, but the tag,
the key and the id are all addressed by the slot alone, so once the core predicts a match it can request all three at
once. A check of as-of join probes at 90% and 100% hits found no cost. One build does pay: with both sides ticking at
10M keys and 75% hits, the natural join build is 11 to 15% slower with the tag than without it, in two side-by-side
runs, though still 8.6% faster than upstream; at 25% hits the tag costs it nothing.

The tag costs one byte per slot: 16 MiB at 10M keys, which fit in 2^24 slots. The slot is 13 bytes for a `long` key,
against the 16 to 28 bytes of the slot layouts it replaced.

The bits come from the top of the hash because the low bits choose the slot. A single column's hash is never negative,
but a hash that combines several columns can be, so `tagFor` masks the tag to seven bits; it would otherwise collide
with the empty and tombstone tags.

## Generated code

`TypedHasherFactory` generates one hasher per key type from three configurations, with code fragments from
`TypedKeyIdFactory` and `TypedKeyValueFactory`. The hashers for a single key column of each primitive type and of
`Object` are pregenerated, with a `TypedHashDispatcher` that picks one, into `impl/join/typed/statickeyid/gen`,
`impl/join/typed/inckeyid/gen` and `impl/join/typed/statickeylongvalue/gen`. Hashers for several key columns are
generated and compiled at runtime.

The tag is the factory's `stateMatch` option: the condition a slot's state must meet before the generated search
compares its key. Only the key id configurations set it, so every other generated hasher is unchanged. To regenerate the
pregenerated hashers after changing the factory or a fragment:

```sh
./gradlew replicateTypedHashers
```

## Who uses which hasher

Each manager picks its hasher by which inputs tick and which side it builds from. The helpers choose the manager;
`JoinControl` chooses the build side and the initial table size, which is 4,096 slots unless a data index gives the key
count.

| Manager | Used for | Hasher | Built from | State kept by id |
|---|---|---|---|---|
| `SymbolTableCombiner` | Static joins of symbol table columns | Static key id | The symbol tables | None: the id is the combined symbol id. |
| `StaticChunkedCrossJoinStateManager` | Cross join, both static | Static key id | The smaller side | Right row sets. |
| `LeftOnlyIncrementalChunkedCrossJoinStateManager` | Cross join, left ticking | Static key id | Right | Right row sets. |
| `RightIncrementalChunkedCrossJoinStateManager` | Cross join, right ticking | Incremental key id | Left, and right when both tick | Left and right row sets, tracker cookie. |
| `StaticKeyLongValueNaturalJoinStateManager` | Natural join built from a static right side: left ticking, or both static with the right smaller | Key to long | Right | None: the slot holds the right row key, or a mark for a duplicate. |
| `StaticKeyIdNaturalJoinStateManager` | Natural join, both static, built from the left or its data index | Static key id | Left | Right row key, in a flat array grown by copying. |
| `RightIncrementalKeyIdNaturalJoinStateManager` | Natural join, right ticking | Static key id | Left or its data index | Left row set, right row key, tracker cookie. |
| `IncrementalKeyIdNaturalJoinStateManager` | Natural join, both ticking | Incremental key id | Right, then left | Left row set, right state, tracker cookie. |
| `StaticKeyIdAsOfJoinStateManager` | As-of join, right static | Static key id | Left or right | Left and right row sets. |
| `RightIncrementalKeyIdAsOfJoinStateManager` | As-of join, right ticking | Incremental key id | Left, and right when both tick | Left and right row sets or segmented sorted arrays, state byte, tracker cookie. |

- **Removal.** Three managers remove keys and reuse their ids: the cross join and the as-of join when both sides tick,
  through `releaseEmptySlots` and `releaseEmptyBuckets`, and the natural join when both sides tick, through
  `removeDeadIds`. Each resets a key's state when a build reports its id as `ADDED`, since the id may have been another
  key's.
- **Per-row records.** Only the static natural join built from the left and the cross joins record an id per row: the
  first for each left row, in an `IntegerArraySource`, and the cross joins in a row redirection from row key to id. The
  other managers rebuild row output from their per-id row sets, or write right row keys straight into the left rows'
  redirection.
- **Completion.** The static key id and key-to-long hashers are completed once built, except in `SymbolTableCombiner`,
  which builds and probes the same combiner.
- **Shared helpers.** `CrossJoinRightRowSets` keeps the right row sets for all three cross joins, as builders that it
  converts to row sets once, in place. `KeyIdJoinContext` builds and probes key chunks for the two ticking natural
  joins.

## How dense ids made the joins faster

The measurements are in the next section; these are the mechanisms behind them. They are inferred from the code and from
which benchmarks moved, and none was isolated by an experiment of its own.

- **A rehash moves 13 bytes per entry, not 25 to 28.** The ticking builds start at 4,096 slots and double until every
  key fits, nine times for a million keys. The key id hasher moves an 8-byte key, its tag and its id; per-key state sits
  in block-based arrays that grow without copying. Natural join builds with the right side ticking are 52 to 58% faster
  at 1M and 10M keys, as-of join builds 19 to 69%. At 100K keys the table doubles three fewer times, and the
  right-ticking natural join build gains nothing.
- **No object references in the slot arrays.** The as-of join kept two row set references in every slot, so every rehash
  moved references that the garbage collector tracks. Its builds with only the right side ticking gain the most, 50 to
  69%.
- **State arrays as long as the key count.** Per-key state no longer pays for empty slots, which at a load of 0.6 is 68%
  of every state array.
- **Ids in arrival order.** Keys that arrive together get consecutive ids, so their state is adjacent and a batch of
  them reads it in a contiguous run, which the hardware prefetches; upstream read it at random slots. The natural join's
  key churn runs 43 to 68% faster, and the as-of join's update with both sides ticking is 31 to 56% faster, which fits
  the rows that benchmark replaces having consecutive ids. Updates scattered across a table's keys would not see this.
- **Nothing to follow when a key moves.** The natural join's tracker no longer follows keys that a rehash moves, because
  ids never move.

Ids cost something too, and the design answers each cost:

- **A hit pays a dependent load from id to state.** Where the state is one `long` that every probe reads, the
  key-to-long hasher keeps it in the slot. Elsewhere the load replaces a slot-indexed read rather than adding to one, or
  reads state that is now dense and smaller.
- **A miss scanned 4-byte ids.** The tag fixed it.
- **With the tag, a hit reads a third array.** It cost the natural join build with both sides ticking at 10M keys and
  75% hits 11 to 15% against the hasher without the tag. At that size none of the arrays fits in cache, and three
  quarters of the build's rows are hits.
- **Memory moves rather than disappears.** Estimated from the slot and state layouts, at 10M keys the ticking natural
  joins use 3 to 11% less memory than upstream. The static natural join built from the left, whose per-key state is a
  single `long`, uses about 11% more: it pays for the id and the tag and gains little from dense state.

## Benchmarks

### Method

JMH average time, 3 forks, on an Apple M1 Max with 10 cores and 64 GB, JDK 21 with a 16 GB heap so that object pointers
stay compressed. Each benchmark ran at one key count on upstream main and then on the branch before the next started,
each run waited for two minutes of at least 65% CPU idle, and CPU idle was sampled throughout. A case slower with an
interval that did not overlap upstream's, or by 5% or more, would have run a second time; none was. The baseline for
every comparison is upstream main after DH-23922 merged. The natural and as-of join runs compare upstream main at
3c29c12bf7 with DH-24013 at 5786106123, overnight on 2026-10-11.

The natural and as-of join benchmarks hold the hit rate steady at every key count. Each side holds the key count of
distinct keys, of which 25% or 75%, and so that share of each side's rows, occur on the other side; each side's rows are
shuffled with its own seed. A side has 1M rows, or one per key above 1M, and a natural join's right side has one row per
key. Update benchmarks remove and re-add 100K rows per cycle.

```sh
./gradlew :engine-benchmark:jmhRunNaturalJoinLeftReprobe
./gradlew :engine-benchmark:jmhRunNaturalJoinBuild
./gradlew :engine-benchmark:jmhRunAsOfJoinUpdate
./gradlew :engine-benchmark:jmhRunAsOfJoinBuild
./gradlew :engine-benchmark:jmhRunNaturalJoinChurn
./gradlew :engine-benchmark:jmhRunCrossJoin
```

The tables give the change in time against upstream main, negative when DH-24013 is faster. ¹ marks a result within
noise or under 5%.

### Natural join

| Case | 100K, 25% | 100K, 75% | 1M, 25% | 1M, 75% | 10M, 25% | 10M, 75% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Left rows re-probed, static right | −6.7% | −4.1%¹ | −1.1%¹ | −4.3%¹ | 0.0%¹ | −5.2% |
| Build, both static | −6.7% | −5.4% | −28% | −17% | −36% | −25% |
| Build, left ticking | −6.6% | −6.3% | −3.7%¹ | −4.4%¹ | −1.8%¹ | −3.8%¹ |
| Build, right ticking | +0.2%¹ | −0.4%¹ | −57% | −58% | −55% | −52% |
| Build, both ticking | −12% | −11% | −54% | −44% | −30% | −8.6% |

The static build builds from the smaller side, and from the left when the sides are equal, as they are at 1M and 10M
keys. That build is presized for the left row count, and it probes every right key, three quarters of which miss at 25%
hits; the tag makes it 17 to 36% faster, against 8 to 23% without the tag. The left-ticking build and the re-probe use
the key-to-long hasher, and so does the static build at 100K keys.

`NaturalJoinChurnBenchmark` measures one update cycle on a window of 500K right keys that slides by 10K keys per cycle,
through a key space 2 or 16 times the live keys, in ms per cycle:

| Case | Upstream | DH-24013 | Change |
| --- | ---: | ---: | ---: |
| Both ticking, key space 2x | 6.50 ± 0.27 | 3.71 ± 0.14 | −43% |
| Both ticking, key space 16x | 7.27 ± 0.42 | 3.79 ± 0.15 | −48% |
| Right ticking, key space 2x | 5.69 ± 0.14 | 2.17 ± 0.12 | −62% |
| Right ticking, key space 16x | 6.78 ± 0.28 | 2.20 ± 0.09 | −68% |

### As-of join

| Case | 100K, 25% | 100K, 75% | 1M, 25% | 1M, 75% | 10M, 25% | 10M, 75% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Update, left rows replaced | −5.6% | −6.6% | −23% | −10% | −18% | −0.2%¹ |
| Update, right rows replaced | −8.4% | −9.3% | −11% | −3.2%¹ | −2.6%¹ | −0.5%¹ |
| Update, both sides replaced | −6.8% | −6.3% | −54% | −36% | −56% | −31% |
| Build, both static | −7.4% | −7.9% | −49% | −50% | −43% | −44% |
| Build, left ticking | −5.7% | −5.4% | −37% | −19% | −33% | −26% |
| Build, right ticking | −10% | −7.8% | −61% | −50% | −69% | −66% |
| Build, both ticking | −6.1% | −7.2% | −38% | −36% | −48% | −56% |

Before the tag, three of the updates with one side ticking were 5 to 14% slower than upstream at 25% hits, at 1M and 10M
keys. At 90% and 100% hits, where almost every probe hits and reads all three slot arrays, a check of the update with
the right side ticking, at 2 forks, ran 7% faster than upstream at 1M keys and within noise at 10M.

### Cross join

The cross join already used the key id hashers in the baseline, so DH-24013 changes it only through the shared hasher
code, slot tag included. The build benchmarks join 1M rows on each side, with keys drawn from a key space of 100K, 1M or
10M values; the sliding window moves a window of keys through a key space 2 or 16 times the live keys. In ms per
operation:

| Benchmark | Upstream | DH-24013 | Change |
| --- | ---: | ---: | ---: |
| Build, both static, key space 100K | 75.6 ± 1.7 | 77.3 ± 1.5 | +2.2%¹ |
| Build, both static, key space 1M | 126.6 ± 3.0 | 124.2 ± 2.2 | −1.9%¹ |
| Build, both static, key space 10M | 102.9 ± 1.4 | 103.0 ± 0.8 | +0.1%¹ |
| Build, both ticking, key space 100K | 219.3 ± 2.4 | 226.9 ± 13.1 | +3.5%¹ |
| Build, both ticking, key space 1M | 243.1 ± 2.4 | 226.2 ± 4.0 | −7.0% |
| Build, both ticking, key space 10M | 185.8 ± 3.6 | 179.3 ± 6.8 | −3.5%¹ |
| Sliding window, key space 2x | 13.30 ± 0.15 | 13.45 ± 0.07 | +1.1%¹ |
| Sliding window, key space 16x | 13.81 ± 0.53 | 14.09 ± 0.30 | +2.0%¹ |

The cross join and the natural join churn ran on 2026-10-11, against upstream main at 3c29c12bf7, with the same code as
the overnight run, including the slot tag. The static build at a 100K key space and both sliding windows were 4.3 to
9.5% slower on their first run and within noise on their second, which the table gives; the both-ticking build at a 1M
key space also ran twice, because one upstream fork of its first run was disturbed. Seven of the eight cases are within
3.5% of upstream, as unchanged logic should be.

## Open questions

- The tag's cost on hit-heavy builds at 10M keys might be avoided by reading the id alongside the key once the tag
  matches, or by keeping the tag and the id in one array; neither is tried.
- The as-of join and the cross join with only the right side ticking use the incremental hasher, though with a static
  left they never remove a key or build after the initial build. The static hasher would serve them, as it serves the
  natural join with only the right side ticking.
- Probes are read-only, so a large probe could be split across threads by chunk ranges, each with its own context. The
  natural join's probe of a static right table, which writes each left row's redirection and nothing else, is the
  natural first candidate; builds assign ids in order and would need partitioning by hash.
