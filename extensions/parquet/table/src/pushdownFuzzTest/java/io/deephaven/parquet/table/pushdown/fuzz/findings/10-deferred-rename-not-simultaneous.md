# Finding 10 — a deferred `renameColumns` that swaps or rotates names produced the wrong data

**Severity:** high — silent wrong results from `readTable(...).renameColumns(...)`, and the same three
lines give different answers in memory and on disk.
**Status:** **FIXED**, with regression test.
**Repro test:** `extensions/parquet/table/src/test/java/io/deephaven/parquet/table/DeferredRenameSimultaneityTest.java`
**Fuzzer seeds (5, all now passing):** `5201278404043255708L`, `2423783905725303439L`,
`5559549332320180280L`, `-8144732105314013200L`, `5702961989472887051L`
**Share of the run that found it:** 4 of 36 failures. Seed `2423783905725303439L` runs under
`ALL_PUSHDOWN_DISABLED`, so this is **not** a pushdown defect.

## Symptom

```java
ParquetTools.readTable(dir).renameColumns("A = B", "B = A").coalesce();
//    B | A                in memory, and correct:   B | A
//    1 | 1                                          1 | x
//    2 | 2                                          2 | y
//    3 | 3                                          3 | z
```

Column `A` lost its values entirely and duplicated `B`'s. The fuzzer met the loud version, because it
follows the rename with a `merge`:

```
ClassCastException: ResettableWritableObjectChunk cannot be cast to WritableIntChunk
    at IntChunkPage.fillChunkAppend
    at UnionColumnSource.fillChunkFromMultipleSources
```

`UnionSourceManager` types its columns from `constituentDefinition()` — taken from the *uncoalesced*
table, and correct — and fetches sources from each coalesced constituent, which had the wrong types.
The exception is therefore a consequence, not the defect.

## Root cause

`RedefinableTable.renameColumns` lowers a rename into `SourceColumn`s, and
`DeferredViewTable.applyDeferredViews` applied them with:

```java
result = result.view(List.of(SelectColumn.copyFrom(deferredViewColumns)));
```

**`view` is sequential and a rename is simultaneous.** `view` evaluates its columns in order, each
against a name space that already holds the ones before it. For `(B = A, A = B)` the second column
resolves `B` to the value the first just produced, so both end up with the original `A`.

Measured directly, on `[A=1,2,3 (int); B=x,y,z (String); C=1.5,2.5,3.5]`:

| operation | result |
| --- | --- |
| `renameColumns("B = A", "C = B")` | `B=[1,2,3]`, `C=[x,y,z]` — correct, simultaneous |
| `view("B = A", "C = B")` | `B=[1,2,3]`, `C=[1,2,3]` — `C` read the new `B` |
| `renameColumns("A = B", "B = C", "C = A")` | correct rotation |
| `view("A = B", "B = C", "C = A")` | `C` read the new `A` |

So the defect covers swaps, chains and rotations — any rename that reassigns a name another of its
columns reads from. `QueryTable.renameColumns` is simultaneous and was always right, which is why the
answer depended on whether the table had been coalesced.

### The same mistake in the filter path

`DeferredViewTable.applyFilterRenamings` builds its outer→inner map in the same loop-with-lookback
style:

```java
final String sourceName = renames.getOrDefault(innerName, innerName);
```

so for a swap the second column resolved through the entry the first had just added, and
`where("A == \`x\`")` on the swapped table pushed down as `A == \`x\`` against the *source* `A` (ints)
and returned **0 rows**. Two places had to agree about the view's semantics, and both composed
sequentially.

## Fix

[`DeferredViewTable`](../../../../../../../../../../../engine/table/src/main/java/io/deephaven/engine/table/impl/DeferredViewTable.java)

A single `SimultaneousRename` classifier, used by both sites, recognizes the shape that needs
simultaneous treatment. `applyDeferredViews` then replays it with `Table.renameColumns` — which *is*
simultaneous — followed by a projection of the deferred columns' own names to fix their order and drop
anything the view excludes. That projection is a `view` of bare names, each mapping to itself, so it
cannot reintroduce the hazard. `applyFilterRenamings` builds its map straight from the original name
space for the same shape.

The classifier is deliberately narrow, and each condition earns its place:

- **All columns must be `SourceColumn`s.** Anything computed genuinely wants sequential evaluation.
- **Some name must be reassigned while another column still reads it.** Without that, `view` is
  already correct, so behaviour is untouched for every ordinary rename.
- **Every source name must exist in the inner table.** This is what separates a rename from a
  deliberately sequential view. `updateView("T = X", "X = Y", "Y = T")` swaps through a temporary and
  *depends* on being evaluated in order; its `T` is produced by an earlier entry rather than read from
  the inner table. The pre-existing `TestParquetTools.testColumnSwapping` covers exactly that, and
  caught an earlier version of this fix that lacked this condition.
- **One canonical target per source column**, so a source read by several deferred columns is renamed
  once and the duplicating case (finding 7's shape, `updateView("Dup = Renamed")`) keeps working.

## Verification

- `DeferredRenameSimultaneityTest`, 10 tests. Each asserts that the deferred rename produces exactly
  what the same rename produces **in memory**, comparing definitions and every value — the property
  that actually matters. Covers the swap, a chain, a rotation, a swap leaving a third column alone, a
  plain rename to fresh names, a single masking rename, rename-then-`merge` for both a swap and a
  rotation, a filter over a swapped column, and a duplicating `updateView` that must stay on the
  sequential path.
- Pre-existing `TestParquetTools` passes, including `testColumnSwapping`, `testChangedThenRenamed`,
  `testMultipleIdenticalRenames` and `testOverloadAsRename` — the sequential-view cases this fix must
  not disturb.
- Full `:engine-table:test`, `:extensions-parquet-table:test`, `:extensions-parquet-base:test`,
  `:engine-time:test` and `:codec-builtin:test` pass.
- All 5 fuzzer seeds pass and join `INTERESTING_SEEDS`, including `5702961989472887051L`, which
  `OLD_FINDINGS.md` recorded as its finding 7c and attributed to the bench's own oracle being
  unreliable. It was this defect.
