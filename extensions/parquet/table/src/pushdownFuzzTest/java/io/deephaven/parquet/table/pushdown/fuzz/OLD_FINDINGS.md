# Pushdown fuzzer findings (DH-23557)

Everything here was found by the bench in this directory. This file is the handoff record; each
finding notes whether it is still open. Finding 1 is **fixed** (DH-23602); the rest are open.

Replay any seed with:

```bash
./gradlew :extensions-parquet-table:pushdownFuzzTest -PforceTest=true \
    -DPushdownFuzzer.maxTableSize=1000 -DPushdownFuzzer.dumpOnMismatch=true \
    "-DPushdownFuzzer.seeds=<seed>"
```

A case seed is **not** usable as a `baseSeed` (case seeds come from `new Random(baseSeed).nextLong()`),
so always replay with `seeds`. A seed also only reproduces at the `maxTableSize` it was found at, since
that bound is consumed while generating the case — every seed here needs `maxTableSize=1000`. At the
default `100000` only 3 of the 18 reproduce, which makes `testOpenFindingSeeds` look misleadingly
green.

Note that `failFast` defaults to `true`, so add `-DPushdownFuzzer.failFast=false
-DPushdownFuzzer.maxFailures=1000` to survey a whole run rather than stopping at the first mismatch.

Baseline: 300 cases, `maxTableSize=1000`, `baseSeed=0`, ~30 s, no coverage gaps. This was **15 failing
cases** when the findings below were first recorded, and is **18** as of the 2026-09-09 merge from
`main` — finding 1's seed now passes, and finding 7 records four seeds that the older run did not flag.
That merge also brought DH-23488 ("Correct parquet statistics pushdown and restore lost pruning"),
which rewrote the pushdown handlers, so the count is not attributable to any single change.

---

## 1. `merge` propagated `SORTED_COLUMNS_ATTRIBUTE`, turning a true sort claim false — wrong rows

**Status:** **FIXED** — DH-23602, "Don't propagate SortedColumns through merge" (#8488), merged to `main`
2026-09-09. Confirmed on this branch after merging up: all of `MergeSortedAttributeFindingTest` passes
and `merge(t, t)` now reports no sorted-columns attribute.
**Reduced:** `MergeSortedAttributeFindingTest` (no longer `@Ignore`d; now a passing regression test).
**Severity was:** high — silent wrong results, in memory, from three lines of ordinary API.

`TableTools.merge` copied the sorted-columns attribute from its constituents to the merged result, but
the concatenation of two sorted tables is not sorted. Sorted-column pushdown then binary-searched
against the false claim and `where()` returned the wrong rows — both **dropping** matching rows and
**returning** non-matching ones.

No unsafe call was involved and no Parquet, nulls, or refreshing table were needed. `SortOperation` tags
every `sort()`/`sortDescending()` result itself, so the entire reproduction was:

```java
final Table t = TableTools.newTable(TableTools.intCol("Col0", 5, 3, 1)).sort("Col0");
final Table merged = TableTools.merge(t, t);   // merged is [1,3,5,1,3,5], claimed Col0=Ascending
merged.where("Col0 <= 3");                     // 2 rows; the correct answer is 4
```

Measured before the fix (each verified by a test method):

| sorted table | filter | returned | correct |
| --- | --- | --- | --- |
| `sort("Col0")` on `[5,3,1]` | `Col0 <= 3` | **2** | 4 (two matching rows dropped) |
| `sortDescending("Col0")` | `Col0 <= 3` | **5** | 4 |
| `sortDescending("Col0")` | `Col0 == 3` | **4** | 2 |
| `sort("Col0")` | `Col0 == 3` | **4** | 2 |
| with a null, `sortDescending` | `Col0 == null` | **4** | 2 |
| with a null, `sort` | `Col0 == null` | **1** | 2 |

`SortedColumnsAttribute.withOrderForColumn` documents this consequence ("range and match filters will
silently return incorrect ... results with no error or warning") — but documents it as the price of an
*unvalidated caller assertion*. Here the caller asserted nothing: `sort()` made a true claim and
`merge` invalidated it on their behalf. That is what made propagation the defect.

**The fix:** `PartitionedTableImpl.computeSharedAttributes` drops `SORTED_COLUMNS_ATTRIBUTE` whenever
there is more than one constituent. A single-constituent `merge(t)` is genuinely sorted and keeps it.
The engine-side regression test is `PartitionedTableTest.testMergeSortedColumnsAttribute`, which also
covers the `partitionBy().transform(sort)` shape.

Merged tables no longer advertise sortedness, which gives up the table-level binary search for
in-memory constituents in exchange for correct results. Per-constituent sortedness is not currently
consulted during union pushdown (`UnionSourcePushdownFilterContext` builds its matchers without
`SortedColumnPushdownManager.wrap`), so recovering that performance is separate follow-up work.
Parquet-backed constituents are unaffected: their binary search runs at the region level off per-file
metadata sort columns.

`FuzzLayout.AVOID_KNOWN_MERGE_SORTED_ATTRIBUTE_DIVERGENCE`, which kept the fuzzer from generating
merge-plus-declared-sort combinations while this was open, has been removed along with its guard, so
those combinations are generated again.

*A Parquet-based variant was the original reduction (write a sorted table, `readTable().coalesce()`
republishes the order from file metadata, then merge). It is strictly more complicated and has been
replaced by the in-memory form above; fuzzer seed `-7423979211207825555` is that shape.*

---

## 2. Pre-epoch `LocalDateTime` with a sub-second part cannot be read back from parquet

**Reduced:** `PreEpochLocalDateTimeFindingTest` (`@Ignore`d).
**Severity:** high — data loss on ordinary values; not pushdown-specific.

Writing succeeds; reading fails with
`java.time.DateTimeException: Invalid value for NanoOfSecond (valid values 0 - 999999999): -1`.

Confirmed to affect *any* pre-epoch value with a non-zero sub-second part — `1900-06-15T12:30:00.123456789`
as much as `1969-12-31T23:59:59.999999999`. Pre-epoch whole seconds and all post-epoch values are fine
(verified by the test's control case). The epoch-nanos reconstruction needs `floorDiv`/`floorMod`;
truncating `/` and `%` yield a nano-of-second of `-1`.

The value is excluded from `FuzzType.LOCAL_DATE_TIME` so this does not mask pushdown findings.

---

## 3. Rename + duplicated column: `where` fails on the disk table but succeeds in memory

**Not reduced.** Reproducible by seed.
**Severity:** medium — a query that works on an in-memory table throws on the parquet-backed one.
**Seeds (5):** `8540064508133314173`, `8922140309403778699`, `-4605199251911937283`,
`8576325184258344286`, `7496982466862244149`

`FormulaCompilationException: ... Cannot find variable or class dup0_ColN` on the disk table only, where
`dup0_ColN` is a column added by `updateView("dup0_ColN = ColN")` over a renamed column. The in-memory
oracle resolves it; the disk table does not.

Suspected cause (unverified): on an uncoalesced `SourceTable`, `renameColumns`/`updateView` produce a
`DeferredViewTable`, whose `where` splits filters and pushes some below the view — so a filter
referencing a view-only column can be handed to a source that does not have it. Worth checking
`DeferredViewTable` filter splitting against renamed + duplicated columns.

Related, same cluster, also disk-only failures:
- `TableInitializationException` from `where` — seeds `428667830982598836`, `-4788457857850447244`
- `AssertionFailure: asserted newName != null, instead newName == null` — seed `6513657358736753182`.
  This one is an internal assertion in name mapping, and it fires even under the
  all-pushdown-disabled toggle profile, which points at context construction rather than at a
  pushdown action.

---

## 4. Parquet sorting-column metadata is never translated out of parquet name space

**Root cause found and reduced:** `PermutedRenameSortedColumnFindingTest` (`@Ignore`d).
**Severity:** high — silent wrong results (rows dropped), from `readTable` + a filter, and reproducible with all
pushdown disabled.
**Seed:** `-6688467811848818630`

`ParquetTableLocation.initialize` sets

```java
sortingColumns = SortColumnInfo.sortColumns(tableInfo.sortingColumns());
```

directly from the file's Deephaven `TableInfo`, so `TableLocation.getSortedColumns()` returns names in **parquet**
space. Both of its consumers read them as **table**-space names. `addColumnNameMapping` makes the two spaces differ,
and neither consumer applies `readInstructions`:

1. `SourceTable.doCoalesce` (`SourceTable.java:337`) calls
   `SortedColumnsAttribute.setOrderForColumn(resultTable, sc.column().name(), order)` — publishing the parquet name as
   the coalesced table's `SORTED_COLUMNS_ATTRIBUTE`. That is a user-visible false claim, and it drives the table-level
   `SortedColumnPushdownManager` and `AbstractRangeFilter`'s binary search.
2. Every `ParquetColumnRegion*.estimatePushdownAction` / `performPushdownAction` (e.g.
   `ParquetColumnRegionDouble.java:96,135`) compares `firstSortedColumn.column().name()` against
   `ctx.filterColumnToManagerColumnName().getOrDefault(col, col)`, whose output is *manager* (table) space. Its
   `// Need to handle column renames` comment is accurate about the `renameColumns` rename and silent about the
   read-time one.

Each drops rows on its own. The reduction's `rangeFilterWithAllPushdownDisabled` isolates the first, and shows this
is **not only a pushdown defect**: with every pushdown switch off, rows are still dropped, because
`AbstractRangeFilter.filter` consults `SortedColumnsAttribute.getOrderForColumn` and binary-searches with no pushdown
flag gating it at all. In the fuzz seed only the second consumer is reachable, because `postRead=MERGE` and `merge` no
longer propagates the sorted attribute (finding 1 / DH-23602) — which is why disabling either
`DISABLE_WHERE_PUSHDOWN_SORTED_COLUMN_LOCATION` or `DISABLE_WHERE_PUSHDOWN_MERGED_TABLES` makes that seed pass while
disabling statistics, dictionary and data-index pushdown does not.

**Why a permutation was required to see it, and why the reduction attempts failed.** Two ingredients must coincide,
and the recorded attempts each had only one. First, the file must actually record a sorting column: `sortForWrite`
calls `sort()`, `SortOperation` tags its result, and `ParquetTableWriter` (line 175) writes that tag into `TableInfo`
— so the metadata is written even under `SortMode.ATTRIBUTE_AFTER_READ`, which `declaresFileMetadata()` reports as
*not* declaring file metadata. Reductions that dropped the sort dropped the bug. Second, the rename must be a
permutation: under a uniform rename the untranslated parquet name matches no table column, the sorted action declines,
and the answer is right by accident. Only a permutation leaves the stale name valid — and pointing at a different,
unsorted column.

The six-line reduction, with no fuzzer machinery, merge, `head`, data index, page size or toggle profile:

```java
// storage: A sorted ascending, B unsorted. The file records sorting column "A".
ParquetTools.writeTable(TableTools.newTable(
        TableTools.intCol("A", 1, 2),
        TableTools.doubleCol("B", -0.0, -1.0)).sort("A"), file);

// rotate the names: table A is now storage B (the unsorted doubles), table B is storage A
final Table disk = ParquetTools.readTable(file, new ParquetInstructions.Builder()
        .addColumnNameMapping("A", "B")
        .addColumnNameMapping("B", "A")
        .build());

disk.coalesce();                  // claims SortedColumns={A=Ascending} — A is [-0.0, -1.0]
disk.where("A <= -1.0").size();   // 0; the correct answer is 1
disk.where("A == -1.0").size();   // 0; the correct answer is 1
```

**Suggested fix.** In `ParquetTableLocation.initialize`, map each sorting column's name through
`readInstructions.getColumnNameFromParquetColumnNameOrDefault` so `getSortedColumns()` returns table-space names.
That is the only consumer-visible place holding the `ParquetInstructions`, the rest of the location's API is already
table-space (`makeColumnLocation` does exactly this translation in the other direction), and one change corrects both
consumers. `TableLocation.getSortedColumns()`'s javadoc should then state the name space — its silence is what let the
producer and both consumers disagree. Note `SortColumnInfo` records only the first sorted column, so this is one
mapping per location, not a hot path.

Neither pre-existing rename test can catch this: `ParquetTableFilterTest.flatPartitionsColumnRenameTest` and
`flatPartitionsInstructionColumnRenameTest` rename every column with a uniform `_renamed` suffix, which is precisely
the shape under which the untranslated name resolves to nothing and the wrong code returns the right answer.

---

## 5. Exception-type divergence between the two paths

**Severity:** low — both sides fail, but differently. Still worth a look: an internal assertion is not
an appropriate user-facing error.

- Seed `-6914829020992303508`: memory raises `ClassCastException`; disk raises
  `AssertionFailure: asserted newName != null` (a rename-map internal assertion).
- Seed `7911583618620128154`: memory `IllegalArgumentException` vs disk `NumberFormatException`.
- Seeds `5492728113478971232`, `1579821865212908206`: memory throws (including
  `ClassCastException: String cannot be cast to Character`) where **disk succeeds** — the disk path
  silently tolerates a type error the memory path rejects. Note
  `ParquetTableLocation.pushdownDataIndex` swallows all exceptions (TODO DH-19443), which is a
  plausible route.
- Seed `4173799138601369497`: the oracle's own `select()` fails — a read defect, related to finding 2.

---

## 6. Write-path robustness issues found incidentally

Not pushdown bugs; the bench works around each so they do not mask pushdown findings. Each workaround
is commented at its site with this rationale.

| Issue | Bench workaround |
| --- | --- |
| An explicit `RowGroupInfo` on a **zero-row** write throws `IllegalArgumentException: Number of groups must be at least 1, got: 0`. Empty files beside populated ones are a shape worth testing. | `FuzzLayout.write` omits row-group info for empty slices |
| Writing an **empty partitioned** table throws `ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0`. | partitioned layout requires a non-empty table |
| A `LocalTime` **partitioning** column formats to `Col0=00:00:02`; the colons make an invalid path and the write fails with `Failed to create URI from relative path`. `PartitionFormatter`/`PartitionParser` both claim support. | `FuzzType.LOCAL_TIME.partitionable()` returns `false` |
| `Instant`/`LocalDateTime` within one second of the `long`-nanos boundary overflow on conversion (`Converting -9223372037 seconds to nanos would overflow`) because seconds are multiplied before the sub-second part is added, even when the final value is in range. | `FuzzType.SAFE_MIN/MAX_EPOCH_NANOS` stay one second inside |
| A partitioned write rejects an explicit data index on a partitioning column (`Cannot add index on partitioning column`) — reasonable, since those get an automatic `PartitioningColumnDataIndex`, but worth confirming it is intended. | index columns are chosen after the partitioning column is known |

---

## 7. Seeds surfaced by the 2026-09-09 merge from `main` — not yet triaged

**Not reduced.** Reproducible by seed; recorded in `OPEN_FINDING_SEEDS` as 7a–7d.

Three of these are merge-plus-declared-sort shapes that the fuzzer could not generate while finding 1
was open (`AVOID_KNOWN_MERGE_SORTED_ATTRIBUTE_DIVERGENCE` suppressed them); removing that guard made
them reachable. The fourth is unrelated to merge. None is a recurrence of finding 1 — the merged table
correctly claims no sort order in all of them.

**7a — silent wrong results, `or(range, !=)` over a sorted + indexed column.** Seed
`1681357320861610709`. `SINGLE_FILE`, `sort=BOTH/Descending`, `postRead=MERGE`, `selection=HEAD`,
`renames=TABLE/CHAINED`, filter `or(Col3 >= fuzzP4, Col2 != fuzzP5)` over a sorted+indexed `LocalDate`
`Col2` and an indexed `BigDecimal` `Col3`. Disk returns **7** rows, memory **6**. The extra row has
`Col3 = 8.09598` (not `>= 55.5658`) and `Col2 = 1970-01-01` (not `!= 1970-01-01`), so it matches
neither disjunct: the disk path over-returns a non-matching row. This is the most serious of the four
and the one to reduce first.

**7b — memory throws where disk succeeds.** Seed `6249197149364475148`. Memory raises
`ClassCastException: String cannot be cast to Boolean` from a `` `ÿ` <= Col2 <= `ÿ` `` range filter;
disk does not. Same shape as finding 5's third bullet, with a different pair of types.

**7c — the oracle's own `select()` fails under a permuted rename.** Seed `5702961989472887051`.
`Select([Col3, Col2, Col0, Col1])` throws
`ClassCastException: ResettableWritableShortChunk cannot be cast to WritableDoubleChunk` on a
`TABLE/PERMUTATION` rename that rotates a `double`/`long`/`LocalTime`/`short` set of columns. Note the
case runs under `ALL_PUSHDOWN_DISABLED`, so this is not a pushdown defect; it is the permuted-rename
cluster of finding 4 reached through `select` rather than through `where`, and it means the bench's own
oracle is unreliable for this shape.

**7d — `Instant` partitioning column produces an invalid partition path.** Seed
`-2281078010550439077`. A partitioned write fails with
`Failed to create URI from relative path: Col0=2023-11-14T22:13:21Z/` /
`URISyntaxException: Illegal character in scheme name at index 4`. This is exactly finding 6's
`LocalTime` partitioning issue for a second type: the colons in the formatted value make an invalid
path. `FuzzType.LOCAL_TIME.partitionable()` is already `false` as a workaround; `Instant` is not, so
either it needs the same workaround or -- better -- `PartitionFormatter` needs to escape or reformat
values containing colons.

---

## Bench self-validation still to do

The plan calls for injecting a fault per axis and confirming the bench catches it — the acceptance test
for each generator. Findings 1 (now fixed) and 2 already demonstrate the bench catches real bugs, but the
per-axis injections (statistics, rename direction, sorted direction, extreme values, dictionary nulls,
emptiness, barriers/serial, the execution-loop `retain` pair, partitioning) have not been run.
