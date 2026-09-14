# Finding 16 — **bench defect**: the bench claimed a table-wide sort order that partitioning had destroyed

**This is a defect in the fuzzer, not in Deephaven.** It produced *false positives*: two of the four
failures in the sweep that found it were the bench asserting something untrue and then correctly
being told a different answer.
**Severity:** high, for the bench — a false positive in a correctness fuzzer is worse than a miss,
because it costs triage time and erodes trust in the whole corpus.
**Status:** **FIXED.**
**Fuzzer seeds (2, both now passing):** `-1220343102263136052L`, `-3258625118121555365L`

## Symptom

```
filtered results differ: Result table has size 39 vs. expected 13
  disk attributes: {SystemicTable=true, SortedColumns=Col1=Ascending, AddOnly=true}
filter=Col1 < -1
```

with `layout=PARTITIONED`, `sort=ATTRIBUTE_AFTER_READ/Ascending`, and — decisively —
`toggles=ALL_PUSHDOWN_DISABLED` still failing. A wrong answer with every pushdown switch off points
at either the read path or the bench, and here it was the bench.

## Root cause

`FuzzLayout.sortForWrite` sorts the whole table before a partitioned write, and says why:

```java
// For a partitioned layout, sorting globally first is what makes a per-partition declaration
// truthful: partitionBy preserves the source's relative row order within each constituent,
// so every partition of a globally sorted table is itself sorted.
```

That reasoning is correct, and the per-file metadata each partition carries is true. But
`applyAfterRead` then declared the attribute on the **whole read-back table**:

```java
if ((sortMode == SortMode.ATTRIBUTE_AFTER_READ || sortMode == SortMode.BOTH)
        && sortColumn != null
        && postRead != PostRead.MERGE) {
    table = SortedColumnsAttribute.withOrderForColumn(table, sortColumn.resultName(), sortOrder);
}
```

The read-back table is the partitions concatenated **in partition-key order**, which is not the sort
order. In the failing case `Col1` really was ascending within each `Col0` partition — `-1, -1, 1,
32767, 32767` for `Col0 = -2` — and then started over at `-2147483646` for `Col0 = -1`. Globally it
is not sorted at all.

`SortedColumnsAttribute.withOrderForColumn` validates nothing; its javadoc is explicit that a false
claim makes "range and match filters ... silently return incorrect ... results with no error or
warning". So the engine did exactly what it was told, and the bench had told it something false.

## Fix

[`FuzzLayout.applyAfterRead`](../FuzzLayout.java) excludes the partitioned layout:

```java
&& fileLayout != FileLayout.PARTITIONED
```

`FLAT_MULTI` is unaffected: `sortForWrite` sorts globally, `write` slices that order into
`table_00000…` files, and the flat layout reads them back in name order, so the concatenation is the
sort order.

## Why this is not "suppressing a failure"

The campaign's ground rule is that the bench generates every shape it can and every resulting failure
gets triaged and fixed, not worked around. This guard is not an exception to that, for the same reason
the existing `postRead != PostRead.MERGE` guard beside it is not:

`SortedColumnsAttribute` is an **unvalidated caller assertion**. When the bench sets it, the bench is
the caller making the claim. A claim that is false makes every downstream answer legitimately
unpredictable, so a mismatch proves nothing about Deephaven — it only proves the bench lied. There is
no defect hiding behind this guard, because there is no correct behaviour to compare against.

The distinction from a real suppression is worth stating: had this been the *engine* deriving a
table-wide sort claim from partitioned files on its own, that would have been a genuine high-severity
defect — that is precisely findings 9 and 11. Here the engine derived nothing; the bench asserted it.

## Verification

- Seeds `-1220343102263136052L` and `-3258625118121555365L` pass.
- The sweep that produced them had 4 failures; 2 were this, and the other 2 are genuine pushdown
  defects still under investigation (a sorted-column action with `NaN`, and a data-index action with
  heavy nulls under nested negation).
- The guard is narrow: only the table-wide declaration for `PARTITIONED` is dropped. Per-file sorting
  metadata is still written and still exercised, so partitioned sorted-column pushdown remains under
  test — which is the shape findings 9 and 11 came from.
