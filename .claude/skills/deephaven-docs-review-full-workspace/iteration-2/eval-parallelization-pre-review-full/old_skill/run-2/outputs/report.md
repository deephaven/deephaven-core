## Editorial summary

**Category:** Concept guide. It lives at `docs/python/conceptual/query-engine/parallelization.md`, and the sidebar lists it under Performance only so readers can find it; it is still a concept guide. **Scope:** the Python file only, reviewed as a report with no edits.

The page is well organized. It opens with a quick-reference table, gives a good comparison of `with_serial` and barriers where the reader first meets both, and builds the counter example step by step. The biggest problem is the reader's mental model. The "What does NOT get parallelized" list puts `view`, `update_view` and `lazy_update` in the safe-from-concurrency bucket. It also says Python formulas are "never run concurrently" on standard GIL builds. Both claims are wrong in ways that lead readers to write unsafe code. The second largest problem is configuration detail pasted into the explanations: property names and defaults appear in parentheses in about seven places. **Verdict: needs revision.** The structure is sound; the fixes are targeted changes to claims and to where detail sits.

## Developmental notes

1. **Purpose (Developmental).** Stated in one sentence: *after reading this page, a Python developer can tell when a formula or filter is unsafe to run in parallel, and can fix it with `with_serial`, barriers or `Filter` objects.* Lines 6 and 130 support this purpose, so it is recoverable.
2. **Key message competes with the opening callout (lines 6–11).** The main point is "Deephaven parallelizes for you, and most queries need no changes." It is stated at line 6, but then a Breaking-change IMPORTANT callout plus a "Quick check" come next, before any explanation. They set an alarmed tone before the reader has a model. It is repeated at line 130 and line 343.
   - Fix: keep the callout, but shorten it to two sentences: what changed in version 41 and a link. Let line 6 carry the key message.
3. **The mental model contains two wrong claims.** These are the core claims a reader would take away, handed to the accuracy step first:
   - (a) the page parallelizes across tables, rows and columns;
   - (b) deferred columns (`view`, `update_view`, `lazy_update`) are not parallelized — **wrong**;
   - (c) formulas are assumed stateless by default since version 41;
   - (d) Python-backed formulas never run concurrently on GIL builds — **partly wrong**;
   - (e) `with_serial` orders rows inside one column, and other columns can still run at the same time;
   - (f) barriers order one column relative to another;
   - (g) serial columns wait for each other only when implicit barriers are on (off by default);
   - (h) small tables are computed on one core.

   Claims (b) and (d) are the most expensive errors on the page. See Accuracy findings 1 and 2.
4. **Audience fit.** A few terms are used before they are explained:
   - "update graph" at line 30 (linked at 52);
   - "partitioning columns" at line 329 (never defined);
   - "selectables" at line 75, used before `Selectable` is introduced at line 134.

   Two internal component names are presented as proper nouns: "Operation Initialization Thread Pool" and "Update Graph Processor Thread Pool" (UGP is a legacy name).
5. **Scope.** Several parts are configuration or reference content, not concept:
   - the whole "Implicit barriers" subsection (lines 318–325), which is about a configuration property;
   - most of "Stateful partition filters" (lines 327–331);
   - the thread-pool property paragraphs (lines 83–89).

   These belong in one Configuration section at the end, or behind a link to `conceptual/query-table-configuration.md`.
6. **Progression.** Good overall: mechanism, then safety by default, then control, then how to choose, then takeaways. The one weak point is that decision guidance appears four times: the callout's "Quick check," the Quick reference table, "Choosing an approach," and "Key takeaways." On a 356-line page that is at least one too many; see Structure.

## Accuracy

1. **Deferred columns are wrongly listed as "not parallelized" (line 70).** This is a wrong mental model.
   - `view`, `update_view` and `lazy_update` build `ViewColumnSource`s. Their formulas run later, on whichever thread reads the column. That thread may be a parallel `update`/`where` downstream, and a formula can run again on each read.
   - The engine's own guard proves the danger. In `QueryTable.java` (around lines 2035–2062): `"view and updateView cannot respect barriers"`, and, with the default `statelessSelectByDefault`, `"A stateful column cannot safely be used in a view or updateView."`
   - The PR's own `reference/table-operations/select/view.md:23` says rows "may be evaluated in any order, at any time, and by any thread."
   - Fix: take these three out of the "NOT parallelized" list. Say that deferred columns do no work up front, but their formulas run on the reader's threads, possibly concurrently, so they must be stateless.
   - Related inconsistency: the restriction at line 156 names `view` and `update_view` but leaves out `lazy_update`, even though line 70 groups all three. The engine has no guard for `lazy_update` (see AQ6).
2. **The GIL callout overstates what happens on standard builds (line 75).** It says "on a standard (GIL-enabled) build, they're never run concurrently."
   - Source: `DhFormulaColumn.isParallelizable()` returns `!usesPython || PythonFreeThreadUtil.isPythonFreeThreaded()`. That only disables splitting one column's rows across threads (`SelectColumnLayer`: `canParallelizeThisColumn = ... sc.isStateless() && sc.isParallelizable()`).
   - Running different columns at the same time is controlled separately: `SelectColumnLayer.allowCrossColumnParallelization()` returns `selectColumn.isStateless()`, which is true by default. So two Python columns in one `update` can be scheduled on different threads at once, with the GIL interleaving them.
   - For filters, the claim holds: `ConditionFilter.permitParallelization()` returns false on non-free-threaded Python.
   - Fix: "On a standard build, Deephaven doesn't split a Python formula's rows across threads, but separate Python columns can still run at the same time." Raised as AQ1 for SME confirmation.
3. **The race example's output and explanation (lines 180–190) are implausible and contradict the page.**
   - On a GIL build, A's own rows are never split, so A could not show 0, 1, 5, 4, 9. The page's own caution says this.
   - On a free-threaded build, row splitting starts only at `minimumParallelSelectRows` (1<<22). Rows 0–4 all fall in the first segment, which one thread evaluates in order.
   - "row 4 has `A=4` after row 3 has `A=5`" is off by one against the table (the 5 is in the third row).
   - "no 10-19 visible" means nothing in a five-row excerpt.
   - "`B` not following `A + 1`" states an expectation no correct run meets. Evaluated column by column, B is roughly A + 5,000,000.
   - Author query AQ3.
4. **The Quick reference row "Column A must finish before Column B → Barriers" (line 19) is misleading.** The `ConcurrencyControl.withSerial` javadoc says: "if column B references column A, then the necessary inputs from column A are evaluated before column B." Barriers are needed only for hidden dependencies, such as shared state. Rewrite the row as, for example, "B depends on A through shared state (not a column reference)."
5. **"Implicit barriers" (lines 320–323) mixes up two settings.**
   - `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is a Java field, not something a user sets.
   - The labels "Stateless mode (default)" and "Stateful mode" describe `statelessSelectByDefault`, not this property.
   - Source (`QueryTable.java` ~400): `getBooleanWithDefault("QueryTable.serialSelectImplicitBarriers", !STATELESS_SELECT_BY_DEFAULT)`, so it defaults to false.
   - Say this plainly: off by default; on when `statelessSelectByDefault=false`, or when set explicitly.
   - Source discrepancy for a follow-up: the `ConcurrencyControl.withSerial` javadoc (`table-api/.../ConcurrencyControl.java`) says it defaults "to the value of `QueryTable.statelessSelectByDefault`", which is the opposite of the code (AQ2).
6. **"What gets parallelized" (lines 62–66) is incomplete.** It reads as a complete list. `UpdateBy.java`, `RangeJoinOperation.java` and `ConstructSnapshot.java` also use the operation-initializer or update-graph job schedulers. Either add `update_by` and range joins, or introduce the list as examples ("including").
7. **Several absolute or overstated claims.**
   - Line 93, "By default, Deephaven parallelizes operations that are stateless," implies the engine detects statelessness. It doesn't; it *assumes* every formula is stateless (`STATELESS_SELECT_BY_DEFAULT` → `isStateless()` returns true).
   - Lines 97–99 define stateless as "same output for the same input." The page's own first example uses `randomGaussian` and `randomInt`, which fail that definition yet run in parallel correctly. The javadoc definition is narrower: "one row does not depend on the order of evaluation for another row."
   - Line 148, "on a single thread": the javadoc guarantees only "never be invoked concurrently with itself."
   - Line 83, "dividing the rows among CPU cores": this happens only for eligible columns above the threshold.
   - Line 244, "both read `counter = 0`": this is one possible interleaving, not a certainty.
   - Line 50, "`updateThreads` being greater than 1, which is the default": the default is `-1`, which resolves to `availableProcessors()` (`PeriodicUpdateGraph.java:140`). It is greater than 1 only on multi-core hosts.
8. **The barrier example (lines 246–278) is correct by contract, but you can't see it working.** Both columns are serial, so `analyzer.anyParallelColumns()` is false and `QueryTable.java:1825` picks an `ImmediateJobScheduler`. Every run gives 0–9 / 10–19 with or without the barrier. The barrier still carries the guarantee, since the engine promises no ordering without it, so keep it. Just don't imply that removing it visibly breaks this 10-row example.
9. **Broken inbound anchor (re-verify step, corpus-wide scan).** `docs/python/how-to-guides/predicate-pushdown.md:13` and the Groovy copy of that page link to `.../parallelization/#controlling-concurrency-for-select-update-and-where`. This PR removes that heading; the old heading was "Controlling Concurrency for `select`, `update` and `where`". Point those links at `#controlling-execution-order`. The other inbound anchors all still resolve:
   - `#serialization` from `query-table-configuration.md`, `view.md` and `update-view.md`;
   - `#barriers` from `Filter.md` and `Selectable.md`.

   Any restructuring must keep both anchors.
10. **Links.**
    - All internal links resolve at f2ef483084.
    - Four of them resolve only because this PR adds the targets: `Selectable.md`, `Filter.md`, `crash-course/parallelization.md`, and the `update.md`/`where.md` `#serial-execution` anchors. Don't split those files into a separate PR.
    - `Barrier` and `ConcurrencyControl` link to the pydoc even though `reference/query-language/types/Barrier.md` and `ConcurrencyControl.md` exist.
    - External links need a manual check: the pydoc URLs, the Python free-threading how-to, and the Java 11 `availableProcessors` page.
11. **Verified as correct.**
    - The version 41 flip of both `statelessFiltersByDefault` and `statelessSelectByDefault`: commit 8ea55b6a1d, first tag go/v41.0.0.
    - Thresholds: `minimumParallelSelectRows` is 1<<22 (about 4.2M). The `where` rule is `numberOfRows / 2 > parallelWhereRowsPerSegment` (1<<16). `minimumParallelSortRows` is 1<<20, and `parallelSort` is a real property.
    - Both thread-pool properties default to `-1`, which resolves to `availableProcessors`.
    - The APIs and imports exist: `Selectable.parse`, `with_serial`, `with_declared_barriers` and `with_respected_barriers` (each accepts one `Barrier` or a sequence), `deephaven.filters.is_null`/`not_`, `deephaven.concurrency_control.Barrier`, and `update`/`where` accepting `Selectable` and `Filter`.
    - Partition-filter behavior (`PartitionAwareSourceTable.whereImpl`): an unmarked partitioning filter is given priority whatever the stateless default; a serial filter is not.
    - The serial-filter "absolute reordering barrier" wording matches the javadoc.
    - "Each barrier can only be declared by one operation" matches the javadoc.
12. **Groovy sibling (outside the requested scope, heads-up only).** At f2ef483084 it repeats the `view`/`updateView`/`lazyUpdate` "not parallelized" bullet and the implicit-barriers wording, so findings 1, 5 and 6 apply there too.

## Structure

1. **Configuration detail sits inside the explanations (Level of abstraction).**
   - Property names and defaults appear in parentheses or inline at lines 50, 66, 83, 87, 124, 126 and 320–323.
   - The Python GIL/free-threading caution sits under "How parallelization works > Within a single table."
   - Fix: add one "Configuration" section before "Related documentation", or link to `query-table-configuration.md`. Move the GIL caution to a Python-specific note in "Controlling execution order." Word the explanations at their own level, for example "once a table is large enough to be worth splitting."
2. **The intro's three categories don't match the headings (Parent/child terminology).** Line 26 says there are three ways (across tables, across rows, across columns), but there are only two child headings: "Across tables" and "Within a single table." Either state it as "two ways: across tables, and within a table (by rows and by columns)," or use three headings.
3. **"What gets / does NOT get parallelized" mixes kinds of items and sits under the wrong heading.**
   - The "NOT" list mixes operations (`view` and the others), a modifier (`with_serial`) and a scheduling state ("Operations waiting for dependencies"), which is not an operation at all.
   - The list covers `sort` and update-graph scheduling but sits under "Within a single table."
   - Fix: move both lists up to "How parallelization works." Keep only operations in them, and cut the "waiting for dependencies" bullet.
4. **"Stateful partition filters" is an orphaned aside (lines 327–331).** It is a niche topic with an undefined term ("partitioning columns"). Nothing in the Quick reference or "Choosing an approach" connects to it.
   - Fix: make it a short note under "Serial filters," with a link to the partitioned-table docs, or move it to the Configuration or advanced section. Keep the lead-in sentence, since it's a good bridge.
5. **Decision guidance is repeated and the counter setup is repeated (Length and repeated-example fatigue).**
   - The page is 356 lines. Decision guidance appears four times: the callout's Quick check, the Quick reference, "Choosing an approach," and "Key takeaways."
   - The counter function is defined three times (lines 162, 194, 246).
   - Fix: merge "Choosing an approach" into "Key takeaways." Its link back to the Quick reference already acknowledges the overlap. After the first definition of the counter, say "using the same `get_and_increment_counter` as above," and use a `test-set` to reuse it.
6. **The Quick reference uses terms before they're defined (line 13).** "Barriers" and "implicit barriers" are used before line 137. Keep the table early, since that's the right call, but add a one-line forward pointer: "`with_serial` and barriers are explained in [Controlling execution order]."
7. **Re-verify step.** No edits were applied, so nothing was moved. If the author applies items 1, 3 or 4, preserve the `#serialization` and `#barriers` anchors, since other pages link to them. Also re-check the six anchors this page links to itself, listed under Accuracy finding 9's scan.

## Examples

1. **Serial filters (lines 219–232).** The example doesn't show what its lead-in names. The lead-in says to use `Filter` objects "when a filter has stateful side effects," but `is_null("X").with_serial()` has none, so it teaches the syntax but not the reason. Use a filter with a real side effect, such as a Python predicate that appends to a list or counts rows.
2. **Counter race, then fix (lines 162–211).** The fix answers a different scenario. The broken version has two columns, A and B; the fix has one column, ID. With two serial columns and implicit barriers off, A and B can still interfere with each other, which is exactly what the barrier section solves next.
   - Fix: make the first pair a single column in both the broken and fixed versions, and let the barrier section introduce the second column.
   - Replace the invented output table with a described outcome, or with real output labeled with the Python build it came from.
3. **Multiple barriers (lines 291–311).** There is no shared state, so the barriers change nothing you can see. The lead-in "when columns have different dependencies" is not what the code shows. Either add shared state, or label the block as showing only how to declare barriers.
4. **Test cost.** The fixed counter runs 5,000,000 Python calls in a snapshot-tested block. Serial ordering doesn't depend on table size, so a much smaller `empty_table` gives the same lesson with a fast test. The `skip-test` on the race block is justified because its output is non-deterministic.
5. **No examples for two concepts.** "Implicit barriers" and "Stateful partition filters" are explained without code. That is acceptable if they move to Configuration; otherwise add a short example of each.
6. **Across-tables example.** It uses `randomGaussian` and `randomInt`, which conflicts with the page's own definition of stateless (see Accuracy finding 7). Either fix the definition or use deterministic formulas here.

## Style

Mechanical checks all passed: no dot-prefixed method names in prose, no empty `()` in prose, no `[here]` link text, no curly quotes, em dashes spaced correctly, and headings in sentence case.

- **Configuration detail in parentheses (about 7 places):** for example, line 50 "(This depends on `PeriodicUpdateGraph.updateThreads`...)" and line 66. These overlap with Structure finding 1.
- **Made-up labels "across tables / across rows / across columns" (about 8 uses, lines 26–87):** the style rule names these exact phrases. Prefer "concurrent table updates," "concurrent row calculations" and "concurrent column calculations," or define the label once.
- **"Stateless" and "thread-safe" used as if they mean the same thing (3 places):** line 17 ("Thread-safe, no shared state"), lines 93–99, and line 337. Pick one meaning and keep it.
- **Internal names capitalized as proper nouns (2):** "Operation Initialization Thread Pool" and "Update Graph Processor Thread Pool" (lines 83, 87). Use "the operation-initialization thread pool" and "the update graph's thread pool."
- **Link issues (3):**
  - line 50, the link text "Thread pools" doesn't match its target heading;
  - line 153 links `with_serial` to `update.md#serial-execution`, while every other mention links to `Selectable.md#with_serial`;
  - the first mentions of `Barrier` and `ConcurrencyControl` link to the pydoc instead of the internal reference pages.
- **Passive voice (about 4):** lines 70, 83 and 87 ("This is handled by..."). For example, "The operation-initialization thread pool handles this."
- **Future tense (1):** line 9, "will now produce" → "now produces."
- **One idea per paragraph (2):** line 87 is a single sentence of about 70 words, and line 331 makes four separate claims. Split both.
- **Code readability (1):** line 112 uses the character literal `' '` for string concatenation. Use a backtick string `` ` ` `` so readers don't mistake it for a date-time literal.

## Author queries

- AQ1 [Within a single table, line 75]: On a GIL build, can two Python-backed stateless columns in one `update` run at the same time on different threads? Source (`allowCrossColumnParallelization` returns `isStateless()`) suggests yes, which would make "never run concurrently" wrong.
- AQ2 [Implicit barriers, line 320]: The `ConcurrencyControl.withSerial` javadoc says `serialSelectImplicitBarriers` defaults to the value of `statelessSelectByDefault`, but `QueryTable.java` uses `!STATELESS_SELECT_BY_DEFAULT`. Which is intended? One of them needs a source fix.
- AQ3 [Example: a counter needs serialization, lines 182–190]: Did the sample output come from a real run? On which Python build, and with which thread settings?
- AQ4 [Stateful partition filters, line 331]: A serial filter anywhere in the list stops later partition filters from being given priority, too (`PartitionAwareSourceTable.whereImpl`). Should the page say this?
- AQ5 [Breaking-change callout, line 9]: Is "Deephaven 40 and earlier assumed all formulas required sequential processing" exact? Before version 41 the implicit-barrier default was true, and filters were stateful by default too. Should the callout name filters?
- AQ6 [Serialization, line 156]: `lazy_update` has no guard against serial or barrier columns, unlike `view` and `update_view`. Is it intended to be allowed?

## Strengths

1. The comparison of `with_serial` and barriers (lines 139–144) comes right where the reader first meets both mechanisms. Keep it.
2. The Quick reference table near the top gives readers a map before the details. Keep it early; just fix the rows noted above.
3. The comments inside the barrier example explain *why* ("serial (protect counter) + declares barrier (must finish first)"), not just what the code does, and the follow-up line 285 correctly says when `with_serial` isn't needed.