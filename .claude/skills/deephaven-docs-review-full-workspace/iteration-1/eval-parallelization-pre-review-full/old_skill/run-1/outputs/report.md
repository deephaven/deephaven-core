<!-- Saved by the orchestrating session from the subagent's returned text; the subagent's own Write was refused. -->

**Two-sentence summary:** The most serious accuracy problems are that the GIL caution wrongly says Python-backed formulas never run concurrently (on a GIL build they still run in parallel across columns and tables), and the counter and barrier examples describe races that the code as written doesn't produce (for example, "B not following A + 1" when the correct output is A=0–9, B=10–19). Separately, one inbound link is broken because this PR renamed a heading (`predicate-pushdown.md` → `#controlling-concurrency-for-select-update-and-where`), `with_serial` is presented as if it serializes shared resources across columns, the doc gets the implicit-barriers default wrong, and structurally the counter example breaks its own thread and the size-threshold caveat arrives about 65 lines after the claims it qualifies.

---

# Full review: `docs/python/conceptual/query-engine/parallelization.md` (DOC-857, snapshot of f2ef483084)

**Category (step 0):** Concept guide. It lives under `conceptual/`, and its sidebar placement under Best practices and troubleshooting → Performance is the documented discoverability exception, not a category override. That means concept-guide weighting: split or duplicated explanations and topic interleaving count more heavily, and an orphaned aside counts less than it would in a reference page.

**Scope:** This covers the Python file only. You put the Groovy sibling out of scope, so the accuracy skill's mandatory cross-language consistency check (step 4a) was **not run**. See the follow-ups at the end.

**Mode:** Report only. I edited nothing. Step 2 proposes structural changes but applies none, so step 3's spot-check re-verification had nothing to re-check. Step 3's corpus-wide inbound-link scan did run, against the f2ef483084 tree, and found a broken inbound link (A3).

**Source of truth:** Engine and Python source in `/Users/margaretkennedy/dhc-skills-chip` (HEAD 01ce57ef51). The engine files that matter here differ from f2ef483084 only in unrelated where-listener and UpdateHelper locking changes. I checked link targets against the f2ef483084 tree, because `Selectable.md`, `Filter.md`, and the Crash Course parallelization page exist there but not at HEAD.

## Accuracy (step 1: deephaven-core-accuracy-check)

### High

**A1. The GIL caution says Python-backed formulas are "never run concurrently" on a GIL build. That's false: they can still run concurrently across columns and across tables.** (line 75)
- **Within a column:** the row-splitting gate is `canParallelizeThisColumn = … && sc.isStateless() && sc.isParallelizable()` (`SelectColumnLayer.java:115-117`). `DhFormulaColumn.isParallelizable()` returns `!usesPython || PythonFreeThreadUtil.isPythonFreeThreaded()` (`:899-905`). So on a GIL build, a Python-backed column is never split across rows.
- **Across columns:** cross-column scheduling is gated only on statelessness: `allowCrossColumnParallelization() { return selectColumn.isStateless(); }` (`SelectColumnLayer.java:657`). `anyParallelColumns()` then selects the `OperationInitializerJobScheduler` (`QueryTable.java:1825-1828`). The UpdateHelper javadoc says so directly: "Layers that do not depend on one another run concurrently on the job scheduler".
- **Effect:** the two default Python columns `A` and `B` in the counter example run on different threads at the same time, interleaving under the GIL. Tables that share a source can also run concurrently on update-graph threads.
- **Fix:** say that on a GIL build Deephaven never splits a single Python-backed column or filter across rows, but separate columns and separate tables can still run at the same time. The caution's second paragraph ("use `with_serial` regardless") is correct and should stay.

**A2. The race example's expected-output reasoning contradicts the engine and the doc's own barrier section.** (lines 180-190)
- **Wrong symptom:** Deephaven evaluates column by column. Even a fully serialized, ordered run gives A = 0…N-1 and B = N…2N-1, as line 280 itself says. So "`B` not following `A + 1`" isn't a symptom of a race.
- **Incoherent symptom:** "gaps (no 10-19 visible)" doesn't make sense when the table shows only 5 rows.
- **Wrong pattern for a GIL build:** `A` is never row-split (A1), so the realistic failure is A and B interleaving: each column increasing, with gaps where the other took values. `A=5` followed by `A=4` within rows 0–4 is only plausible on a free-threaded build. Even there, 5M rows splits into just two segments: `divisionSize = max(MINIMUM_PARALLEL_SELECT_ROWS, …)` with a `1L << 22` minimum (`SelectColumnLayer.java:205-208`).
- **Fix:** rewrite the symptom list and the sample table to match, and say the exact pattern depends on the Python build.

**A3. Broken inbound link (found by the corpus-wide scan in step 3).** `docs/python/how-to-guides/predicate-pushdown.md:13` links to `https://deephaven.io/core/docs/conceptual/query-engine/parallelization/#controlling-concurrency-for-select-update-and-where`. This PR renamed that heading to `## Controlling execution order`, so the fragment no longer resolves. Update it to `#controlling-execution-order` in this PR. All other inbound anchors still resolve: `#serialization` from `view.md`, `update-view.md`, and `query-table-configuration.md`, and `#barriers` from `Filter.md` and `Selectable.md`.

**A4. The implicit-barriers section (lines 320-325) gets the default wrong and names the wrong thing.**
- **Default:** `serialSelectImplicitBarriers` defaults to `!STATELESS_SELECT_BY_DEFAULT` (`QueryTable.java:400-402`). A user who sets `statelessSelectByDefault=false` therefore gets implicit barriers on. The doc says the default is always off.
- **Field vs. property:** `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is a Java static field, not a configuration property.
- **Invented names:** "Stateless mode" and "Stateful mode" appear nowhere in source. The property is a boolean.
- **Scope:** implicit barriers apply only to serial selectables. For filters, serial is always "an absolute reordering barrier" (`ConcurrencyControl.withSerial`).

**A5. `with_serial` alone doesn't serialize access to a shared resource across columns, but several places say or imply it does.** The source contract is "never be invoked concurrently with itself" (`table-api/.../ConcurrencyControl.java`). With implicit barriers off (the default), other serial columns can run at the same time. Every occurrence found in the duplicate-claim sweep:
- Line 9: the callout says "unless you mark it with `with_serial`". This contradicts the doc's own IMPORTANT at line 283.
- Line 20: the Quick reference row for file I/O and logging.
- Line 22: the Quick reference row for non-thread-safe libraries ("Forces single-threaded access").
- Line 148.
- Line 338.
- Line 346.

Fix: qualify each one. `with_serial` stops a single column or filter from running concurrently with itself; if more than one column touches the resource, also use barriers or `serialSelectImplicitBarriers`.

### Medium

**A6. The barrier example's "would race" claims don't hold for the code as written.** (lines 244, 280)
- **Why:** both columns are `with_serial`, so neither is stateless. `anyParallelColumns()` returns false, so the analyzer uses `ImmediateJobScheduler` and runs the layers one after another (`QueryTable.java:1825-1830`; `SelectAndViewAnalyzer.doKickOffWork`).
- **Consequence:** remove the barrier and you still get 0–9 and 10–19 in practice. The barrier is still right, because it's the only *guarantee*.
- **Overstated sentences:**
  - "both read `counter = 0`" misdescribes a race.
  - "Without `with_serial`, rows within each column would also race" is false for 10 rows with a Python UDF.
- **Fix:** reword around the guarantee ("could run concurrently / isn't guaranteed").

**A7. The doc says Deephaven "parallelizes operations that are stateless" (lines 93-99), as if the engine detects statelessness. It assumes it.** With the default setting, `DhFormulaColumn.isStateless()` returns true without looking at the formula (`:889-891`). `ConditionFilter.permitParallelization()` likewise returns `STATELESS_FILTERS_BY_DEFAULT`. The pre-PR text said "the engine assumes that all user expressions are stateless", and this PR dropped that sentence; restore it.

Two smaller wording problems:
- Line 124 says "marked stateless", but Python has no API for that. It should say "serial".
- "Doesn't read … global variables" (line 97) is too strong: reading an immutable global is stateless.

**A8. The line-35 code comment repeats a claim this same PR fixed elsewhere.** "adds a row every second" is the wording that f2ef483084 fixed in the Crash Course, because `TimeTable.refresh` inserts a range of rows based on elapsed time. Use "rows arriving at one-second intervals".

**A9. Row splitting is described without its threshold until the caveat at line 124, and "core" is used where the engine means "thread".**
- Lines 58 and 83 describe rows being divided across cores unconditionally. That only happens at or above `minimumParallelSelectRows` (`SelectColumnLayer.java:205`).
- The work goes to pool threads, not specific cores. The same "core" wording appears at lines 6, 26, 50, 60, 124, and 343.
- Line 124's "on a single core" below the threshold ignores two exceptions: cross-column concurrency still applies, and columns whose result type is `Table` or `RowSet` are split at any size.

**A10. The "What gets parallelized" list (lines 62-66) is incomplete.** Deriving the set from source, these are missing:
- `update_by` (`UpdateBy.java:237-338`).
- `range_join` (`RangeJoinOperation.java:259`).
- Parallel snapshots: `QueryTable.enableParallelSnapshot` / `minimumParallelSnapshotRows` (`ConstructSnapshot.java:1456`).

Either add them or reword the list as examples. ⚠️ Needs SME input on whether `update_by` and `range_join` are worth naming for users.

**A11. The serial-filter example doesn't show the case it motivates.** The lead-in (lines 217-232) says to use serial filters for stateful side effects, but the example serializes `is_null` and `not_`, which have no side effects. Either say it only shows syntax, or use a stateful Python filter.

**A12. The breaking-change callout (line 9) is overstated.**
- "will now produce" should be "can produce".
- It leaves out that version 41 also flipped filters to stateless by default. Commit 8ea55b6a1d is first contained in `go/v41.0.0`, and both prior defaults were false.
- The version numbers themselves check out.

### Low

- **A13. Stateful partition filters (lines 327-331) omit some conditions.** Checked against `PartitionAwareSourceTable.whereImpl` and `isPrioritizablePartitioningFilter`:
  - Refreshing filters and filters using virtual row variables are excluded from the optimization.
  - Once any serial filter appears, every later filter is deferred to row-level evaluation, as is any filter that respects a non-prioritized barrier.
  - Correct: prioritization ignores `STATELESS_FILTERS_BY_DEFAULT`.
- **A14. Line 50: `updateThreads` is "greater than 1" by default.** The default is -1, which means `availableProcessors`, so it's above 1 only on a multi-core host. The thread-pool properties and defaults at lines 83, 87, and 89 are verified.
- **A15. Barriers are missing an ordering rule.** A respected barrier must be declared earlier in left-to-right order, or the engine throws `IllegalArgumentException` (`SelectAndViewAnalyzer.java:196-201`). "Each barrier can only be declared by one operation" is verified.
- **A16. The `with_serial` restriction box (line 156) omits `lazy_update`.** Its Python signature takes only strings (`table.py:1389`).
- **A17. Line 148 says "on a single thread".** The source guarantees only "never invoked concurrently with itself", which line 213 matches. Use that wording.
- **A18. Misnamed column (line 120):** `Squared = sqrt(X)` computes a square root.
- **A19. Link targets.**
  - Line 153: `with_serial` points to `update.md#serial-execution`, while every other `with_serial` link goes to `Selectable.md#with_serial`.
  - Lines 137 and 153 link `Barrier` and `ConcurrencyControl` to external Pydoc, but in-repo `Barrier.md` and `ConcurrencyControl.md` exist at f2ef483084.
  - Every other internal link and in-page anchor resolves.
  - External links (the Pydoc URLs, the Python free-threading how-to, and the Oracle Javadoc) need a manual check.

**Verified with no issue:**
- Every config property name and default: `minimumParallelSortRows` (`1L<<20`), `parallelSort`, `minimumParallelSelectRows` (`1L<<22`, about 4.2M), `parallelWhereRowsPerSegment` (`1<<16`, gated by `numberOfRows / 2 >`), `statelessSelectByDefault`, `statelessFiltersByDefault`.
- All the Python APIs used: `Selectable.parse`, the `with_*` methods on `Selectable` and `Filter` (list arguments accepted), `Barrier`, `is_null`, `not_`, and the `update`/`where` signatures.
- `randomGaussian` and `randomInt`.
- Serial filters can't be reordered.
- The multiple-barriers execution order.

## Structure (step 2: deephaven-doc-structure-review; report only)

- **S1 (high). The counter example breaks its own thread.** The problem uses two columns (A, B), the fix switches to one column (`ID`), and Barriers then says it's "building on" it. Make the Serialization example single-column (problem, then `with_serial`), and add the second column only in Barriers. That consolidates three copies of the counter function into one progressive example, and also fixes A2.
- **S2. Cross-table parallelism is explained twice in full,** in "Across tables" (28-52) and again in the Updates paragraph (87), with the update graph introduced at both 52 and 85. The phases section also comes after the parallelism sections, even though cross-table parallelism exists only in the update phase. Either put phases first as the framing, or cut the Updates paragraph to a back-reference.
- **S3. The threshold caveat (NOTE at 123-124) comes about 65 lines after the claims it limits** (58, 83), and it's framed as a note about the small example tables. Move the select, where, and sort thresholds into "Within a single table" right after **Across rows**.
- **S4. Terms are used before they're defined.** `with_serial`, barriers, implicit barriers, "Python-backed", "non-parallelizable", and notifications all appear before **Key concepts** at line 132. Move Key concepts up to just after the Quick reference, or link the Quick reference cells to their sections. Move the GIL caution's `with_serial` paragraph into Serialization.
- **S5. Three summary passes for one decision:** Quick reference, Choosing an approach, and Key takeaways. Fold Choosing an approach into the Quick reference and keep Key takeaways as the one closing summary. Keeping the Quick reference near the top is the right call.
- **S6. Callouts drift.** The breaking-change callout (9) says `with_serial` is enough. The IMPORTANT at 283 says you need both `with_serial` and a barrier. The NOTE at 150 says to use `with_serial` only when parallelization causes wrong results. Reconcile the top callout and link it to Controlling execution order.
- **S7. Stateful partition filters is a niche aside, and "location" is never defined.** Move it under Serialization after Serial filters, or add a lead-in that says which tables have partitioning columns. Keep the heading text so the anchor stays stable.
- **S8. Config properties are scattered** across lines 66, 83, 87, 124, 126, and 320. Gather them into one table near the end that links to Query table configuration.
- **S9. The intro promises three ways but the headings show two.** Line 26 lists across tables, rows, and columns; the headings are "Across tables" and "Within a single table". Use three headings or reword line 26.
- **Length:** 356 lines, 7 code blocks, and the counter function defined 3 times, which meets both conditions of the length and repetition check. S1 and S5 are the consolidation moves.

Structural edits not applied (report-only); deferring re-verification to the orchestrator's steps 3-4.

## Step 3: re-verification and links

- **Spot checks:** none run, because nothing was applied. If S1, S3, or S4 are applied, spot-check each moved or reworded paragraph separately. Escalate to a full accuracy re-pass, since the threshold and `with_serial` claims appear in several places in this file.
- **Caveats to keep during any move:** the threshold note and "not running concurrently isn't the same as running in row-set order".
- **Content the PR dropped compared with the pre-PR file:**
  - The "engine assumes all user expressions are stateless" sentence (restore it; see A7).
  - "Track processing time" was dropped from Related documentation, even though that page still links here. Possibly intentional; unconfirmed.
  - The `merge` dependency illustration now survives only as the bullet at line 72.
- **Inbound links (corpus-wide scan, `docs/python` at f2ef483084):** 23 in total. Anchored ones: `#serialization` (3), `#barriers` (4), and `#controlling-concurrency-for-select-update-and-where` (1, broken; see A3). Any restructure must keep the `### Serialization` and `### Barriers` headings. In-page anchors that proposed moves could affect: `#query-phases-and-thread-pools`, `#example-a-counter-needs-serialization` (S1 could rename it), and `#example-extending-the-counter-with-a-barrier`.

## Style (step 4: deephaven-writing-style)

The required mechanical searches came back clean: no dot-prefixed method names or empty `()` in prose, no `[here]` link text, and no curly quotes. Em dashes are spaced, headings are sentence case, naming conventions are followed, and first mentions are linked.

- **Y1.** Future tense at line 9 ("will now produce").
- **Y2.** "Operation Initialization Thread Pool" and "Update Graph Processor Thread Pool" read as proper nouns, and "Update Graph Processor" is a legacy name. Use plain language.
- **Y3.** Undefined jargon: "notifications" (50), "Python-backed" (75), "non-parallelizable column" (77), and "location" (329-331).
- **Y4.** Paragraphs doing too much: line 87, line 331, and the CAUTION at 75. Split them.
- **Y5.** Link wording: `[Thread pools](#query-phases-and-thread-pools)` should match the heading, and the line-153 `with_serial` link text doesn't match where it goes.
- **Y6.** The `order=result` fix example runs a Python UDF over 5,000,000 rows during doc builds. The result is deterministic at any size, so shrink it.
- **Y7.** Related documentation could add Selectable, Filter, Barrier, Query table configuration, the Crash Course page, and Track processing time. All exist at f2ef483084.
- **Y8.** The tone suits a concept guide.

## Follow-ups outside this review's scope

1. **Groovy sibling (not reviewed, at your request).** A1, A2, A4, A5, A6, A8, and A12 are probably repeated there, and `docs/groovy/how-to-guides/predicate-pushdown.md:13` has the same broken anchor as A3.
2. **Source javadoc bug.** `table-api/src/main/java/io/deephaven/api/ConcurrencyControl.java` (`withSerial` javadoc) says `serialSelectImplicitBarriers` defaults "to the value of `statelessSelectByDefault`", but the code (`QueryTable.java:400-402`) uses the negation. Worth a separate engine-side fix.
