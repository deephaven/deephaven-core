# Review comments on docs/python/getting-started/crash-course/parallelization.md

Source: copilot-pull-request-reviewer[bot], review round 7 on this PR. Line numbers refer to
`evals/files/crash-course-parallelization.md`. Earlier rounds on this page were already addressed.

**C1 (line 155)**
The broken-counter snippet calls `empty_table(100)` but never imports it. A reader who copies this block gets `NameError: name 'empty_table' is not defined`. Add `from deephaven import empty_table`, as the corrected version below does.

**C2 (line 173)**
The explanation says two cores "both return 6," but the illustrative output table above contains no 6 — it shows duplicated 2s and 5s and a missing 6. Make the prose and the table agree.

**C3 (line 198)**
"Use `with_serial` any time your formula depends on shared state" overstates what `with_serial` provides. Per `ConcurrencyControl.withSerial`, the expression "will never be invoked concurrently with itself," but with `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` false (the default), no ordering is imposed between different serial selectables. If two columns share the counter, `with_serial` on each is not sufficient; barriers are also required.

**C4 (line 2)**
The page title "Query Parallelization" uses title case. The documentation style guide requires sentence case for headings and titles. Change it to "Query parallelization."

**C5 (line 40)**
"sized to your CPU cores by default" should name the controlling property. The update graph's thread count comes from `PeriodicUpdateGraph.updateThreads` (default `-1`, resolved to `Runtime.availableProcessors()`), and `updateThreads=1` is a valid configuration under which these tables never update concurrently. Please state the property and its default here.

**C6 (line 55)**
The chunk count is determined by the operation-initialization scheduler's thread count (`OperationInitializationThreadPool.threads`), not directly by the machine's core count. Qualify this example in terms of `OperationInitializationThreadPool.threads` so readers with a non-default pool size aren't misled.

**C7 (line 57)**
"at least a few million rows" is imprecise. State the exact threshold — `QueryTable.minimumParallelSelectRows`, default `1L << 22` (4,194,304) — and note that the comparison is `>=` against the rows added plus modified in each update, not the table size, and that formulas returning `Table` or `RowSet` are split regardless of size.

**C8 (line 180)**
The corrected example uses only 100 rows, which is below the parallelization threshold, so it would behave correctly even without `with_serial` and doesn't demonstrate the fix. Increase the table to more than 4,194,304 rows so the example actually exercises parallel execution.

**C9 (line 204)**
"Deephaven runs formulas in parallel by default" is too absolute. Qualify it: formulas are split across rows only when the update has at least `QueryTable.minimumParallelSelectRows` rows, the job scheduler has more than one thread, the update has no shifts, and — for Python-backed formulas — the build is free-threaded.

**C10 (line 198)**
"parallelization isn't the only way execution order can vary" is an unsupported claim; nothing on the page explains what else could reorder evaluation. Remove the clause or cite the mechanism.
