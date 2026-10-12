# Eval results: `deephaven-core-accuracy-spot-check`

Evals for this skill are in [`evals.json`](./evals.json), and the documents they run against are in [`files/`](./files/). They were written and run with Anthropic's skill-creator tooling for [DOC-1524](https://deephaven.atlassian.net/browse/DOC-1524) (PR #8704), which refined the review skills after Chip Kent's review of the parallelization docs (PR #7457).

Raw run outputs, grading files, and skill snapshots are kept out of the repo. This file is the record. "Baseline" means the skill before the change being tested; each iteration names it.

## Iteration 1: new skill vs pre-PR skill

| | New | Pre-PR |
| --- | --- | --- |
| Expectations passed | 3/3 | 2/3 |

- Both caught the wrong threshold and traced it to `SelectColumnLayer`. Only the placement expectation discriminated: the pre-PR fix put the property name and default into concept-guide prose.
- Both flagged a stale claim in `query-table-configuration.md`, filed as DOC-1522.

## Caveats

- Runs are small, so treat pass counts as evidence that a behavior appears, not as stable rates.
- There is only iteration 1: 1 run per configuration, graded inline, not blind.
- Judgment-call expectations can vary between graders; the notes say where.

## DOC-1560 (PR #8860): whole-page accuracy and audit fixes

Method: "main" is the skill on `main`; "new" is the PR branch. Each trial read the skill from its configuration's folder and verified against the same checkout. A separate grader scored each eval's reports blind (shuffled, unlabeled) against the expectations. Two to three runs per cell unless noted, so treat counts as direction, not rates.

First round, scored by the session that wrote it: eval 1 main 6/6, final 3/3 (1 run); eval 2 (new) main 0/6, final 2/9 (3 runs). Eval 2's unchanged "processed in sequence" claim needs the engine's parallel formula path to refute, and no configuration did.

Second round:

| Eval | What it tests | main | new |
| --- | --- | --- | --- |
| 3 (new) | An unchanged neighbor sentence calls `time_table` a blink table | 6/6 | 6/6 |
| 4 (new) | A reaggregating-formula claim the source doesn't settle | 3/6 | 6/6 |

- Eval 3 doesn't separate the configurations: in a two-sentence paragraph the `main` skill also checks the neighbor. It guards against losing that behavior.
- Eval 4: one `main` run confirmed the false "output must have the same name as the input" requirement. Every new run either refuted it from `AggregationProcessor`'s reaggregated converter or marked it unverified with an example to run.

## DOC-1990 round 5: an over-broad sentence a fix pass wrote (2026-10-12)

While restructuring the Crash Course chapter for PR #7457, a fix pass wrote "Parallelization produces incorrect results when a row's calculation depends on something outside that row." Copilot caught it; reading an immutable global is fine, and the chapter's own stateless examples do it. `deephaven-core-accuracy-check`'s overstated-guarantee pitfall now names this shape (**Over-broad unsafe condition**) next to **Thread affinity**, and `deephaven-docs-review-full` requires a spot-check of every sentence a fix pass writes. Eval 5 asks for a spot-check of that sentence. Fixture: [`eval-fixtures/crash-course-parallelization-d186.md`](../../eval-fixtures/crash-course-parallelization-d186.md). Baseline is the skill at `upstream/main` (29668d5d0c); this skill's text did not change in the round, so the eval measures the shared pitfall list it reads from `deephaven-core-accuracy-check`.

3 runs per configuration, graded blind, one grader agent.

| Eval | What it tests | New | Baseline |
| --- | --- | --- | --- |
| 5 (3 expectations) | Over-broad unsafe condition; narrower rewrite; control on the two bullets | 9/9 | 8/9 |

- Every run in both configurations refuted the sentence from `SelectColumn.isStateless` and `DhFormulaColumn.isImmutableType` and proposed "shared state that other rows change, or the order in which rows are processed". That is the Crash Course's level of precision; the finer cut (unsynchronized access and order dependence are independent failure modes, so a thread-safe counter can still be order-dependent) lives in the accuracy-check rule. The baseline skill already verifies a changed sentence against source well enough to catch this; the eval guards that behavior rather than separating the configurations.
- The one baseline miss was the control: that run rewrote the row-ordering bullet ("row 2 before row 1") as literally wrong because the engine splits rows into large divisions. The grader held that a Crash Course illustration is not a defect.
