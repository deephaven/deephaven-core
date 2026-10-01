# Eval results: `deephaven-docs-review-full`

Evals for this skill are in [`evals.json`](./evals.json), and the documents they run against are in [`files/`](./files/) and the shared [`eval-fixtures/`](../../eval-fixtures/) folder. They were written and run with Anthropic's skill-creator tooling for [DOC-1524](https://deephaven.atlassian.net/browse/DOC-1524) (PR #8704), which refined the review skills after Chip Kent's review of the parallelization docs (PR #7457).

Raw run outputs, grading files, and skill snapshots are kept out of the repo. This file is the record. "Baseline" means the skill before the change being tested; each iteration names it.

## Report mode (eval 1)

Fixture: the parallelization concept guide at `f2ef483084`.

| | New | Baseline |
| --- | --- | --- |
| Iteration 1 (baseline: pre-PR) | 5/5 | 1/5 |
| Iteration 2 (baseline: iteration 1), prescriptive-rows expectation | 3/3 | 1/3 |

- The pre-PR review didn't flag the `view` misclassification, and didn't produce an editorial summary, developmental notes, or author queries.
- Report-shape expectations passed in all 6 iteration-2 runs.
- One no-reinjection miss per arm was a strict grader call.
- An examples-pass expectation was added after review. All 6 saved iteration-2 reports pass it (both arms have the pass), so it guards against a regression rather than discriminating.

## Edit mode (eval 2)

Fixture: the liveness-scope concept guide in both languages, with real defects and one trap. The doc's `@liveness_scope()` is correct, though the pydoc shows the bare form. Baseline: pre-PR skills.

| | New | Pre-PR |
| --- | --- | --- |
| Iteration 3 (11 expectations) | 30/33 | 32/33 |
| Iteration 4 (12 expectations) | 34/36 | 30/36 |
| Iteration 5 | 36/36 | 32/36 |

- **Iteration 3: a regression.** The developmental pass led edit mode to act on its own "needs restructuring" verdict. Runs rewrote sections, swapped in unrun examples, retitled the page and added caveats, changing about twice as much text.
- **Iteration 4:** fixed that. "Apply the fixes" now means targeted fixes, with restructuring reported instead, and the placement gate covers text the reviewer writes itself. But one run deferred two small real fixes. The "edits stay targeted" expectation was added after iteration 3.
- **Iteration 5:** the rule now says a targeted fix isn't deferred because its example has bigger problems. Every run made every repair, with small diffs.
- **Across all three:** no run fell for the decorator trap. Baseline scores varied between graders on the same runs (32/33, 30/36, 32/36), mainly on the hedge and "targeted" judgment calls.

## Caveats

- Runs are small, so treat pass counts as evidence that a behavior appears, not as stable rates.
- Iteration 1 was 1 run per configuration, graded inline. Iterations 2 to 5 were 3 runs per configuration, scored blind by independent grader agents.
- Judgment-call expectations can vary between graders; the notes say where.
