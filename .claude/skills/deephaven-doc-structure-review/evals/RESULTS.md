# Eval results: `deephaven-doc-structure-review`

Evals for this skill are in [`evals.json`](./evals.json), and the documents they run against are in [`files/`](./files/). They were written and run with Anthropic's skill-creator tooling for [DOC-1524](https://deephaven.atlassian.net/browse/DOC-1524) (PR #8704), which refined the review skills after Chip Kent's review of the parallelization docs (PR #7457).

Raw run outputs, grading files, and skill snapshots are kept out of the repo. This file is the record. "Baseline" means the skill before the change being tested; each iteration names it.

## Iteration 1: new skill vs pre-PR skill

Fixture: the parallelization concept guide at `f2ef483084`.

| | New | Pre-PR |
| --- | --- | --- |
| Expectations passed | 5/5 | 2/5 |

- Discriminating: the purpose statement, mixed-kind table rows, and a list that doesn't match its section. The pre-PR run also proposed adding property names to the breaking-change callout.
- One pre-PR pass was graded leniently, so iteration 2 split that expectation in two.

## Iteration 2: blind regrade of the iteration-1 outputs

| | New | Pre-PR |
| --- | --- | --- |
| Expectations passed | 6/6 | 2/6 |

The split confirmed the lenient call: the pre-PR run passes the GIL half and fails the configuration half.

## Caveats

- Runs are small, so treat pass counts as evidence that a behavior appears, not as stable rates.
- Both iterations use the same single run per configuration. Iteration 1 graded it inline; iteration 2 regraded it blind with an independent grader agent.
- Judgment-call expectations can vary between graders; the notes say where.
