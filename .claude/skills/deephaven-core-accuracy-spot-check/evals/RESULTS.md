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
