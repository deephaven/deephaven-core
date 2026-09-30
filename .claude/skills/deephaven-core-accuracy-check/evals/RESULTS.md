# Eval results: `deephaven-core-accuracy-check`

Evals for this skill are in [`evals.json`](./evals.json), and the documents they run against are in [`files/`](./files/). They were written and run with Anthropic's skill-creator tooling for [DOC-1524](https://deephaven.atlassian.net/browse/DOC-1524) (PR #8704), which refined the review skills after Chip Kent's review of the parallelization docs (PR #7457).

Raw run outputs, grading files, and skill snapshots are kept out of the repo. This file is the record. "Baseline" means the skill before the change being tested; each iteration names it.

## Iteration 1: new skill vs pre-PR skill (eval 1)

Fixture: the parallelization concept guide at DOC-857 commit `f2ef483084`, the version Chip reviewed.

| | New | Pre-PR |
| --- | --- | --- |
| Expectations passed | 4/4 | 0/4 |

- The pre-PR skill never questioned the `view`/`update_view`/`lazy_update` "not parallelized" classification, though it quoted the same `QueryTable` guard.
- Both found the same set of other real defects. The change targets the mental-model miss, not general thoroughness.

## Iteration 2: new skill vs iteration-1 skill (evals 1 and 2)

Change tested: the "Prescriptive rows claim sufficiency" pitfall. Eval 2 is held out: the Crash Course chapter, which wasn't used to write the change.

| Target expectation | New | Iteration 1 |
| --- | --- | --- |
| `with_serial` alone is insufficient across columns (both evals) | 6/6 | 3/6 |
| ...held-out eval only | 3/3 | 1/3 |

- Everything iteration 1 added held in all 3 runs.
- The held-out control expectation (per-update threshold) was noisy in both arms. It tests general thoroughness, not this change.

## Caveats

- Runs are small, so treat pass counts as evidence that a behavior appears, not as stable rates.
- Iteration 1 was 1 run per configuration, graded inline. Iteration 2 was 3 runs per configuration, scored blind by an independent grader agent (reports shuffled and unlabeled).
- Judgment-call expectations can vary between graders; the notes say where.
