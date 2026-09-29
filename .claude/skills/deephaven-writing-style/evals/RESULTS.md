# Eval results: `deephaven-writing-style`

Evals for this skill are in [`evals.json`](./evals.json), and the documents they run against are in [`files/`](./files/). They were written and run with Anthropic's skill-creator tooling for [DOC-1524](https://deephaven.atlassian.net/browse/DOC-1524) (PR #8704), which refined the review skills after Chip Kent's review of the parallelization docs (PR #7457).

Raw run outputs, grading files, and skill snapshots are kept out of the repo. This file is the record. "Baseline" means the skill before the change being tested; each iteration names it.

## Iteration 1: new skill vs pre-PR skill (eval 1)

| | New | Pre-PR |
| --- | --- | --- |
| Expectations passed | 5/5 | 3/5 |

- Only the new skill proposed self-describing or recognized terms ("concurrent row calculations", "pure function"). The pre-PR run called "across rows" good plain language.
- The stateless/thread-safe and parenthetical expectations passed in both arms. The fixture was too blunt, so iteration 2 added a subtler one (eval 2).

## Iteration 2: new skill vs iteration-1 skill (evals 1 and 2)

Change tested: a configuration property moved out of a parenthetical into its own narrative sentence is still injection.

| Target expectation | New | Iteration 1 |
| --- | --- | --- |
| No configuration property reinjected into prose (both evals) | 6/6 | 1/6 |

- Terminology expectations passed in all 12 runs, including the subtler fixture.
- The terminology expectations were later reworded to accept "recognized or self-describing" terms, to match the revised rule.

## Caveats

- Runs are small (1 run per configuration in iteration 1, 3 after that), so treat pass counts as evidence that a behavior appears, not as stable rates.
- From iteration 2 on, independent grader agents scored the reports blind (shuffled and unlabeled). Judgment-call expectations still vary between graders; the notes say where.
