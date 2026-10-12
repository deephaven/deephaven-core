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
