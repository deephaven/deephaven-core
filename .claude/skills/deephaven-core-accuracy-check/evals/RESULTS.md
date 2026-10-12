# Eval results: `deephaven-core-accuracy-check`

Evals for this skill are in [`evals.json`](./evals.json), and the documents they run against are in the shared [`eval-fixtures/`](../../eval-fixtures/) folder. They were written and run with Anthropic's skill-creator tooling for [DOC-1524](https://deephaven.atlassian.net/browse/DOC-1524) (PR #8704), which refined the review skills after Chip Kent's review of the parallelization docs (PR #7457).

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

## DOC-1560 (PR #8860): whole-page accuracy and audit fixes

Method: "main" is the skill on `main`; "new" is the PR branch. Each trial read the skill from its configuration's folder and verified against the same checkout. A separate grader scored each eval's reports blind (shuffled, unlabeled) against the expectations. Two to three runs per cell unless noted, so treat counts as direction, not rates.

First round (claim ledger, one row per clause, shown in the report), scored by the session that wrote it:

| Eval | main | Final |
| --- | --- | --- |
| 1 | 10/10 (2 runs) | 5/5 (1) |
| 2 | 4/6 (2) | 3/3 (1) |
| 3 (new) | 3/10 (2) | 11/15 (3) |

Second round (checks from the DOC-1560 overhaul pilots), new evals 4-6:

| Eval | What it tests | main | new |
| --- | --- | --- | --- |
| 4 (new) | Published Docker images: evidence comes from deephaven-server-docker, not `docker/` | 4/6 | 6/6 |
| 5 (new) | Rollup table marks supported aggregations unsupported; stale `Table.rollup` docstring; TODO linking a closed issue | 7/10 | 10/10 |
| 6 (new) | Join flowchart has no multi-join path although the page teaches one | 5/8 | 8/8 |

- Eval 4: one `main` run cited this repo's `docker/server-slim` Dockerfile and raised the false concern that `START_OPTS` drops the Groovy console. Every new run cited deephaven-server-docker.
- Eval 5: both configurations flagged `group` from the engine, so the stale-docstring rule doesn't separate them here. The difference was the TODO: `main` didn't check issue 2079's state.
- Eval 6: `main` read the chart but missed the missing multi-join path or that the SVG is shared.
- Regression, new skill only (1 run): eval 1 5/5, eval 2 3/3, eval 3 4/5. The eval 3 miss is the test-source condition in the `i`/`ii` restriction, which no run has stated.

### Rerun after removing eval answers from the skill text

Copilot pointed out that several worked examples in the skills named the exact defects these evals look for, so an agent could pass by repeating the example. The examples are now general, and the new-skill runs were repeated (2 runs each; `main` is unchanged, so its scores carry over). These rows replace the ones above as the measure of the rules themselves.

| Eval | main | new, examples named the answer | new, general examples |
| --- | --- | --- | --- |
| 4 | 4/6 | 6/6 | 5/6 |
| 5 | 7/10 | 10/10 | 10/10 |
| 6 | 5/8 | 8/8 | 8/8 |

- Eval 4: one run still raised the `START_OPTS` concern as a hedged finding with an author query; the other took the console type from the published image's property file.

- Coverage gap: no accuracy-check eval yet exercises the deprecation rule (a runnable but deprecated API, with `@Deprecated` or "Use X instead" as evidence) or checks that the Python and Groovy twins are both reviewed. Copilot flagged this on #8860; it's follow-up work.
