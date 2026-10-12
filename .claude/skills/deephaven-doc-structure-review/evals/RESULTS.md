# Eval results: `deephaven-doc-structure-review`

Evals for this skill are in [`evals.json`](./evals.json), and the document they run against is in the shared [`eval-fixtures/`](../../eval-fixtures/) folder. They were written and run with Anthropic's skill-creator tooling for [DOC-1524](https://deephaven.atlassian.net/browse/DOC-1524) (PR #8704), which refined the review skills after Chip Kent's review of the parallelization docs (PR #7457).

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

## DOC-1745 round 4: Chip Kent's second review of the parallelization guide

This round adds a **Section can't be read on its own** check and extends **Terminology introduced before it's defined** from the opening sections to the whole page, with a page-wide term list in the structure map. The fixtures are real passages from the guide at `609b266973` ([`eval-fixtures/parallelization-609b-chip-round2.md`](../../eval-fixtures/parallelization-609b-chip-round2.md)). Eval 4 is a held-out fixture, the Crash Course draft. Baseline is the skill at `upstream/main` before this change.

3 runs per configuration, graded blind (reports shuffled and relabeled), one grader agent per eval.

| Eval | Check tested | New | Baseline |
| --- | --- | --- | --- |
| 2 | Self-contained sections | 10/12 | 4/12 |
| 3 | Undefined terminology in "Implicit barriers" | 13/15 | 3/15 |
| 4 (held out) | Terms never defined on the page | 9/9 | 8/9 |

- An expectation that the section never says how implicit barriers are enabled was removed after review. The opener does describe what they do, and the skill says a behavioral definition is enough, with the controlling property staying in the Configuration section. The eval now rests on the undefined mode labels, "this setting", defining at first use, and ranking the finding, where the baseline falls short because its terminology check scans only the opening sections. Totals were recomputed from the existing grades without that expectation.
- The held-out Crash Course fixture is near the ceiling for both configurations, because "stateless" and "row-set order" are easy to spot. It shows no regression; it doesn't show an improvement.
- The one gray area is a section that builds on an earlier one. The rule is that the opener must name what it builds on and restate what it needs, and then the pointer passes even when the target is several sections up. "The same applies to filters" fails. In iteration 1 the new skill still listed the passing "Building on the counter example above" opener as a defect in 2 of 3 runs.

### Iteration 2: tightened wording, eval 2

Baseline here is iteration 1 of the new skill, 3 runs each, graded blind.

| Eval | Iteration 2 | Iteration 1 |
| --- | --- | --- |
| 2 | 12/12 | 9/12 |

The one control expectation (the named, restated opener isn't a defect) went from 0/3 to 3/3 after the rule was reworded. The same expectation scored 1/3 for iteration 1 in the first grading pass, so treat that control as noisy.

Wording note: eval 2's rule expectation now accepts a pointer to any earlier section that names its target and restates what it needs, regardless of distance, matching the control that accepts "Building on the counter example above".

