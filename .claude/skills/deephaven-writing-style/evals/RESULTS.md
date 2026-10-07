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

- Runs are small, so treat pass counts as evidence that a behavior appears, not as stable rates.
- Iteration 1 was 1 run per configuration, graded inline. Iteration 2 was 3 runs per configuration, scored blind by an independent grader agent.
- Judgment-call expectations can vary between graders; the notes say where.

## DOC-1745 round 4: Chip Kent's second review of the parallelization guide

Chip's second review of PR #7457 ([review 5431364163](https://github.com/deephaven/deephaven-core/pull/7457#pullrequestreview-5431364163), 23 comments) landed after #8704 merged. This round adds four rules to this skill: sentences that need rescuing punctuation, one idea per bullet, linking the first mention in each `##` section, and backticked column names in prose. The fixtures are the passages Chip flagged, taken verbatim from the guide at `609b266973` ([`eval-fixtures/parallelization-609b-chip-round2.md`](../../eval-fixtures/parallelization-609b-chip-round2.md)). Evals 3 to 6 are those fixtures. Eval 7 is a held-out fixture, the Crash Course draft, that wasn't used to write the rules. Baseline is the skill at `upstream/main` before this change.

3 runs per configuration, each report graded blind (reports shuffled and relabeled) by one grader agent per eval.

| Eval | Rule tested | New | Baseline |
| --- | --- | --- | --- |
| 3 | Sentences that need rescuing punctuation | 24/27 | 13/27 |
| 4 | Link first mention per `##` section | 13/15 | 6/15 |
| 5 | One idea per bullet | 12/12 | 8/12 |
| 6 | Backticked column names | 12/12 | 7/12 |
| 7 (held out) | Colon sentence, per-section link | 9/9 | 5/9 |

- The baseline doesn't catch the linking miss at all (eval 4, 0/3 on the section-opening expectations). It checks only that the first mention in the file is linked, and the guide links everything in its introduction.
- The baseline rarely flags the barrier definition, the implicit-barriers opener, or the configuration intro (1 of 9 expectation-runs) and never names the problem as one pattern (0/3). The new skill names it as a pattern in all 3 runs.
- No baseline run stated column-name backticking as a doc-set convention (eval 6, 0/3). The corpus count (about 509 backticked against about 59 bare, mostly in headings and UI labels) is now in the skill.
- Control expectations (clean paragraphs, code blocks, and short bullets not flagged) passed in all 15 iteration-1 runs of the new skill.
- Iteration 1 misses led to two wording changes: a rewrite for every flagged paragraph, and a link target for every flagged identifier.

### Iteration 2: tightened wording, evals 3 and 4

Baseline here is iteration 1 of the new skill, 3 runs each, graded blind.

| Eval | Iteration 2 | Iteration 1 |
| --- | --- | --- |
| 3 | 26/27 | 24/27 |
| 4 | 14/15 | 13/15 |

The link-target expectation in eval 4 went from 1/3 to 3/3. The remaining miss in eval 3 is the implicit-barriers opener, where one run flagged the paragraph but proposed no split in both iterations.

Fixture note: after review, a stray closing fence was removed from `awk-excerpt.md` and the opening fence was added to the code block in `column-names-excerpt.md`. Evals 3 and 6 were run on the earlier, unbalanced versions (the reviewers read the raw text), so a re-run on the corrected files would confirm the scores.
