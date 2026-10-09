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

### Eval 3 negative controls (review follow-up)

Copilot's review pointed out that only the bold-label exemption had a negative control. Eval 3 now has a tenth expectation covering the other exemptions: a colon introducing a list, a colon introducing a code block, and a single dash before a pointer. These ran 3 times per iteration on the new skill only (no baseline), graded blind.

| Iteration | Change | Eval 3 (10 expectations) |
| --- | --- | --- |
| A | Control used a sentence with a dash and a colon | 28/30 (control 2/3) |
| B | Em-dash rule reworded to agree with the colon exemption | 27/30 (control 1/3) |
| C | Control passages replaced with one-purpose sentences | 30/30 |

In iterations A and B, reviewers flagged the control sentence because it contained both a dash and a colon, which the skill's own em-dash rule allows to be questioned. The em-dash wording now says a colon that introduces a list or code block doesn't count, and the control passages each test one exemption.

### Eval 8: bare type names (review follow-up)

Copilot noted that the required link sweep started from method-shaped identifiers, so a bare `Filter` or `Selectable` could be missed. Eval 8 uses the guide's opening section with the links on `Selectable` and `Filter` removed (the links appear later in the excerpt), plus a code block as a control. 3 runs per configuration, graded blind.

| | New | Baseline |
| --- | --- | --- |
| Eval 8 (4 expectations) | 12/12 | 12/12 |

The baseline skill also flagged the bare type names, so this eval doesn't separate the two versions. It guards against a regression in the sweep that now names type names explicitly.

Iteration D: the rule now treats hard-to-read sentences as the defect and punctuation as evidence for it (review follow-up). Eval 3 scored 28/30 (new skill only, 3 runs, graded blind). The two misses were a rewrite that kept a parenthetical, and a control sentence flagged for its unnamed "this setting" rather than for punctuation.

Iteration E: the rule now calls punctuation a clue to inspect rather than evidence of a defect by itself, and the rewrite ban covers parenthetical asides (review follow-up). Eval 3 scored 29/30 (new skill only, 3 runs, graded blind). The one miss is a rewrite that kept the acronym expansion "(global interpreter lock)" in parentheses, the same miss seen in earlier iterations.

Grader note: the recurring eval 3 miss ("(global interpreter lock)" kept in a rewrite) defines the acronym GIL on first use, which the skill requires, so it is not a parenthetical aside. Expectation 7 now says so. Earlier scores for that expectation were graded under the stricter wording, so treat those misses as a strict grader call; I haven't re-graded them.

Wording note: eval 4 expectation 1 now requires the flag on the section's opening paragraph (the first mention), not a later bullet. Re-graded on the new wording, the scores are unchanged (new 3/3, baseline 0/3, and 3/3 for iteration 2).

Eval 6 extension (review follow-up): the column-name sweep now also searches the prose for names the page defines, used alone or in a coordinated phrase ("A and B run in parallel"). The fixture gained that passage and eval 6 gained a fifth expectation. New skill only, 3 runs, graded blind: 14/15, and the new expectation passed in all 3 runs. The one miss stated the backtick rule without citing the corpus count or the skill.

## DOC-1560 (PR #8860): whole-page accuracy and audit fixes

Method: "main" is the skill on `main`; "new" is the PR branch. Each trial read the skill from its configuration's folder and verified against the same checkout. A separate grader scored each eval's reports blind (shuffled, unlabeled) against the expectations. Two to three runs per cell unless noted, so treat counts as direction, not rates.

| Eval | What it tests | main | new |
| --- | --- | --- | --- |
| 9 (new) | `order=` tags list tables their block didn't create | 4/6 | 6/6 |

One `main` run also flagged the setup block's `order=` tag or the shared `test-set`, which the control expectation rules out.

### Chip Kent's 2026-10-09 review of #7457 (linking and readability)

Chip's third review of the parallelization concept guide flagged bare API names that the per-`##`-section linking rule allows (later mentions in a new paragraph, bold lead terms in a contrast list, every name in Key takeaways), a "wall of text" section, and a qualifier with no concrete meaning. He asked twice for these real cases to become evals. Changes: the linking unit is now each paragraph, list, and table, with every API name linked in summary blocks; new **Walls of text** and **Qualifiers with no concrete referent** rules, each with a search step; the example identifiers were removed from the rule text so the new eval stays held out. Eval 10 uses the page exactly as Chip reviewed it (#7457 at bf296eeffa); eval 4's expectations moved from the per-section rule to the per-paragraph rule.

| Eval | main | new (first rule text) | new (with search steps) |
| --- | --- | --- | --- |
| 10 (Chip's cases, 6 expectations) | 4/12 | 8/12 | 10/12 |
| 4 (regression, new only) | - | 10/10 | - |

- Both `main` runs missed the per-paragraph bare names (line 139, the bold lead terms at 143-144); every new run caught them.
- The wall-of-text check passed in 1 of 2 runs before its search step and 2 of 2 after.
- No run, in either configuration, flagged "with no Deephaven configuration". After the search step, both new runs found the phrase and judged it a checkable fact ("there is no property to set"), which is what the rule asks for; Chip read it as unclear. This stays a judgment call; not tuned further.

**Correction (expectation 5 of eval 10).** As first written, expectation 5 required flagging "with no Deephaven configuration", but the rule lets a qualifier pass when it states a checkable fact, and the final new runs made exactly that call explicitly. The expectation now accepts either a rewrite/removal or an explicit explanation; silence still fails. Rescored from the graders' recorded reasons (no rerun): `main` 4/12 (neither run mentioned the phrase), new first rule text 8/12 (neither run mentioned it), new with search steps 12/12 (both runs explained why the phrase passes).

