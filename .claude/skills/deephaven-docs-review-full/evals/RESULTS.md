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

Fixture: copies of the liveness-scope concept guide in both languages (each run edited its own copies, standing in for the `docs/` pages; the prompt now names the copies directly and says to edit only those), with real defects and one trap. The doc's `@liveness_scope()` is correct, though the pydoc shows the bare form. Baseline: pre-PR skills.

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

## DOC-1560 (PR #8860): whole-page accuracy and audit fixes

Method: "main" is the skill on `main`; "new" is the PR branch. Each trial read the skill from its configuration's folder and verified against the same checkout. A separate grader scored each eval's reports blind (shuffled, unlabeled) against the expectations. Two to three runs per cell unless noted, so treat counts as direction, not rates.

First round, scored by the session that wrote it: eval 1 main 8/9, final 9/9 (1 run each); eval 2 main 10/12, first version 9/12, not rerun.

Second round:

| Eval | What it tests | main | new |
| --- | --- | --- | --- |
| 1 (regression, new only, 1 run) | Report mode, now with the ledger table in Coverage | - | 9/9 |
| 2 (regression, new only, 1 run) | Edit mode, ordinary review | - | 10/12 |
| 3 (new) | Audit-mode overhaul keeps accurate content and stays in scope | 12/12 | 11/12 |

- Eval 2's two misses match earlier rounds: the unreleased `LivenessScope` demo was only recommended, not fixed, and the report didn't say passages were re-verified after editing. Eval 2 still expects targeted edits, which shows the audit exception doesn't leak into ordinary reviews.
- Eval 3, first try: one new run replaced the defined term "root node" with a synonym (new 11/12, main 12/12). The keep-terms rule now says terms the page defines stay word for word. Rerun: new 11/12, main 12/12, with "root node" kept in every run; the one miss renamed a heading and a variable without a stated reason.
- Eval 3 doesn't separate the configurations well: the `main` skill's default fix-not-rewrite limit already keeps edits small. The eval guards the audit exception's "keep what is accurate" and "edit only in-scope pages" rules against regressions.

### Rerun after removing eval answers from the skill text

Copilot pointed out that several worked examples in the skills named the exact defects these evals look for, so an agent could pass by repeating the example. The examples are now general, and the new-skill runs were repeated (2 runs each; `main` is unchanged, so its scores carry over). These rows replace the ones above as the measure of the rules themselves.

| Eval | main | new, examples named the answer | new, general examples |
| --- | --- | --- | --- |
| 3 | 12/12 | 11/12 | 10/12, then 12/12 |

- With general examples, both runs renamed variables and headings or reworded a correct note without a reason. The keep-accurate rule now says not to rename variables, tables, or headings or recast a correct note unless a finding calls for it, and to give a reason for every listed change. The rerun scored 12/12.

### Eval 3 with the recorded findings supplied

The audit exception now requires the caller's list of recorded findings (Copilot noted the skill had no way to get them). Eval 3 now passes the rollup page's real recorded findings (Jira DOC-1620 and DOC-1908) as a fixture and adds an expectation that the recorded minor and style findings that hold up are applied, not only the wrong ones. Both configurations were rerun (2 runs each).

| Eval | main | new |
| --- | --- | --- |
| 3 (7 expectations) | 10/14 | 14/14 |

- Both `main` runs applied the recorded finding that said to delete the TODO, which is stale: the hierarchy guide was never written. Both new runs checked it and kept the TODO.
- Both `main` runs copied the recorded fix wording that replaced "leaf nodes" with "the _leaf_ level", dropping a term the page defines. Both new runs kept it.
- Every run applied the minor and style findings (the `AggApproxPct` row, the `syntax` tag, tense and voice fixes).


## DOC-1990 round 5: spot-check what the fix pass writes (2026-10-12)

Two overstatements reached PR #7457 after its full review had passed: the restructure that followed Chip Kent's 2026-10-12 review added a summary table cell ("keeps the rows in order on one thread") and a Crash Course sentence ("depends on something outside that row") that the accuracy step never saw, because it ran before they existed. The placement gate now ends with a rule: run `deephaven-core-accuracy-spot-check` on every sentence the fix pass writes or rewrites, before pushing, treating the fix pass's own output as unreviewed content. Eval 4 (edit mode) gives the pre-fix guide at `3748d6ab90` with four recorded structure and style findings whose fixes force new sentences about `with_serial`, and checks that the report verifies them and that the new text stays inside the contract. Fixtures: `evals/files/parallelization/python-parallelization.md` and `evals/files/parallelization-findings/parallelization-review-findings.md`. Baseline is the skill at `upstream/main` (29668d5d0c).

3 runs per configuration, graded blind from each run's report and a diff of the file it left, one grader agent.

| Eval | What it tests | New | Baseline |
| --- | --- | --- | --- |
| 4 (6 expectations) | Summary table added; new sentences verified against source; no thread-affinity wording; block moved and counts aligned; parallel list; scope and traceability | 17/18 | 18/18 |

- Every run in both configurations applied the four findings, wrote a per-sentence verification table citing the `ConcurrencyControl` javadoc, and kept the new `with_serial` wording to "one row at a time, in order". The baseline skill's existing re-verify step for moved or rewritten prose already produces this behavior on a page this size, so the eval does not separate the configurations; it guards the behavior and documents the case that motivated the rule.
- The one new-skill miss was traceability: one run reworded the `transform`/`proxy` item while reporting items 3 to 6 unchanged.
- Two baseline runs went beyond the findings and found the `update_by` rolling-formula defect (see the accuracy-check results); one of them also replaced the sort sentence on the strength of `PeriodicUpdateGraph.configureRefreshThread`, which contradicts the `SortHelpers` javadoc the sentence rests on. Both are recorded as follow-ups for the PR, not as eval outcomes.

## Eval 5: PR-wide page coverage (#8910, Copilot's third review)

The PR-wide rule in `SKILL.md` says to list every changed doc page in both languages and to put a page-by-step table at the top of the report. Eval 5 tests only that preamble: the fixture is a file holding the output of `git diff --name-only <base>...HEAD -- docs` (a real git repo cannot be stored as a fixture), and the prompt asks for the page list and the table without reviewing anything. It does not test that every step is then run on every page.

One run per configuration, graded by the author against the six expectations (not blind):

| Check | Changed skill | Baseline |
| --- | --- | --- |
| Lists all six pages (three pairs) | pass | pass |
| Drops sidebar.json and snapshots | pass | pass |
| Keeps the configuration and update pages | pass | pass |
| One row per page, one column per step | pass | fail |
| Every cell a check or a stated reason | partial | fail |
| Names `git diff --name-only` | pass | pass |

- The discovery filter does not discriminate: the baseline lists the same six pages, drops the same files, and infers the same command from the skill's `allowed-tools`.
- The table does: the baseline produced a page-pair table with a "Reviewed in full: Yes" column, and no per-step columns, so a missing examples or re-verify pass would not be visible. The changed skill produced the six step columns.
- "Partial" because the changed skill's cells read "pending" (with "n/a unless edits applied" for re-verify), which is a stated status but not a check or a reason.
- One run per configuration is a signal, not a measurement.
- The eval 5 prompt was reworded after these runs to say the fixture is the unfiltered name-only diff of `docs` (the skill's `'docs/*.md'` pathspec was added after the runs and would drop the snapshot and `sidebar.json` entries the fixture contains). The runs were made with the earlier wording, "the output of the PR page discovery command", and were not repeated.

### Eval 5, second version: renames, deletions, and the discovery command (#8910, Copilot's tenth to twelfth reviews)

Copilot found that `--diff-filter=D` omits the old path of a rename (git reports it as `R`), so links and sidebar entries that still point at the old URL would go unchecked; that the eval prompt asked for "only the page list and table" while an expectation required naming the discovery command; that an expectation still accepted `--name-only`, which cannot tell a deletion from a rename; and that a run which pre-filled check marks for steps nobody had run should not score as passing. The rule now uses `git diff --name-status -M`, treats a deleted page and the old path of a rename as removed pages with their own rows, and says to mark a cell with a check only once that step has run (otherwise `pending` or a stated reason). The fixture is unfiltered name-status output (modified pages, one rename, one deletion, plus `sidebar.json` and snapshot entries), the prompt asks for the discovery command with the page list and table, and there are eight expectations. One run per configuration, graded by the author (not blind); the earlier version's results above used a name-only fixture.

| Check | Changed skill, first run | Changed skill, after the cell rule | Baseline |
| --- | --- | --- | --- |
| Lists the six modified pages | pass | pass | pass |
| Drops sidebar.json and snapshots | pass | pass | pass |
| Keeps the configuration and update pages | pass | pass | pass |
| One row per page, one column per step | pass | pass | fail |
| Check only for a step that has run, otherwise a stated status or reason | fail | pass | fail |
| Names `git diff --name-status -M` | pass | pass | pass |
| Rename: new path reviewed, old path its own removed-page row | pass | pass | fail |
| Deleted page: own row with a reason, no step marks | pass | pass | pass |
| Total | 7/8 | 8/8 | 5/8 |

- The first changed-skill run marked every review step with a check although nothing had been reviewed, because the prompt said not to review. That is the "assumption that the step was done" the expectation rules out, so it scores 7 of 8, not the 8 of 8 first recorded here.
- Adding "mark a cell with a check only once that step has run; until then write `pending`" to the rule, and rerunning the changed skill once, gave `pending` in every step cell and an explicit "nothing has run yet" line: 8 of 8.
- The baseline produced a Page / Status / Reviewed-as table with no step columns, so it has no step cells to fill, and noted the rename's old path only inside the new path's row. It did list the deleted page with a reason, but as a list item, not a table row. It names the status-aware command because the prompt gives it.
- One run per configuration is a signal, not a measurement. The eval still does not test that every step is then run on every page.
