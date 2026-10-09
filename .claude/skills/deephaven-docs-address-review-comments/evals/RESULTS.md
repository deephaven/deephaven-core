# Eval results: `deephaven-docs-address-review-comments`

Evals for this skill are in [`evals.json`](./evals.json), and the documents they run against are in [`files/`](./files/) and the shared [`eval-fixtures/`](../../eval-fixtures/) folder. They were written and run with Anthropic's skill-creator tooling for [DOC-1524](https://deephaven.atlassian.net/browse/DOC-1524) (PR #8704), which refined the review skills after Chip Kent's review of the parallelization docs (PR #7457).

Raw run outputs, grading files, and skill snapshots are kept out of the repo. This file is the record. "Baseline" means the skill before the change being tested; each iteration names it.

## Setup

Ten Copilot-style comments modeled on real comments from the parallelization PR, against the Crash Course chapter at `f2ef483084`:
- four are real defects (C1–C3, and C9's overstated takeaway);
- the rest are true but misplaced detail, a title-case change that conflicts with the Crash Course convention, a request that conflicts with an earlier round, and a "this claim is unsupported" comment about a claim the javadoc supports.

Both arms chose their own skills from a directory. The baseline had the existing skills, without this one.

| | New skill | Existing skills |
| --- | --- | --- |
| Iteration 1 (as first written) | 21/24 | 22/24 |
| Iteration 2 (after fixes) | 23/24 | 22/24 |

- **Iteration 1:** the existing skills already carried most of the judgment. The skill's "redirect by default" rule cost one run the `with_serial` fix, and one run proposed an anchor that doesn't exist.
- **Iteration 2:** fixed both. Overstated prescriptions are now Apply even in late rounds, and proposed anchors are verified. It also stopped runs deleting a true claim a reviewer called unsupported: 3/3 vs 1/3 for the baseline. The baseline runs predate the accuracy check's "unexplained is not unsupported" rule, so that gain measures both changes together.
- **Scoring change:** the C9 expectation was tightened after review, so declining a real defect now fails. One new-skill run drops, from 24/24 to 23/24. The skill now also counts a takeaway that contradicts the page's own body as Apply. That change hasn't been re-run.

## Caveats

- Runs are small, so treat pass counts as evidence that a behavior appears, not as stable rates.
- Both iterations were 3 runs per configuration. Iteration 2 was scored blind by an independent grader agent (reports shuffled and unlabeled).
- Judgment-call expectations can vary between graders; the notes say where.

## DOC-1560 (PR #8860): whole-page accuracy and audit fixes

Method: "main" is the skill on `main`; "new" is the PR branch. Each trial read the skill from its configuration's folder and verified against the same checkout. A separate grader scored each eval's reports blind (shuffled, unlabeled) against the expectations. Two to three runs per cell unless noted, so treat counts as direction, not rates.

| Eval | What it tests | main | new |
| --- | --- | --- | --- |
| 1 (regression) | Copilot triage on the Crash Course parallelization page | 15/16 | 14/16 |
| 2 (new) | After a merge with #8798, find and restore the corrections the resolution dropped (edit mode) | 8/12 | 12/12 |

- Eval 2, first try: every run in both configurations saw that the join TIP started with #8798's sentence and missed that its last clause ("so `natural_join` should be preferred in most places") was gone, and one new run rewrote a correction that had survived in substance (new 8/12, main 10/12). The merge rule now says to compare whole sentences and to leave surviving corrections alone. Rerun: new 12/12, main 8/12; both new runs restored the TIP clause and the "required" marking and changed nothing else.
- Eval 1: a single new run first scored 6/8 (it declined C9 and deleted the C10 clause). The two-run rerun against `main` scored 14/16 against 15/16, so that was noise, not a regression.
- Eval 2 answers the open request for an edit-mode eval.

### Rerun after removing eval answers from the skill text

Copilot pointed out that several worked examples in the skills named the exact defects these evals look for, so an agent could pass by repeating the example. The examples are now general, and the new-skill runs were repeated (2 runs each; `main` is unchanged, so its scores carry over). These rows replace the ones above as the measure of the rules themselves.

| Eval | main | new, examples named the answer | new, general examples |
| --- | --- | --- | --- |
| 2 | 8/12 | 12/12 | 10/12, then 9/12 |

- Without the example, no run restored the join TIP's lost conclusion. The merged TIP ends with a conditional ("if each left row needs at most one right match, use `natural_join` instead"), which every run judged to have kept #8798's point in substance. Rewording the rule to read the diff's `-` and `+` lines together didn't change that, and one rerun also missed the `aggs` "required" marking. The remaining gain over `main` is in marking `aggs` required and in not rewriting surviving corrections; catching a weakened conclusion still depends on judgment. Not tuned further, to avoid fitting the rule to this one fixture.

### Correction: expectation 2 was wrong

Copilot pointed out, and the fixture diff confirms, that #8798 didn't change the join TIP's closing clause ("so `natural_join` should be preferred in most places"): it appears unchanged on both the `-` and `+` lines, and #8798 changed only the explanation of how `join` works. The branch's conditional rewording ("If each left row needs at most one right match, use `natural_join` instead") is the branch's own edit, so leaving it is correct. Expectation 2 is now a control: don't treat that clause as a lost #8798 correction. Rescored from the graders' recorded reasons (no rerun; each grader said whether the run restored the clause):

| Configuration | Old score | Rescored |
| --- | --- | --- |
| `main` (2 runs) | 8/12 | 10/12 |
| new, examples named the answer (2 runs) | 12/12 | 10/12 |
| new, general examples (2 runs) | 10/12 | 12/12 |
| new, general examples, after the `-`/`+` rule (2 runs) | 9/12 | 11/12 |

So the runs that "missed" the TIP handled the merge correctly, and the version whose example named the clause was taught to restore something that wasn't lost. The objective lost correction in this fixture is the `aggs` "required" marking.

- Coverage gap: no eval exercises the pre-push sweep (step 5), where an edit-mode run must catch a pre-existing defect elsewhere on the page that no comment mentions. Copilot flagged this on #8860; it's follow-up work.
