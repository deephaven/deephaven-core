# Eval results: `deephaven-docs-address-review-comments`

Evals for this skill are in [`evals.json`](./evals.json), and the documents they run against are in [`files/`](./files/). They were written and run with Anthropic's skill-creator tooling for [DOC-1524](https://deephaven.atlassian.net/browse/DOC-1524) (PR #8704), which refined the review skills after Chip Kent's review of the parallelization docs (PR #7457).

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

- Runs are small (1 run per configuration in iteration 1, 3 after that), so treat pass counts as evidence that a behavior appears, not as stable rates.
- From iteration 2 on, independent grader agents scored the reports blind (shuffled and unlabeled). Judgment-call expectations still vary between graders; the notes say where.
