# Skill Benchmark: deephaven-docs-review-full

**Model**: claude-opus-5-5
**Date**: 2026-09-29T20:57:56Z
**Evals**: 2 (3 runs each per configuration)

## Summary

| Metric | With Skill | Old Skill | Delta |
|--------|------------|---------------|-------|
| Pass Rate | 91% ± 0% | 97% ± 5% | -0.06 |
| Time | 481.3s ± 53.5s | 476.7s ± 89.2s | +4.6s |
| Tokens | 166065 ± 8589 | 151906 ± 10064 | +14158 |

## Notes

- Result: regression. PR skills 30/33 vs pre-PR 32/33. Every expectation about correctness passed in all 6 runs: the demo description, the invalid code, the decorator trap, the unreleased scope in both languages, the GUI claim, terminology, links and anchors, and well-formedness.
- The one failing expectation (no hedges or caveats added to the concept narrative) failed in 3/3 PR runs vs 1/3 pre-PR. The PR skill's developmental pass judged the page 'needs restructuring' (2 of 3 runs), and in edit mode the runs acted on that verdict: they rewrote sections, replaced the demo with new ticking examples they couldn't run, and retitled the page (2 of 3). The prose they wrote accreted asides and caveats. The placement gate covered caveats that came from findings, not ones the reviewer wrote itself.
- PR runs changed about twice as much text (136-156 lines added, 152-211 removed, vs 89-101 added and 61-68 removed), with a larger review burden and unrun examples.
- The trap held in every run: no run changed @liveness_scope() to the pydoc's bare @liveness_scope, and several reported that the pydoc and the reference page are the ones that are wrong.
- Automated checks (balanced fences, link targets, anchors) were clean for all 6 runs.