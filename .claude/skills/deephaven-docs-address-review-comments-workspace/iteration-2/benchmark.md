# Skill Benchmark: deephaven-docs-address-review-comments

**Model**: claude-opus-5-5
**Date**: 2026-09-29T20:38:24Z
**Evals**: 1 (3 runs each per configuration)

## Summary

| Metric | With Skill | Old Skill | Delta |
|--------|------------|---------------|-------|
| Pass Rate | 100% ± 0% | 92% ± 7% | +0.08 |
| Time | 312.7s ± 16.3s | 293.1s ± 14.0s | +19.5s |
| Tokens | 125680 ± 4467 | 116186 ± 3608 | +9494 |

## Notes

- Result: new 24/24 vs baseline 22/24 (the baseline's 3 runs regraded blind; they scored 22/24 in iteration 1 too, so grading was consistent).
- C10 (reviewer calls a true claim unsupported): new 3/3 vs baseline 1/3 (iteration 1: 1/3 for both). The fix separates what a comment says about the page from what it says about the source.
- C3 (overstated with_serial prescription): new 3/3 (iteration 1: 2/3). An overstated prescription now counts as flatly wrong, so the late-round bar no longer pushes it to redirect.
- All 3 new runs checked their proposed anchors against the reviewed tree (step 4a). No run proposed a nonexistent anchor, where iteration 1 had one.
- Caveat: the accuracy-check 'unexplained is not unsupported' rule also reaches the baseline path, but the baseline runs here predate it. So the C10 gain measures the review-comment skill plus that rule together, against the old skill set.
- One run declined C9 and offered the tutorial-level rewording as optional; the grader counted that as a pass. Run cost is about 120-130k tokens and 5 minutes, similar to the baseline.
- Tightened after the fact, from the PR review: the C9 expectation now requires a fix to be proposed. Declining C9, or offering the rewording only as optional, fails. Under the tightened expectation, with_skill run-2 fails, so the new skill scores **23/24**. The 3 baseline runs all proposed the rewording and still score 22/24. The skill now also counts a takeaway that contradicts the page's own body as an Apply defect. That change hasn't been re-run yet.
