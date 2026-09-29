# Skill Benchmark: deephaven-docs-review-full

**Model**: claude-opus-5-5
**Date**: 2026-09-29T21:15:32Z
**Evals**: 2 (3 runs each per configuration)

## Summary

| Metric | With Skill | Old Skill | Delta |
|--------|------------|---------------|-------|
| Pass Rate | 94% ± 10% | 83% ± 0% | +0.11 |
| Time | 422.6s ± 118.8s | 476.7s ± 89.2s | -54.0s |
| Tokens | 155674 ± 16752 | 151906 ± 10064 | +3768 |

## Notes

- Result: fixed skill 34/36 vs pre-PR 30/36. 'Edits stay targeted' 3/3 vs 0/3; 'no hedges or caveats added' 3/3 vs 0/3.
- The fix worked. No new-skill run retitled the page, replaced the demo, or reordered sections, and each reported its restructuring ideas instead. The new-skill diffs shrank to 39-59 lines added (iteration 3: 136-156; baseline about 95).
- Overcorrection in 1 of 3 runs: it deferred two small real fixes (the module-level `return table` and adding release() to the demo) as 'beyond fix, not rewrite'. The other two runs made both fixes, so the rule allows small example fixes, but that one run read it too broadly. Worth tightening: fixing broken code in an existing example is a targeted fix.
- Grader variance: the iteration-3 grader passed 2 of 3 of these same baseline runs on 'no hedges or caveats added'; this grader failed all 3. The strict call applies to both arms equally here, but the baseline's 0/3 on that expectation partly reflects the stricter grader.
- The 'edits stay targeted' expectation was added after iteration 3 exposed the rewrites. The baseline fails it (retitles, reorders, merges sections).