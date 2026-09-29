# Skill Benchmark: deephaven-docs-review-full

**Model**: claude-opus-5-5
**Date**: 2026-09-29T21:48:10Z
**Evals**: 2 (3 runs each per configuration)

## Summary

| Metric | With Skill | Old Skill | Delta |
|--------|------------|---------------|-------|
| Pass Rate | 100% ± 0% | 89% ± 10% | +0.11 |
| Time | 337.1s ± 57.6s | 476.7s ± 89.2s | -139.5s |
| Tokens | 149894 ± 10269 | 151906 ± 10064 | -2013 |

## Notes

- Result: 36/36 vs 32/36. Every new-skill run made every targeted repair: the module-level return, the undefined name, preserve/release added to the demo in both languages. None retitled the page, replaced the demo, reordered sections, or added hedges.
- This fixes iteration 4's overcorrection, where 1 of 3 runs deferred two small real fixes as 'rewrites'.
- Diffs stayed small: 60-69 lines added, 34-43 removed (iteration 3: 136-156 added; baseline: about 95).
- Grader variance on the same three baseline runs: 32/33 (iteration 3), 30/36 (iteration 4), 32/36 (iteration 5). The hedge and 'targeted' expectations are the judgment calls; the correctness expectations were stable across graders.
- The GUI-only claim was handled by an author query in 2 of 3 new runs and by removal in the other, all of which the expectation accepts.