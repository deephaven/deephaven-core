# Skill Benchmark: deephaven-docs-address-review-comments

**Model**: claude-opus-5-5
**Date**: 2026-09-29T20:29:40Z
**Evals**: 1 (3 runs each per configuration)

## Summary

| Metric | With Skill | Old Skill | Delta |
|--------|------------|---------------|-------|
| Pass Rate | 88% ± 12% | 92% ± 7% | -0.04 |
| Time | 264.7s ± 20.7s | 293.1s ± 14.0s | -28.5s |
| Tokens | 108272 ± 3461 | 116186 ± 3608 | -7913 |

## Notes

- Result: no improvement over the existing skills (new 21/24, baseline 22/24). The baseline picked accuracy-check + writing-style, whose placement and prescriptive-sufficiency rules already carry most of the judgment.
- Triggering works: all 3 with_skill runs selected this skill from the directory.
- C3 (with_serial insufficient across columns): new 2/3 vs baseline 3/3. One run redirected it: the skill's late-round 'redirect qualify-type comments by default' rule overrode the fact that an overstated prescription is flatly wrong.
- C10 (reviewer calls a true claim 'unsupported'): 1/3 in both arms. Runs treated 'the page doesn't explain it' as 'unsupported' and deleted a claim the SelectColumn.isParallelizable javadoc supports.
- One with_skill run proposed a link to an anchor that doesn't exist at f2ef483084; nothing in the skill asks it to verify its own proposed text.
- All runs declined the title-case change (Crash Course convention), kept config out of the tutorial narrative, declined enlarging the example, and fixed the takeaway at the right resolution.