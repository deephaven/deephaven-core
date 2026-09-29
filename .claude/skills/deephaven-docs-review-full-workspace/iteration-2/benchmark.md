# Skill Benchmark: deephaven-docs-review-full

**Model**: claude-opus-5-5
**Date**: 2026-09-29T19:39:03Z
**Evals**: 1 (3 runs each per configuration)

## Summary

| Metric | With Skill | Old Skill | Delta |
|--------|------------|---------------|-------|
| Pass Rate | 95% ± 8% | 86% ± 14% | +0.10 |
| Time | 488.3s ± 71.4s | 441.5s ± 25.1s | +46.9s |
| Tokens | 170952 ± 7376 | 162288 ± 6633 | +8663 |

## Notes

- Prescriptive quick-reference expectation: new 3/3 vs iteration-1 1/3. The fix propagates through the orchestrated accuracy step.
- No-reinjection expectation: 2/3 in both arms. The new-skill miss is a strict grader call: the proposed narrative wording 'uses all cores by default' was counted as a default in prose. An iteration-1 run failed for proposing a property default change in the breaking-change callout.
- Report-shape expectations (editorial summary and verdict, developmental notes first, view/update_view misclassification, author queries, style as patterns) passed in all 6 runs. The iteration-1 developmental pass and report format are stable across runs.
- Full reviews cost about 160-180k tokens and 7-10 minutes per run in both arms.