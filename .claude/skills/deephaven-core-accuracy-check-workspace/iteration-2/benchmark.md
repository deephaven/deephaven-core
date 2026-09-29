# Skill Benchmark: deephaven-core-accuracy-check

**Model**: claude-opus-5-5
**Date**: 2026-09-29T19:39:03Z
**Evals**: 1, 2 (3 runs each per configuration)

## Summary

| Metric | With Skill | Old Skill | Delta |
|--------|------------|---------------|-------|
| Pass Rate | 89% ± 17% | 80% ± 27% | +0.09 |
| Time | 400.2s ± 87.0s | 362.3s ± 54.5s | +37.9s |
| Tokens | 140517 ± 19146 | 138095 ± 16939 | +2422 |

## Notes

- Target expectation (with_serial alone insufficient for multi-column shared state): new skill 6/6 across both evals; iteration-1 skill 3/6 (2/3 on the original doc, 1/3 on the held-out Crash Course).
- Held-out Crash Course eval (not used to write the skill changes): new 3/3 vs iteration-1 1/3 on the target expectation, so the rule generalizes beyond the with_serial example quoted in the pitfall.
- Every other expectation passed in every run on the original doc (view/update_view classification, QueryTable guard, config placement, author queries). The iteration-1 changes held up under 3 runs.
- Control expectation on the held-out doc (threshold is per-update rows with >=) was noisy in both arms (new 1/3, iteration-1 2/3). It tests general thoroughness, not a rule this iteration changed.
- Grading was blind: an independent grader agent saw the six reports shuffled and labeled A-F.