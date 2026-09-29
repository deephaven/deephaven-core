# Skill Benchmark: deephaven-core-accuracy-check

**Model**: claude-opus-5-5
**Date**: 2026-09-29T19:16:13Z
**Evals**: 1 (1 runs each per configuration)

## Summary

| Metric | With Skill | Old Skill | Delta |
|--------|------------|---------------|-------|
| Pass Rate | 100% ± 0% | 0% ± 0% | +1.00 |
| Time | 477.7s ± 0.0s | 684.3s ± 0.0s | -206.6s |
| Tokens | 148017 ± 0 | 169243 ± 0 | -21226 |

## Notes

- Discriminating: the pre-PR skill never questioned the view/update_view/lazy_update 'not parallelized' classification, even though it read and quoted the same QueryTable guard (to confirm with_serial can't be used with views).
- Both arms found the same large set of other real defects (GIL overstatement, sort init-only, missing update_by/range_join/snapshots, invented counter output, Deephaven 40 claim). The skill change targets the mental-model miss, not general thoroughness.
- The pre-PR run took longer and used more tokens (684s/169k vs 478s/148k). With n=1 that's noise, not a trend.
- n=1 run per configuration: pass rates show whether each targeted behavior appeared, not a statistically meaningful rate.
