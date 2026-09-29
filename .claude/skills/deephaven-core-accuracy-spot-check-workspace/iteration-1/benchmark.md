# Skill Benchmark: deephaven-core-accuracy-spot-check

**Model**: claude-opus-5-5
**Date**: 2026-09-29T19:16:13Z
**Evals**: 1 (1 runs each per configuration)

## Summary

| Metric | With Skill | Old Skill | Delta |
|--------|------------|---------------|-------|
| Pass Rate | 100% ± 0% | 67% ± 0% | +0.33 |
| Time | 98.7s ± 0.0s | 89.8s ± 0.0s | +8.9s |
| Tokens | 72879 ± 0 | 64480 ± 0 | +8399 |

## Notes

- Both arms caught the wrong 1M threshold and traced it to SelectColumnLayer (>=, added+modified rows). Only the placement expectation discriminates: the pre-PR replacement inlines QueryTable.minimumParallelSelectRows and 4,194,304 into concept-guide prose.
- Both arms independently flagged the same stale claim in query-table-configuration.md:114 (filed as DOC-1522).
- n=1 run per configuration: pass rates show whether each targeted behavior appeared, not a statistically meaningful rate.
