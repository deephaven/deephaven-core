# Skill Benchmark: deephaven-doc-structure-review

**Model**: claude-opus-5-5
**Date**: 2026-09-29T19:16:13Z
**Evals**: 1 (1 runs each per configuration)

## Summary

| Metric | With Skill | Old Skill | Delta |
|--------|------------|---------------|-------|
| Pass Rate | 100% ± 0% | 40% ± 0% | +0.60 |
| Time | 196.6s ± 0.0s | 157.0s ± 0.0s | +39.6s |
| Tokens | 87706 ± 0 | 80428 ± 0 | +7278 |

## Notes

- Discriminating: purpose statement, mixed-kind table rows, list/section mismatch. The pre-PR run also proposed adding property names to the breaking-change callout — the opposite of the placement rule.
- Expectation 3 passed for the pre-PR skill only leniently (it moved the GIL note but treated config values as content to add, not relocate). Consider splitting it into GIL and config expectations next iteration.
- n=1 run per configuration: pass rates show whether each targeted behavior appeared, not a statistically meaningful rate.
