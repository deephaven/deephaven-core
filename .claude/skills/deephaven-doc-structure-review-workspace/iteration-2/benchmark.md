# Skill Benchmark: deephaven-doc-structure-review

**Model**: claude-opus-5-5
**Date**: 2026-09-29T19:39:03Z
**Evals**: 1 (1 runs each per configuration)

## Summary

| Metric | With Skill | Old Skill | Delta |
|--------|------------|---------------|-------|
| Pass Rate | 100% ± 0% | 33% ± 0% | +0.67 |
| Time | 196.6s ± 0.0s | 157.0s ± 0.0s | +39.6s |
| Tokens | 87706 ± 0 | 80428 ± 0 | +7278 |

## Notes

- Blind regrade: new skill 6/6, pre-PR skill 2/6. The split confirms the iteration-1 lenient call: the pre-PR run passes the GIL half and fails the config half, because it proposed adding property names to the breaking-change callout.
- The independent grader also failed the pre-PR run on the purpose statement, mixed-kind table rows, and list/section mismatch, matching the iteration-1 inline grading.
- n=1 per configuration (iteration-1 outputs).