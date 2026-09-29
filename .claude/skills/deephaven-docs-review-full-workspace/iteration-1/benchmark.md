# Skill Benchmark: deephaven-docs-review-full

**Model**: claude-opus-5-5
**Date**: 2026-09-29T19:16:13Z
**Evals**: 1 (1 runs each per configuration)

## Summary

| Metric | With Skill | Old Skill | Delta |
|--------|------------|---------------|-------|
| Pass Rate | 100% ± 0% | 20% ± 0% | +0.80 |
| Time | 621.3s ± 0.0s | 765.6s ± 0.0s | -144.3s |
| Tokens | 182597 ± 0 | 193987 ± 0 | -11390 |

## Notes

- Discriminating on report shape (editorial summary + verdict, developmental notes, author queries) and on the key technical miss: the pre-PR full review did not flag the view/update_view/lazy_update misclassification.
- Style-as-patterns passed in both arms — the pre-PR style pass already grouped by issue type.
- The new skill's developmental pass surfaced a finding neither accuracy-check arm led with: the quick-reference table prescribes with_serial alone for shared resources across columns (A1). The pre-PR full review found it too (its A5), so it isn't unique to the new skill, but the new version ranked it as the single biggest issue.
- n=1 run per configuration: pass rates show whether each targeted behavior appeared, not a statistically meaningful rate.
