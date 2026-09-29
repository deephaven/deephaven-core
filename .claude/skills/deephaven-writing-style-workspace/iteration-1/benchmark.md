# Skill Benchmark: deephaven-writing-style

**Model**: claude-opus-5-5
**Date**: 2026-09-29T19:16:13Z
**Evals**: 1 (1 runs each per configuration)

## Summary

| Metric | With Skill | Old Skill | Delta |
|--------|------------|---------------|-------|
| Pass Rate | 100% ± 0% | 60% ± 0% | +0.40 |
| Time | 71.8s ± 0.0s | 91.0s ± 0.0s | -19.2s |
| Tokens | 64668 ± 0 | 66085 ± 0 | -1417 |

## Notes

- Non-discriminating: stateless/thread-safe interchange, the config parenthetical, and the .with_serial() control all passed in both arms. The fixture states the stateless=thread-safe claim so bluntly that the pre-PR skill caught it; a subtler fixture would test the new rule better.
- Discriminating: only the new skill proposed recognized terms ('concurrent row calculations', 'pure function'); the pre-PR run called 'across rows' 'good plain-language labels'.
- n=1 run per configuration: pass rates show whether each targeted behavior appeared, not a statistically meaningful rate.
