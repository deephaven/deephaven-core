# Skill Benchmark: deephaven-writing-style

**Model**: claude-opus-5-5
**Date**: 2026-09-29T19:39:03Z
**Evals**: 1, 2 (3 runs each per configuration)

## Summary

| Metric | With Skill | Old Skill | Delta |
|--------|------------|---------------|-------|
| Pass Rate | 100% ± 0% | 78% ± 16% | +0.22 |
| Time | 72.7s ± 10.5s | 74.0s ± 6.8s | -1.3s |
| Tokens | 67966 ± 2522 | 65602 ± 2342 | +2364 |

## Notes

- No-reinjection expectation: new skill 6/6 across both fixtures; iteration-1 skill 1/6. The iteration-1 skill proposed the property as a separate narrative sentence (parallel excerpt) or suggested writing out the exact default inline (fan-out excerpt).
- Fan-out fixture (subtler than iteration 1's): the iteration-1 skill also failed to flag the standalone config sentence as misplaced in 2/3 runs; the new skill flagged it 3/3.
- Terminology expectations (coined labels, pure function, stateless vs thread-safe) passed in all 12 runs, including the subtler fixture. Those rules were already in iteration 1 and still hold.