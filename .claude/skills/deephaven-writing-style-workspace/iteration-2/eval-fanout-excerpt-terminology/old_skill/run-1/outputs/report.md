# Style review: `fanout-excerpt.md`

**Category:** Concept guide. The title "Formula evaluation and threads" and the content explain how the engine spreads formula work across threads, which is engine mechanism. A concept guide can be explanatory and use contractions, and it can rely on technical vocabulary once that vocabulary is defined. This is a style review only. I made no edits and checked no facts, apart from one quick lookup noted under item 7.

## Mechanical checks (all clean)

- **Dot-prefixed method names in prose** (`` `\.[a-z] ``): none found.
- **Empty parentheses after a backticked identifier in prose** (`` `foo()` ``): none found.
- **Non-descriptive link labels** (`[here]`, `[click here]`, `[this page]`): none found.
- **Smart quotes:** none found.
- **Headings:** all are in sentence case, and none contain links.

## Findings

### 1. "Stateless" and "thread-safe" are used as if they meant the same thing (high)

- Line 9 describes a formula "whose output depends only on its arguments and that changes nothing outside itself". It then says "Because these formulas are **thread-safe**, row fan-out never changes their results."
- Line 15 describes the opposite case as "isn't **stateless**".

These are different properties. A thread-safe formula can still depend on row order or on shared mutable state; the locking just makes it safe to call from several threads. Swapping between the two words tells the reader they are interchangeable, and the argument on line 9 depends on the formula being stateless, not on it being thread-safe.

- **Fix:** pick "stateless" and use it every time. It is the term the existing corpus uses; `conceptual/query-engine/parallelization.md` defines "_stateless_" and uses it throughout.
- **Suggested wording for line 9:** "Because these formulas are stateless, row fan-out never changes their results."
- Line 9's defining clause is essentially the definition of a pure function. Either name it ("a pure function — its output depends only on its arguments, and it has no side effects") or name it as stateless. Don't leave the definition without its standard name.

### 2. "Fan-out" is a coined term where standard ones exist (high)

"Row fan-out", "column fan-out" and the verb "fan out" (line 5, the line 7 heading, lines 9 and 11) are made-up labels. "Fan-out" appears nowhere else in `docs/python`. The existing docs say "parallelize", "parallelization" and "evaluated concurrently". A reader searching for how Deephaven parallelizes `update` won't find "fan-out", and in the wider industry the word usually refers to messaging or DAG branching, not data parallelism.

- **Suggested terms:** "parallelize" and "parallel evaluation" for the general idea. For the two strategies, "concurrent row calculations" (row ranges split across threads) and "concurrent column calculations" (independent columns evaluated at the same time).
- **Heading on line 7:** "Which formulas the engine can parallelize".
- **Line 11:** "Rows are only split across threads once a table is large enough..."
- Once you pick the terms, use them consistently across the page.

### 3. Passive voice in the opening definitions (medium)

Line 5 has two passive constructions where the actor (the engine) is known and relevant:

- "one column's rows are divided into ranges that different threads evaluate"
  - Suggested: "the engine divides one column's rows into ranges and evaluates each range on a different thread."
- "independent columns in the same `update` are evaluated on different threads"
  - Suggested: "the engine evaluates independent columns in the same `update` on different threads."

### 4. First mentions of methods aren't linked (medium)

- **`update`** (line 5): link the first mention to `../../reference/table-operations/select/update.md`. Adjust the relative path for where this page ends up; other conceptual pages already link it this way.
- **`with_serial`** (line 15): link the first mention to the `with_serial` section of `reference/query-language/types/ConcurrencyControl.md`, or to the `ConcurrencyControl` pydoc.

### 5. The configuration property has no pointer (low)

`QueryTable.minimumParallelSelectRows` (line 11) is backticked correctly. Consider linking it to its entry in `conceptual/query-table-configuration.md` ("Parallel processing with select") so the reader can see how to set it.

### 6. Double negative on line 15 (low)

"isn't stateless" makes the reader invert it in their head. Suggested: "A formula that increments a global counter is **stateful**, so two threads evaluating it at once can both read the same value."

### 7. Hedged number on line 11 (low)

"defaults to about 4.2 million rows" is a vague figure for a value that is exact. From my quick check, the source default is `1L << 22`, which is 4,194,304 rows; `QueryTable.java:337` and `query-table-configuration.md` agree. Suggested: "defaults to 4,194,304 rows (2^22)." Have the accuracy check confirm the number itself.

### 8. "Related documentation" section (conditional)

This excerpt has none. That's fine for an excerpt, but a published concept guide needs one at the end. Likely entries:

- the Parallelization concept guide
- the `update` reference page
- the `ConcurrencyControl` / `with_serial` reference page
- the QueryTable configuration page

## Summary

The formatting is clean: no dot-prefixed methods, no empty parentheses, no bad link text, headings in sentence case and correct backticks. The real problems are with terminology:

1. "Stateless" and "thread-safe" are used as synonyms.
2. "Fan-out" is a coined label that the rest of the docs don't use.

Fix those two first, then the two passive constructions on line 5 and the missing first-mention links.