# Style review: `fanout-excerpt.md`

**Category:** Concept guide. The page is a draft excerpt for `docs/{python,groovy}/conceptual/`, and its content explains *how and why* the engine spreads formula work over threads. That means an explanatory tone, contractions allowed, and the reader can be expected to know more once a term has been defined.

**Mechanical searches (all clean):**
- No dot-prefixed method references in prose.
- No empty `()` after a backticked identifier in prose.
- No `[here]`, `[click here]` or `[this page]` link labels.
- No smart or curly quotes.
- Headings use sentence case and contain no links.

## Findings

### 1. The terms "row fan-out" and "column fan-out" are made up (lines 5, 7, 9, 11)
The draft defines them in the intro, but defining a made-up term isn't enough when standard ones exist. "fan-out" and "fan out" appear **0 times** in `docs/python` and `docs/groovy`. The existing concept guide on this topic (`conceptual/query-engine/parallelization.md`) says "parallelize", "parallelization", "in parallel" and "concurrently". A reader who searches for "fan-out" finds nothing else on the site.
- Suggested replacements: "concurrent row calculations" (or "row-level parallelism") and "concurrent column calculations" (or "column-level parallelism").
- Heading, line 7: "Which formulas the engine can fan out" → "Which formulas the engine can parallelize".

### 2. The page describes a known concept without naming it (line 9)
"any formula whose output depends only on its arguments and that changes nothing outside itself" is the standard definition of a **pure function**. The Deephaven docs call this property **stateless**:
- It appears in 15 doc files.
- `parallelization.md` defines it: "does not depend on any mutable external inputs or the order in which rows are evaluated".
- It matches the config property `QueryTable.statelessSelectByDefault`.

Name the term, for example: "The engine can parallelize any *stateless* formula — one whose output depends only on its arguments and that changes nothing outside itself (a pure function)."

### 3. "Thread-safe" and "stateless" are used as if they were synonyms (lines 9 and 15)
- Line 9 says the qualifying formulas are "**thread-safe**".
- Line 15 says a formula that increments a global counter "isn't **stateless**".

Read together, these tell the reader the two words mean the same thing, and they don't. A formula can be thread-safe (for example, a counter protected by a lock) and still be stateful, and a stateful formula still needs `with_serial` to keep row order. Pick **stateless** and use it in both places.
- Line 9: "Because these formulas are stateless, parallel evaluation never changes their results."

### 4. Passive voice in the intro (line 5)
- "one column's rows are divided into ranges that different threads evaluate" → "the engine divides one column's rows into ranges and evaluates each range on a different thread."
- "independent columns in the same `update` are evaluated on different threads" → "the engine evaluates independent columns in the same `update` on different threads."

The actor (the engine) is known and matters to the reader, so use the active voice.

### 5. Awkward negative (line 15)
"isn't stateless" → "is **stateful**". It's more direct, and it's the Deephaven docs' own opposite term (`parallelization.md` uses "stateful").

### 6. Hedged number (line 11)
"defaults to about 4.2 million rows" is vague. The default is exactly `1L << 22` (4,194,304). `QueryTable.java:337` sets it, and `conceptual/query-table-configuration.md` documents it. State the number exactly: "defaults to 4,194,304 (`1L << 22`) rows". I didn't check the page's other claims for accuracy; a style review doesn't cover that.

### 7. Method references aren't linked on first mention
- `update` (line 5, its first and only mention) should link to `reference/table-operations/select/update.md`.
- `with_serial` (line 15, first and only mention) should link to its reference entry, `reference/query-language/types/ConcurrencyControl.md#with_serial`.
- Optional: link `QueryTable.minimumParallelSelectRows` (line 11) to its section in `conceptual/query-table-configuration.md`. It's a config property, not a method, so the link rule doesn't strictly require this, but it's a natural place to send the reader.

### 8. Vague heading (line 13)
"Formulas that need care" doesn't say what kind of care. Suggested: "Formulas that must run serially" or "Stateful formulas".

### 9. Related documentation section (page structure)
The excerpt has no "Related documentation" section. That's fine if the full page has one. If this is the whole page, it needs one at the end. A concept guide isn't exempt from this rule. Good candidates:
- `parallelization.md`
- `how-to-guides/performance/formula-threads.md`
- the `ConcurrencyControl` reference
- `query-table-configuration.md`

## What's fine
- The contraction "isn't" is acceptable here; only reference pages forbid contractions.
- The global-counter example (line 15) is concrete and explains the risk clearly: "two threads … can both read the same value".
- Code formatting: the backticked identifiers and the config key are formatted correctly.