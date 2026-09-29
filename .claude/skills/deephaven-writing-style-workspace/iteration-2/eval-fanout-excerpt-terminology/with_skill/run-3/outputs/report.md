# Style review: `fanout-excerpt.md`

**Category:** Concept guide, as you said, and it reads like one. I applied the concept-guide profile: an explanatory tone that builds a mental model, and no configuration detail in the narrative. I also assumed Python, since it uses `with_serial`. I made no edits.

## Findings

### 1. Coined terms where standard ones exist: "row fan-out" / "column fan-out" (lines 5, 7, 9, 11)
The whole excerpt rests on "fan-out", a label invented for this page. "Fan-out" and "fan out" appear **0 times** in `docs/python` and `docs/groovy`. "Concurrent" appears 227 times, and the parallelization concept guide (`conceptual/query-engine/parallelization.md`) uses "parallelize". A reader searching the docs, or coming from general CS, won't find or recognize "row fan-out". The heading "Which formulas the engine can fan out" has the same problem.

- Suggest: "concurrent row calculations" (the rows of one column split across threads) and "concurrent column calculations" (independent columns in one `update` evaluated on separate threads). Alternatively, use "parallelize" as the verb, which matches `parallelization.md`.
- Heading suggestion: "Which formulas the engine can parallelize".

### 2. Describing a standard concept instead of naming it (line 9)
"any formula whose output depends only on its arguments and that changes nothing outside itself" is the definition of a **pure function**. Name the term and then add the plain-language gloss. For example: "The engine can parallelize any formula that is a pure function: its output depends only on its arguments, and it changes nothing outside itself."

### 3. Near-synonyms that aren't synonyms: "thread-safe" vs. "stateless" (lines 9, 15)
Line 9 justifies safe parallelization with "Because these formulas are **thread-safe**". Line 15 describes the opposite case as "isn't **stateless**". These are different properties, so using them interchangeably tells the reader they mean the same thing, and here that is misleading. A counter that uses an atomic increment is thread-safe but still not stateless: parallel evaluation still changes which row gets which value. The property that matters is statelessness, or purity.

"Stateless" is also the established term: 65 occurrences in the docs corpus against 7 for "thread-safe", and `parallelization.md` defines *stateless* as the condition for parallelization.

- Suggest line 9: "Because these formulas are stateless, evaluating their rows concurrently never changes their results."
- Keep "stateless" on line 15, and define it on first use (line 9) so line 15 reads as a reuse of the term.

### 4. Configuration injection (line 11)
`QueryTable.minimumParallelSelectRows` and its default ("about 4.2 million rows") sit in the middle of a conceptual explanation. In a concept guide, property names and defaults belong in a Configuration section at the end of the page, or behind a link to `conceptual/query-table-configuration.md`. That page already lists this property with its default, `1L << 22`. Moving the property to its own sentence in the same paragraph doesn't fix this, and neither does repeating it elsewhere in the narrative.

- Suggest line 11: "Concurrent row calculations only happen once a table is large enough to be worth dividing. The [Query table configuration guide](../query-table-configuration.md) covers how to tune that threshold." (Adjust the relative path to wherever the page lands.)

### 5. Passive voice (line 5)
- "one column's rows are divided into ranges that different threads evaluate" → "the engine divides one column's rows into ranges and evaluates each range on a different thread."
- "independent columns in the same `update` are evaluated on different threads" → "the engine evaluates independent columns in the same `update` on different threads."

The actor (the engine) is known and relevant in both sentences, so use active voice.

### 6. Method references: first mentions should link (lines 5, 15)
The mechanical searches found no dot-prefixed method names, no empty `()` in prose, no `[here]`-style link text, no curly quotes, and no misformatted dashes. Two identifiers are bare on first mention even though reference pages exist:

- `update` (line 5): link to `reference/table-operations/select/update.md`.
- `with_serial` (line 15): link to the `with_serial` section of `reference/query-language/types/ConcurrencyControl.md`.

### 7. Related documentation section (page level)
The excerpt has no "Related documentation" section. Concept guides require one. If the full page doesn't have it, add one at the end, linking at least to `parallelization.md`, `query-table-configuration.md`, `ConcurrencyControl.md`, and `update.md`.

## Passed checks
- Title and headings use sentence case and contain no links.
- Straight quotes only, and no misformatted dashes.
- Tone is explanatory and suits a concept guide. The contraction on line 15 ("isn't") is fine outside reference pages.
- Each paragraph carries one idea.
- `with_serial` appears in prose with no leading dot and no empty parentheses, which is correct.

## Out of scope (for an accuracy pass)
- The claim on line 15 that `with_serial` makes the engine evaluate rows "one at a time, in order". `ConcurrencyControl.md` separates serial evaluation from barriers, so run this through `deephaven-core-accuracy-check`.
- The excerpt says row parallelization "never changes their results" but never says whether column parallelization affects stateful formulas. That's a gap in content and structure, not style.