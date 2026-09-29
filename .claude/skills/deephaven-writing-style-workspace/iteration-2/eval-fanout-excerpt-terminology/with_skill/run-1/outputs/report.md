# Style review: `fanout-excerpt.md`

**Category:** Concept guide (as you described it). The tone profile is explanatory, and property names and defaults stay out of the narrative. I found no curly quotes, en dashes or badly spaced em dashes. Both headings use sentence case.

**Mechanical checks (run as literal greps):**
- `` `\.[a-z] `` (dot-prefixed method in prose): no hits.
- Backticked identifier with empty `()` in prose: no hits.
- `[here]` / `[click here]` / `[this page]`: no hits.
- First-mention links: the file has no links at all. Two identifiers have reference pages and are bare on first mention (see finding 5).

## Findings

### 1. Coined terms "row fan-out" and "column fan-out" (lines 5, 7, 9, 11)
These are invented labels, and a reader can't search for them. The phrase "fan-out" appears nowhere in `docs/python`. The existing concept guide, `conceptual/query-engine/parallelization.md`, uses "parallelize"/"parallelism" throughout. The heading "Which formulas the engine can fan out" (line 7) carries the coined term into the page structure as well.

**Fix:** Use standard terms and name them once in the intro. For example: "The engine parallelizes formula evaluation in two ways: concurrent row calculations, where it splits one column's rows into ranges that separate threads evaluate, and concurrent column calculations, where it evaluates independent columns in the same `update` on separate threads." Retitle the heading to something like "Which formulas the engine can parallelize". Replace "row fan-out" on lines 9 and 11 to match.

### 2. The definition has a standard name: "pure function" (line 9)
"any formula whose output depends only on its arguments and that changes nothing outside itself" is the definition of a pure function. Name the recognized term so the reader can connect it to what they already know. For example: "The engine can parallelize any formula that is a pure function — its output depends only on its arguments, and it has no side effects."

### 3. "Thread-safe" and "stateless" used as if they were synonyms (lines 9 and 15)
Line 9 says the qualifying formulas are "thread-safe". Line 15 describes the non-qualifying formula as not "stateless". The two words describe different properties. A formula can be thread-safe, for example by using an atomic counter, and still carry state, and its results would still depend on evaluation order. Swapping the terms tells the reader they mean the same thing.

**Fix:** Pick one term for the property the section is about, and use it on both lines. Line 9's reasoning ("never changes their results") depends on purity or statelessness, not on thread-safety. Change line 9 to "Because these formulas are stateless" (or "pure"), and keep line 15 consistent with that choice. The existing parallelization guide uses "stateless" 12 times, so "stateless" fits the rest of the doc set best. If you choose it, define it once with the pure-function wording from finding 2 and don't switch terms afterward.

### 4. Configuration injection in the narrative (line 11)
"You can tune that size with `QueryTable.minimumParallelSelectRows`, which defaults to about 4.2 million rows." A Concept guide should not name a property or its default in the explanation. Moving the property into its own sentence doesn't fix that; it's still configuration injection. The hedge "about" also loosens a value that the configuration reference gives exactly (`1L << 22` in `conceptual/query-table-configuration.md`).

**Fix:** End the paragraph at the level of the idea, and send readers to the configuration reference instead of the property:

> Row-level parallelism only happens once a table is large enough to be worth dividing. See [Query table configuration](../query-table-configuration.md) for the setting that controls this threshold.

Or put the property and default in a single Configuration section at the end of the page. Don't bring the property name back anywhere in the narrative.

### 5. The first mentions of `update` and `with_serial` aren't linked (lines 5 and 15)
Both have reference pages. Link the first occurrence of each:
- `update` (line 5) → `reference/table-operations/select/update.md`
- `with_serial` (line 15) → `reference/query-language/types/ConcurrencyControl.md#with_serial`

Adjust the relative paths to wherever the page lives under `conceptual/`.

### 6. Passive voice in the definition (line 5)
"one column's rows are divided into ranges that different threads evaluate": the engine does the dividing, so make it the subject. "With column fan-out, independent columns in the same `update` are evaluated on different threads" has the same problem. Rewrite both actively, as in the suggested sentence in finding 1: "the engine splits…", "the engine evaluates…".

### 7. Wording on line 15: "isn't stateless"
The double negative makes the sentence harder to read, and it depends on the term choice in finding 3. Consider: "A formula that increments a global counter has state, so two threads evaluating it at once can read the same value." Or, if you chose "pure function": "…isn't a pure function, so…". The rest of the sentence is fine. `with_serial` has no leading dot and no empty parentheses, which is correct.

### 8. Related documentation section (page-level)
This is an excerpt, so it may not include the end of the page. A published concept guide needs a "Related documentation" section at the end. Good candidates are the parallelization concept guide, `ConcurrencyControl`, `update`, and query table configuration.

## Summary
The most important issues are terminology (findings 1–3) and the configuration detail on line 11 (finding 4). The coined "fan-out" labels, the unnamed pure-function definition and the thread-safe/stateless swap together leave the reader unsure what property actually makes a formula safe to parallelize. Nothing in the mechanical checks (dot-prefixed methods, empty parentheses, "here" links) turned up a problem.