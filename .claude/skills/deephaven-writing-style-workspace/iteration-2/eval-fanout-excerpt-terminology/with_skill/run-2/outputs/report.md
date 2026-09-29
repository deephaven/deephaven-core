# Style review: `fanout-excerpt.md` ("Formula evaluation and threads")

**Category:** Concept guide, as you described it. The file isn't under `docs/{python,groovy}/conceptual/`, so I couldn't confirm the category from its directory. The concept-guide tone applies: explanatory, and contractions are fine. That also means no configuration detail in the narrative.

**Scope:** style only. I didn't check technical accuracy, such as whether the property name and default exist. Use `deephaven-core-accuracy-check` for that. I made no edits.

## Findings

### 1. The two key terms are made-up names, not standard terminology (line 5, and throughout)
"Row fan-out" and "column fan-out" are coined labels. The whole page is built on them. "Fan out" appears nowhere in `docs/python`, so a reader can't search for these terms or match them to any other Deephaven page. The terms also fail the test of meaning the same thing to someone who hasn't read the rest of the page. The line-5 definitions help, but defining a term isn't enough when a recognized one already exists.

- **Suggested:** use descriptive terms. For example, "concurrent row calculations" (one column's rows split across threads) and "concurrent column calculations" (independent columns evaluated at the same time). Frame both as kinds of *parallelization*, the term the existing `conceptual/query-engine/parallelization.md` uses.
- The verb form "fan out" also appears in the heading on line 7 ("Which formulas the engine can fan out") and on line 9 ("The engine can fan out any formula…"). Change these along with the nouns, e.g. "Which formulas the engine can evaluate in parallel."

### 2. A long description stands in for a standard term (line 9)
"any formula whose output depends only on its arguments and that changes nothing outside itself" is the definition of a **pure function**. Name the term and give the plain-language gloss once:
> The engine can parallelize any formula that is a *pure function* — its output depends only on its arguments, and it changes nothing outside itself.

### 3. The page switches between "thread-safe" and "stateless" as if they meant the same thing (lines 9 and 15)
Line 9 calls the qualifying formulas "thread-safe." Line 15 calls the problem formula "isn't stateless." These are different properties. A formula can be thread-safe, for example by using a synchronized counter, and still not be stateless, and it would still give order-dependent results. Using the two words interchangeably tells the reader they're the same thing.
- **Suggested:** pick one term that matches what the section actually requires and use it in both places. The existing parallelization guide uses "stateless" (12 times), so line 9 would become "Because these formulas are stateless, …". You could also use "pure" from finding 2 in both places.

### 4. A configuration property is placed in the middle of the explanation (line 11)
> You can tune that size with `QueryTable.minimumParallelSelectRows`, which defaults to about 4.2 million rows.

This drops a property name and its default into a concept explanation. Moving it to its own sentence in the same paragraph doesn't fix that, and it's already its own sentence here. The sentence before it already makes the point at the right level of detail ("once a table is large enough to be worth dividing").
- **Suggested:** delete this sentence from the narrative. Put the property and default in a Configuration section at the end of the page, or link to the configuration reference (`conceptual/query-table-configuration.md`). Don't bring the property name back anywhere else in the narrative. If the reader needs a pointer at this point, one short clause is enough: "The threshold is configurable; see [Query table configuration](../conceptual/query-table-configuration.md)."

### 5. Passive voice where the actor is known (line 5)
The actor is the engine, and naming it makes the mechanism clearer.
- "one column's rows are divided into ranges that different threads evaluate" → "the engine divides one column's rows into ranges and evaluates each range on a different thread."
- "independent columns in the same `update` are evaluated on different threads" → "the engine evaluates independent columns in the same `update` on different threads."

### 6. Method names aren't linked on first mention (lines 5 and 15)
- `update` (line 5, its first and only mention) should link to `reference/table-operations/select/update.md`.
- `with_serial` (line 15, its first and only mention) should link to `reference/query-language/types/ConcurrencyControl.md#with_serial`, the same target `Barrier.md` uses.

### 7. "Related documentation" section (not shown in the excerpt)
A concept guide needs a "Related documentation" section at the end. The excerpt doesn't show the end of the page, so check it in the full draft. Good candidates are the parallelization guide, `ConcurrencyControl`, `update`, and query table configuration.

## Automated checks that found nothing
- Method names with a leading dot in prose (`` `.x ``): none.
- Empty `()` after method names in prose: none. `with_serial` and `update` are correctly written without parentheses.
- `[here]` / `[click here]` / `[this page]` link text: none.
- Curly quotes and dash spacing: no curly quotes, no en or em dashes.
- Headings use sentence case and contain no links.

## Suggested rewrite (lines 5–11, combining findings 1–6)
> The engine has two ways to spread formula evaluation over threads. With concurrent row calculations, the engine divides one column's rows into ranges and evaluates each range on a different thread. With concurrent column calculations, the engine evaluates independent columns in the same [`update`](../reference/table-operations/select/update.md) on different threads.
>
> ## Which formulas the engine can evaluate in parallel
>
> The engine can parallelize any formula that is stateless — its output depends only on its arguments, and it changes nothing outside itself. Formulas built from column arithmetic, string functions, and the built-in date-time functions all qualify. Because these formulas are stateless, evaluating their rows in parallel never changes their results.
>
> The engine only splits a column's rows across threads once the table is large enough to be worth dividing.

(Put the threshold property and its default in the Configuration section.)