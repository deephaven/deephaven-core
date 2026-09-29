# Style review: `fanout-excerpt.md`

**File:** `.claude/skills/deephaven-writing-style/evals/files/fanout-excerpt.md`
**Category:** Concept guide, based on your description. The fixture isn't under `docs/{python,groovy}/conceptual/`, so I went by what you told me rather than the directory. Concept-guide tone applies: explanatory, contractions fine, analogy welcome.
**Scope:** Style only, reported without editing anything. I didn't verify the technical claims (that's `deephaven-core-accuracy-check`). Where a style problem also looks like it could be a factual one, I say so.

## Mechanical checks

| Search | Hits |
| --- | --- |
| `` `\.[a-z] `` (a leading dot on a method in prose) | None |
| Backticked identifier with empty `()` in prose | None |
| `[here]` / `[click here]` / `[this page]` link text | None |
| Smart or curly quotes | None |
| Em dash spacing | No dashes used |
| Future-tense "will" | None |
| Heading case | Both headings are sentence case, with no links in them |

## Findings

### 1. Terminology: "stateless" and "thread-safe" are used as if they mean the same thing (lines 9 and 15). High priority.

- **Line 9** describes a property ("output depends only on its arguments and … changes nothing outside itself") and then calls it **thread-safe**: "Because these formulas are thread-safe, row fan-out never changes their results."
- **Line 15** calls the opposite **not stateless**: "A formula that increments a global counter isn't stateless…".

These are different properties. A formula can be thread-safe and still stateful. For example, a counter with an atomic increment is thread-safe, but it gives different results depending on row order. So swapping the words tells the reader something false: that being thread-safe is what makes row fan-out safe. The reasoning in line 9 only works if the word is "stateless".

**Fix:** Name the property once, where line 9 defines it, and use that name everywhere after. "Stateless" is the term the doc set already uses for this. `conceptual/query-engine/parallelization.md` defines it (line 57), and `with_serial` / `ConcurrencyControl` are described in terms of it. You could also gloss it with the general CS term:

> The engine can fan out any *stateless* formula — a pure function whose output depends only on its arguments and that changes nothing outside itself. … Because these formulas are stateless, row fan-out never changes their results.

After that, don't use "thread-safe" as a synonym anywhere on the page.

### 2. Terminology: "row fan-out" and "column fan-out" are made-up labels (lines 5, 7, 9, 11). Medium to high priority.

The page defines both terms, but defining a term isn't enough if a standard one already exists. "Fan-out" appears nowhere else in `docs/python` or `docs/groovy`. The existing concept guide (`parallelization.md`) talks about parallelizing "within a single where clause or column expression" and "across independent nodes". A reader searching the docs or the web for "row fan-out" won't find anything.

A test: would "column fan-out" mean the same thing to someone who hasn't read line 5? Probably not. In networking and messaging, "fan-out" usually means one producer sending to many consumers.

**Fix:** Use recognized terms such as:
- "concurrent row calculations" for splitting one column's rows across threads
- "concurrent column calculations" for evaluating independent columns at once

You could also use "parallelizing within a column" and "parallelizing across columns" to match `parallelization.md`. If you want the general CS terms, "data parallelism" and "task parallelism" fit, with a short gloss. Whichever you pick, carry it into the heading at line 7 ("Which formulas the engine can fan out") too.

*Accuracy note (not verified):* before you rename "column fan-out", have someone confirm that the engine really evaluates independent columns of one `update` on different threads. `parallelization.md` describes parallelism across nodes of the update graph (the DAG), which isn't necessarily the same thing.

### 3. Configuration detail inside a concept explanation (line 11). Medium priority.

> You can tune that size with `QueryTable.minimumParallelSelectRows`, which defaults to about 4.2 million rows.

A property name and its default, inside a paragraph about *when* the engine divides work, belong somewhere else (the structure skill's **Level of abstraction** check). Keep the concept ("Row fan-out only happens once a table is large enough to be worth dividing") and do one of these:
- Move the property and default into a Configuration section at the end of the page.
- Link to the configuration reference, `conceptual/query-table-configuration.md` (its "Parallel processing with select" section already documents this property).

Also, "about 4.2 million" is a vague hedge when the exact value is known. That configuration page lists the default as `1L << 22` (4,194,304 rows). If you keep a value, give the exact one.

### 4. Passive voice (line 5). Low to medium priority.

- "one column's rows are divided into ranges that different threads evaluate" — the engine does the dividing, so name it: "the engine divides one column's rows into ranges and evaluates each range on a different thread."
- "independent columns in the same `update` are evaluated on different threads" — rewrite as: "the engine evaluates independent columns in the same `update` on different threads."

### 5. First mentions of methods aren't linked (lines 5 and 15). Low to medium priority.

- **`update`** (line 5, first mention): link it to `reference/table-operations/select/update.md`.
- **`with_serial`** (line 15, first mention): link it to the `with_serial` section of `reference/query-language/types/ConcurrencyControl.md`.

The method names are already written correctly: no leading dot and no empty parentheses.

### 6. Vague heading (line 13). Low priority.

"Formulas that need care" doesn't say what kind of care or why. Once finding 1 is fixed, a heading like "Stateful formulas" or "Formulas that aren't stateless" names the concept directly and pairs with the section above it.

### 7. No "Related documentation" section. Low priority, depends on context.

A concept guide needs a "Related documentation" section at the end. If this excerpt is the end of the page, add one. Good candidates are `parallelization.md`, `query-table-configuration.md`, the `update` reference, and the `ConcurrencyControl` reference. If it's a mid-page excerpt, ignore this.

## Summary

The main problem is terminology. Finding 1 matters most: "thread-safe" and "stateless" are treated as the same thing, which makes line 9's explanation wrong as well as inconsistent. Finding 2 comes next: "row/column fan-out" is a label a reader can't search for or recognize. After that, move the configuration property out of the explanation, make the two passive sentences active, and link the first mentions of `update` and `with_serial`. The mechanical checks all came back clean.