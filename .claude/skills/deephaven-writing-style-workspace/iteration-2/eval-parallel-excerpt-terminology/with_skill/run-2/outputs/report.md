# Style review: `parallel-excerpt.md`

**Category:** Concept guide, as you described it: it explains how and why the engine parallelizes formulas. Concept-guide calibration applies. The tone should be explanatory. Technical vocabulary is fine once it has been defined. Configuration detail stays out of the narrative.

**Scope:** Style only. I did not edit anything. The mechanical searches I ran on the file:
- Backtick-dot (`` `\.[a-z] ``): 1 hit, on line 21.
- Backticked identifier with empty `()`: 1 hit, on line 21. The leading dot hid it from the narrower search, so I checked it by eye.
- `[here]`, `[click here]`, `[this page]`: none.
- Smart quotes: none.
- En or em dashes: none.

---

## High priority

### 1. "Stateless" and "thread-safe" are treated as synonyms (lines 9, 17)
- Line 9 says: "Stateless formulas like these are thread-safe."
- Line 17 then says: "A thread-safe function is also stateless, so you can treat the two words as meaning the same thing."

The style guide names this exact pair as terms that must not be used interchangeably. "Stateless" means the formula doesn't depend on mutable external state or on row order. "Thread-safe" means it can be called safely from several threads at once. These are different properties. A thread-safe function can still be stateful, for example one that increments a synchronized counter. Line 17 tells the reader they're the same thing.

- **Fix:** Delete line 17. Pick **stateless** as the page's term, since `conceptual/query-engine/parallelization.md` already uses it, with **stateful** as its opposite. Use it consistently. Mention "thread-safe" only where you actually mean thread safety.
- **Accuracy:** The claim in line 17 is also likely wrong on the facts. Confirm it with `deephaven-core-accuracy-check`.

### 2. Coined labels instead of standard terms (lines 5, 9)
- **Line 5:** "across rows" and "across tables" are short ad-hoc labels the reader has to decode. The line even has to define them in place ("Across rows means..."). Use the standard phrasing: **concurrent row calculations** (splitting one table's rows across threads) and **concurrent table calculations** (computing independent tables at the same time).
- **Line 9:** "only does math on its inputs and doesn't touch anything else" is a folksy stand-in for a known term. Say **pure function**, or define **stateless** directly. For example: "A formula is stateless when its result depends only on its input values — it doesn't read or change any shared state, and it doesn't depend on the order in which rows are evaluated."

### 3. Configuration property injected mid-sentence (line 21)
The parenthetical "(the engine then skips splitting across rows, controlled by `QueryTable.statelessSelectByDefault`)" is the configuration-injection pattern. The sentence reads fine without it. Moving the property into its own sentence in the same paragraph would still count as injection in a Concept guide.

- **Fix:** Remove the property from the narrative completely. Put it in a Configuration section at the end of the page, or link to the configuration reference (`conceptual/query-table-configuration.md`).
- **Rewrite:** Don't bring the property back anywhere in the narrative. See the suggested rewrite of line 21 at the end.

### 4. Method reference formatting and linking (line 21)
`` `.with_serial()` `` breaks two rules:
- Prose method names take no leading dot.
- Empty parentheses add nothing in prose.

It's also the first mention of `with_serial`, and it isn't linked. A reference target exists: `reference/query-language/types/ConcurrencyControl.md#with_serial`. Barrier.md already links it this way.

- **Fix:** [`with_serial`](../../reference/query-language/types/ConcurrencyControl.md#with_serial). Adjust the relative path to wherever the page ends up.

---

## Medium priority

### 5. Jargon not defined on first use (line 5)
"Independent tables tick at the same time" uses "tick" without defining it. Concept guides can assume more than the Crash Course, but jargon still needs a definition or a link the first time it appears. Either:
- define it ("tables whose data updates live, or 'ticks'"), or
- link to the live/ticking table concept page.

### 6. Headings use a third term (lines 7, 19)
The headings "Safe formulas" and "Unsafe formulas" add "safe" as another near-synonym next to "stateless" and "thread-safe." Once you settle on the terms from item 1, use them in the headings too, for example "Stateless formulas" and "Stateful formulas." Both headings are already in sentence case, so no change is needed there.

### 7. Passive voice (line 21)
"so that rows are processed one at a time, in order" doesn't say who does the processing. The actor, the engine, matters here. Suggested active version: "so the engine evaluates rows one at a time, in order."

### 8. Two ideas in one paragraph (line 5)
Line 5 defines two separate mechanisms in one run-on sentence joined by a semicolon. Split it into two sentences, or use a two-item list, one per kind of concurrency.

---

## Low priority

### 9. Vague phrasing
- **Line 9:** "doesn't touch anything else" is vague. Item 2's definition replaces it.
- **Line 9:** "like these" refers to a code block that has only one statement, which defines two columns. Say "Formulas like `X = i` and `Y = X * 2` are stateless" so the reader knows exactly what "these" means.

### 10. Code example
The code follows the conventions:
- `snake_case` table name `source`
- capitalized column names `X` and `Y`
- spaces around `=`
- a targeted import (`from deephaven import empty_table`)

Consider adding an `order=source` tag so the output table is shown explicitly. This is optional.

### 11. Related documentation
This is an excerpt, so it may not show the whole page. If the published page ends here, it needs a "Related documentation" section, because concept guides aren't exempt. Good candidates:
- the parallelization concept guide
- `ConcurrencyControl`
- the query table configuration page

---

## Suggested rewrite of lines 5 and 21 (illustrative)

> Deephaven runs formulas concurrently in two ways. In concurrent row calculations, the engine splits a table into pieces and computes each piece on its own thread. In concurrent table calculations, the engine updates independent [live tables](<link>) at the same time.

> If a formula updates a global variable, it's stateful. Use [`with_serial`](<ConcurrencyControl link>#with_serial) on the column so the engine evaluates its rows one at a time, in order.

The property name goes in a Configuration section at the end of the page, not in the passage. The rewrite says "thread" where the original said "core". That wording is a guess, so check it during the accuracy review.