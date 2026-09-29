# Style review: `parallel-excerpt.md`

**Category:** Concept guide. The task describes it as a concept-guide excerpt, and the title "How parallel formulas work" says it explains how something works. So it gets the concept-guide tone: explanatory, building up a mental model. The reader can handle technical vocabulary once it has been defined.

**Mechanical searches run:** backtick-dot-lowercase (1 hit, line 21), a backticked identifier with empty `()` in prose (1 hit, line 21), `[here]`/`[click here]`/`[this page]` (none), curly quotes and en dashes (none).

## Findings

### 1. Coined labels "across rows" and "across tables" (line 5, reused on lines 9 and 21)
"Runs formulas across rows and across tables" uses short made-up labels that the reader has to decode. The sentence after it has to define them for that reason. The guide asks for the standard term instead.
- **Suggested:** "Deephaven supports two kinds of parallelism: *concurrent row calculations*, where the engine splits a table into pieces and computes each piece on its own core, and *concurrent table calculations*, where independent tables update at the same time."
- Carry the chosen term into line 9 ("the engine runs them across rows automatically" → "…evaluates them as concurrent row calculations automatically"). Do the same on line 21 ("skips splitting across rows" → "doesn't split the calculation across cores" or similar).

### 2. "Tick" is undefined jargon (line 5)
"Independent tables tick at the same time" uses "tick" without defining it on first use. Either define it in plain language, for example "tables whose data updates on each engine cycle", or link to the ticking/live-table concept page.

### 3. Coined description instead of "pure function" (line 9)
"When it only does math on its inputs and doesn't touch anything else" is the exact pattern the guide warns against. The standard terms are **pure function** and **stateless**. The existing `conceptual/query-engine/parallelization.md` defines stateless as "does not depend on any mutable external inputs or the order in which rows are evaluated". Line 9 is also narrower than that definition. It describes "math", but string manipulation and other operations are stateless too.
- **Suggested:** "A formula is safe to run in parallel when it is *stateless*: it depends only on its inputs and not on mutable external state or row order."

### 4. "Stateless" and "thread-safe" are treated as synonyms (lines 9 and 17)
Line 9 slides from "stateless" to "thread-safe" in one sentence. Line 17 then tells the reader outright to "treat the two words as meaning the same thing." They describe different properties. The guide uses this exact pair as its example of near-synonyms that must not be used interchangeably. A thread-safe function can still keep state, for example a synchronized counter. Such a function is thread-safe but not stateless, and it still needs serial evaluation if row order matters.
- **Fix:** Delete line 17. Pick "stateless" as the term for this page and use it consistently. If thread safety matters to the explanation, give it its own definition and state how it relates to statelessness, without calling them equal.
- Flag line 17 for the accuracy review as well, because the claim itself is wrong, not just imprecise wording.

### 5. `.with_serial()` has a leading dot and empty parentheses in prose (line 21)
Prose method references take no leading dot and no empty `()`. It should be `with_serial`, not `.with_serial()`.

### 6. First mention of `with_serial` isn't linked (line 21)
A reference page exists at `docs/python/reference/query-language/types/ConcurrencyControl.md#with_serial`. The first (and only) mention should link to it, for example ``[`with_serial`](../../reference/query-language/types/ConcurrencyControl.md#with_serial)``. Adjust the relative path for where the page ends up.

### 7. Caveat and property name wedged into a parenthetical (line 21)
"(the engine then skips splitting across rows, controlled by `QueryTable.statelessSelectByDefault`)" carries two separate ideas: what `with_serial` does, and a configuration property. Neither needs to sit in the middle of the instruction, and the sentence reads fine without the parenthetical. The property name also tells the reader nothing at this point. It controls the *default* for all columns, which is a different thing from calling `with_serial` on one column.
- **Suggested:** "Call `with_serial` on the column so the engine evaluates its rows one at a time, in order." Put the property in a separate sentence, a Configuration section, or a link to the parallelization concept guide, which already covers `QueryTable.statelessSelectByDefault`.

### 8. Passive voice (line 21)
"So that rows are processed one at a time, in order": the actor, the engine, is known and matters here. The active rewrite in finding 7 fixes this too.

### 9. "Stateless formulas like these" doesn't point at anything specific (line 9)
"Like these" refers to a code block that hasn't appeared yet, and the reader doesn't know which formulas are meant. Say it directly, for example "Formulas such as `X = i` and `Y = X * 2` are stateless, so…". A column assignment in backticks is fine as a small example.

### 10. Paragraph structure (line 5)
The opening paragraph packs two definitions (row parallelism and table parallelism) into one sentence joined by a semicolon. Consider splitting it into two sentences, or into a short two-item list, so each concept gets its own line. That also makes the terms from finding 1 easier to scan.

## Checked and fine
- Headings use sentence case and contain no links.
- Straight quotes only; no problem em dashes or en dashes.
- Python code: `snake_case` table name (`source`), capitalized column names (`X`, `Y`), spaces around `=` in formulas (`"X = i"`), and a targeted import (`from deephaven import empty_table`, not `import *`).
- No non-descriptive link text.

## Notes
- **Code block tag:** The block has no tag and will be snapshot-tested with its default output. That's acceptable. Add `order=source` only if the full page has several output tables and the order matters.
- **Related documentation:** Because this is an excerpt, the missing "Related documentation" section isn't a defect yet. The finished concept guide needs one. Good candidates are the parallelization concept guide and the `ConcurrencyControl` reference page.
- **Overlap with an existing page:** `docs/python/conceptual/query-engine/parallelization.md` already covers this material. Its terminology (stateless / stateful, `statelessSelectByDefault`) is the one to match. Whether this page should exist separately from that one is a structure question for `deephaven-doc-structure-review`, not a style finding.