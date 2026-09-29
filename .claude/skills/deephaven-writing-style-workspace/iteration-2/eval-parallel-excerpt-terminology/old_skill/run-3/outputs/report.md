# Style review: `parallel-excerpt.md`

**Category:** Concept guide ("How parallel formulas work" explains how and why, and the task calls it a concept-guide draft). An explanatory tone fits. Readers can be expected to know more than Crash Course readers, but new terms still need a definition on first use. I didn't edit anything.

## Findings

### 1. Coined labels instead of standard terms (line 5, repeated on line 9)
"Across rows" and "across tables" are invented labels. The sentence then has to decode them ("Across rows means…; across tables means…"). Someone who skipped that sentence won't understand "runs them across rows automatically" on line 9. Use the standard terms instead:

> Deephaven supports two kinds of parallelism: **concurrent row calculations**, where the engine splits a table's rows into chunks and processes them on separate threads, and **concurrent table calculations**, where independent tables update at the same time.

Line 5 also says tables "tick" without defining it. Either define "ticking" in plain words or link it to the live/ticking table concept page.

### 2. A plain description where a standard term exists (line 9)
"When it only does math on its inputs and doesn't touch anything else" describes a **pure function**. Use that term, which readers can search for and may already know. The line also uses "stateless" without defining it. Either define it here or link it.

### 3. "Stateless" and "thread-safe" treated as the same thing (lines 9 and 17)
Line 9 says stateless formulas "are thread-safe". Line 17 then states outright: "you can treat the two words as meaning the same thing." They are different properties. A function can be thread-safe (for example, it locks shared state) without being stateless. Telling readers the terms are interchangeable teaches them something false. Delete line 17, pick one term (the rest of the parallelization docs use **stateless/stateful**), and use it throughout.

Along the same lines, the headings "Safe formulas" and "Unsafe formulas" add a third related word. Consider "Stateless formulas" and "Stateful formulas" so the headings match the body text.

### 4. Method reference in prose: leading dot, empty parentheses, no link (line 21)
`` `.with_serial()` `` breaks two prose rules. It has a leading dot, and it has empty parentheses, which add nothing. (I confirmed this with the required `` `\.[a-z] `` and `` `name()` `` searches. It is the only hit for either.) This is also the first and only mention of the method, and it isn't linked, even though a reference page exists.

- Fix: ``Use [`with_serial`](../../reference/query-language/types/ConcurrencyControl.md#with_serial) …``. Adjust the relative path to wherever the page ends up.
- On wording: `with_serial` is called on a `Selectable` (or `Filter`), not literally "on the column". Consider "Call `with_serial` on the column's `Selectable`" so the prose names the object readers actually use. See the example in `docs/python/conceptual/query-engine/parallelization.md`: `Selectable.parse(...).with_serial()`.

### 5. Parenthetical carrying a config property (line 21)
"(the engine then skips splitting across rows, controlled by `QueryTable.statelessSelectByDefault`)" puts a behavior note and a configuration property in the middle of the sentence. The sentence reads fine without it. Move the config property to its own sentence or a Configuration section. For example: "To make stateful the default for all `select` and `update` columns, set `QueryTable.statelessSelectByDefault` to `false`." The parenthetical also reuses the coined "across rows" label from finding 1.

### 6. Passive voice (line 21)
"So that rows are processed one at a time, in order": the actor (the engine) is known and matters here. Suggested rewrite: "…so the engine processes rows one at a time, in order."

### 7. "Related documentation" section (whole file)
The excerpt has none. That's fine for a partial draft, but the finished concept guide needs one at the end. Good candidates are the parallelization concept guide, the `ConcurrencyControl` reference, and the formula-threads how-to.

## Checks that passed

- **Headings:** sentence case, no links.
- **Quotes and dashes:** straight quotes only, and no em-dash or hyphen misuse.
- **Link text:** no `[here]`, `[click here]` or `[this page]` link text.
- **Code block:**
  - PEP 8 `snake_case` naming (`source`).
  - A targeted import (`from deephaven import empty_table`), not a wildcard.
  - Capitalized column names (`X`, `Y`).
  - Spaced formulas (`"Y = X * 2"`).
  - No tag needed, since the default output shows the single table.
- **Proper nouns:** "Deephaven" is capitalized correctly. No other proper nouns in the list appear.
- **Tense:** present tense throughout, with no future "will".

## Suggested priority

1. Findings 3 and 1: the terminology problems. Line 17 actively misinforms readers.
2. Finding 4: the `with_serial` formatting and link.
3. Findings 5 and 6: moving the config property out of the sentence and the passive voice.
4. Finding 7: add "Related documentation" when the page is finished.