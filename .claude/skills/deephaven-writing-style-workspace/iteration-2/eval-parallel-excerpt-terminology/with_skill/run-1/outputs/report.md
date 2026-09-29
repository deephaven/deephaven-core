# Style review: `parallel-excerpt.md`

**File:** `.claude/skills/deephaven-writing-style/evals/files/parallel-excerpt.md` (report only, nothing edited)

**Category:** Concept guide, which is what you told me. The file is an excerpt outside `docs/`, so I couldn't confirm the category from its directory. I applied the Concept-guide profile: an explanatory tone, contractions allowed, and no configuration detail in the narrative. Once a term is defined, the page can lean on it more than the Crash Course could.

## Summary

The most serious problem is terminology. The excerpt uses coined labels ("across rows", "across tables") where standard terms exist. It also uses "safe", "stateless", "thread-safe" and "does only math" as if they meant the same thing, and line 17 says outright that they do. The one line with an API reference, line 21, has several mechanical problems:
- The method name has a leading dot and empty parentheses.
- Its first mention has no link.
- A configuration property is wedged into a parenthetical.

## Findings

### Terminology and word choice

1. **Line 5: coined labels instead of standard terms.** "Across rows" and "across tables" are terse labels the reader has to decode. They are also defined only by a clause later in the same sentence. Use the standard terms, "concurrent row calculations" (or "parallelism within a table") and "concurrent table calculations". Once you pick a term, use it everywhere. The coined label comes back on line 9 ("runs them across rows automatically") and line 21 ("skips splitting across rows").
   - Suggested rewrite: "Deephaven parallelizes formula evaluation in two ways. With concurrent row calculations, the engine splits a table into pieces and evaluates each piece on a separate thread. With concurrent table calculations, the engine updates independent tables at the same time."

2. **Line 5: "tick" is undefined jargon.** "Independent tables tick at the same time" uses "tick" with no definition and no link. Either define it in plain words ("update with new data"), or link to the ticking and live table concept page on first use.

3. **Line 5: two ideas in one sentence.** The semicolon joins two separate definitions. Splitting them, as in the rewrite in item 1, fixes both the "one idea per paragraph" problem and the readability problem.

4. **Line 9: an ad-hoc description where a standard term exists.** "It only does math on its inputs and doesn't touch anything else" describes a **pure function**, meaning one with no side effects. Say that.
   - Suggested rewrite: "A formula is safe to evaluate in parallel when it is a pure function: its result depends only on its inputs, and it has no side effects."

5. **Line 9: three near-synonyms used as one.** "Safe", "stateless" and "thread-safe" all appear in two sentences as if they were the same thing. Stateless and thread-safe are different properties, and swapping between them tells the reader they're equivalent. Pick the property the engine actually relies on and use only that word. The default is called "stateless" (for example, `statelessSelectByDefault`), so "stateless" or "pure" is the natural choice. Once "safe" is defined in terms of that property, the section headings can keep it.

6. **Line 17: states outright that stateless and thread-safe mean the same thing. Delete this sentence.** "…so you can treat the two words as meaning the same thing" is exactly the confusion the terminology rule warns against. The reverse direction is also false: a thread-safe function isn't necessarily stateless. A synchronized counter is thread-safe but has state. Remove the sentence rather than reword it. The factual error also belongs in an accuracy check (`deephaven-core-accuracy-check`).

7. **Line 21: say what the problem is.** "It isn't stateless" defines the unsafe case by negation, using a term the page never settled on. Say it has side effects, or that it depends on shared state or row order.
   - Suggested rewrite: "If a formula updates a global variable, it has side effects and isn't safe to evaluate in parallel."

### Method names and links (mechanical checks)

8. **Line 21: `.with_serial()` has a leading dot and empty parentheses.** Both the `` `\.[a-z] `` search and the empty-`()` search hit here. Method names in prose take no leading dot and no empty parentheses, so write it as `with_serial`.

9. **Line 21: the first mention of `with_serial` has no link.** A reference target exists: the `with_serial` section of `docs/python/reference/query-language/types/ConcurrencyControl.md`. Link the first mention, for example ``[`with_serial`](../../reference/query-language/types/ConcurrencyControl.md#with_serial)``. The relative path depends on where the page lands; the path shown assumes a `conceptual/query-engine/` page. "On the column" is also vague: the method is called on a `Selectable` (or a `Filter`), which you could name and link the same way.

10. **Line 14: optional link for `update`.** `update` appears only inside the code block, so the rule for linking method names in prose doesn't apply. If the surrounding prose mentions `update` or `empty_table`, link those first mentions to `reference/table-operations/select/update.md` and `reference/table-operations/create/emptyTable.md`.

### Configuration detail in the narrative

11. **Line 21: the parenthetical injects configuration.** "(the engine then skips splitting across rows, controlled by `QueryTable.statelessSelectByDefault`)" drops a property name into the middle of a conceptual explanation. The sentence reads fine without it, which is the sign it doesn't belong there. In a Concept guide, don't fix this by moving the property to a separate sentence in the same paragraph.
    - Put the property and its default in one Configuration section at the end of the page, and keep it out of the narrative entirely. Note that `docs/python/conceptual/query-table-configuration.md` currently lists `QueryTable.statelessFiltersByDefault` but not `statelessSelectByDefault`, so a plain link there won't reach it. Use a Configuration section on this page, or add the property to that page first.
    - The parenthetical also seems to tie the property to what `with_serial` does. In source (`QueryTable.java`, around line 388), the property sets the engine's *default assumption* about formulas; it doesn't control `with_serial`. Pass this to the accuracy check.
    - Suggested rewrite: "Call [`with_serial`](…) on the column's `Selectable` so the engine evaluates that formula one row at a time, in order."

### Voice

12. **Line 21: passive voice.** "…so that rows are processed one at a time, in order" hides the actor, and the actor matters here. Use "…so the engine processes rows one at a time, in order." The rewrite in item 11 also covers this.

### Code example

13. **Lines 11–15: no code example tag.** The block has no `order=` tag. Add `order=source` so the snapshot tool knows which table to display: ` ```python order=source `. The code itself follows the conventions: a named import, snake_case `source`, capitalized column names, and spaces around `=`.

### Page structure

14. **No "Related documentation" section.** That's expected in an excerpt, but the finished Concept guide needs one at the end. Candidates: the `ConcurrencyControl` reference, `conceptual/query-engine/parallelization.md`, and the configuration reference.

## Passes

- **Headings:** Both use sentence case and contain no links.
- **Quotes and dashes:** Only straight quotes. No stray hyphens or en dashes used as dashes; there are no em dashes to check.
- **Link text:** No `[here]`, `[click here]` or `[this page]` link labels.
- **Proper nouns:** No capitalization problems.
- **Tone:** The contractions ("doesn't", "isn't") and explanatory tone fit a Concept guide.
- **Code conventions:** PEP 8 naming and no wildcard imports.

## Out of scope, passed on

- The claim that a thread-safe function is also stateless (line 17) is false.
- It's unclear what `QueryTable.statelessSelectByDefault` actually controls (line 21).
- "Computes each piece on its own core" (line 5): the engine uses threads, not cores.

These are accuracy questions for `deephaven-core-accuracy-check`.