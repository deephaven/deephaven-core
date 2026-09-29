# Style review: `parallel-excerpt.md`

**Category:** This is a Concept guide. The draft is described as an excerpt for a deephaven-core concept guide, and its subject, how the engine parallelizes formulas, is the same mechanism covered in `docs/python/conceptual/query-engine/parallelization.md`. So the Concept-guide profile applies: an explanatory tone, contractions are fine, and configuration detail stays out of the narrative.

I ran the mechanical checks as literal searches:
- **Leading-dot method refs in prose:** one hit, `` `.with_serial()` `` on line 21.
- **Empty parens in prose:** one hit, the same `` `.with_serial()` ``.
- **First-mention links:** `with_serial` has a reference anchor (`docs/python/reference/query-language/types/ConcurrencyControl.md#with_serial`), but its first and only prose mention on line 21 is not linked. `empty_table` and `update` appear only inside the code block, so they need no link.
- **`[here]`, `[click here]`, `[this page]` link labels:** none.
- **Headings:** the title and both H2s are in sentence case and contain no links.
- **Quotes and dashes:** straight quotes only; no em dashes or hyphen-dashes.
- **Code block:** follows PEP 8, uses a named import, spaces around `=`, and capitalized column names (`X`, `Y`). No problems.

## Findings

### 1. Coined phrases "across rows" / "across tables" (lines 5, 9, 21): **terminology**
The draft invents two labels and then has to decode them: "Across rows means... across tables means...". Neither label means anything to a reader who hasn't read line 5. Use the standard terms **concurrent row calculations** (one operation splits its rows across threads) and **concurrent table calculations** (independent tables update at the same time). Use them everywhere the ad-hoc labels appear: line 9 "runs them across rows automatically" and line 21 "skips splitting across rows".

### 2. "Only does math on its inputs and doesn't touch anything else" (line 9): **terminology**
This describes a **pure function** without using the recognized term. Name it: "A formula is safe to parallelize when it's a pure function: its result depends only on its inputs, and it has no side effects."

### 3. "Stateless" and "thread-safe" used as synonyms (lines 9, 17, 21): **terminology consistency (most serious)**
Line 9 says "Stateless formulas like these are thread-safe". Line 17 then tells the reader outright that "you can treat the two words as meaning the same thing". That is wrong: they describe different properties. A function can be thread-safe and still stateful, for example a synchronized counter. Here, "stateless" means the order of evaluation doesn't matter, which is what lets the engine split the rows. Thread safety alone doesn't guarantee that. **Delete line 17 entirely.** Pick "stateless" as the term this page uses (it matches the engine's own vocabulary: stateless vs. stateful / `with_serial`) and use it consistently. Only mention thread safety if the page separately explains how it differs.

### 4. `` `.with_serial()` `` in prose (line 21): **method names in prose**
This has a leading dot and empty parentheses. Both are wrong in prose. Write `with_serial`, and link this first mention: [`with_serial`](../../reference/query-language/types/ConcurrencyControl.md#with_serial) (adjust the relative path to where the page lands).

### 5. Configuration property wedged into a parenthetical (line 21): **configuration injection**
"(the engine then skips splitting across rows, controlled by `QueryTable.statelessSelectByDefault`)". The sentence reads fine without the parenthetical, and a property name doesn't belong in a Concept guide's narrative. Moving it into its own sentence in the same paragraph would still be configuration injection. Put `QueryTable.statelessSelectByDefault` in the page's Configuration section, or link to the configuration reference, and leave it out of the rewritten sentence. (Not a style issue, but raise it with the accuracy reviewer: this parenthetical also suggests the property controls what `with_serial` does. The property actually sets whether formulas are treated as stateless *by default*, which is a separate thing from what `with_serial` does. Worth a `deephaven-core-accuracy-check` pass.)

### 6. Undefined jargon "tick" (line 5): **define on first use**
"Independent tables tick at the same time" is the page's first use of "tick". Either define it ("tables that update in the same update cycle") or link to the live/ticking table concept page.

### 7. Passive voice (line 21): **active voice**
"So that rows are processed one at a time, in order". The actor is known (the engine), so say "so the engine processes rows one at a time, in order."

### 8. "On the column" (line 21): **precision / terminology**
`with_serial` is called on a Selectable (or Filter), not on a column. Say "on the Selectable". This matches `parallelization.md`: "use `with_serial` on a Selectable".

### 9. "Its own core" (line 5): **precision**
The engine hands pieces to threads, not to CPU cores. "On a separate thread" is the accurate and standard phrasing.

### 10. Related documentation (page structure): **note for the full page**
The excerpt has no "Related documentation" section. That's expected for an excerpt, but the finished concept guide needs one at the end. Likely entries are the `ConcurrencyControl` reference, `Barrier`, and the parallelization concept guide.

## Suggested rewrite

This applies findings 1–9 and keeps the configuration property out of the narrative:

````markdown
---
title: How parallel formulas work
---

Deephaven parallelizes work in two ways. With concurrent row calculations, the engine splits a single operation's rows into pieces and computes each piece on a separate thread. With concurrent table calculations, the engine updates independent tables at the same time within an update cycle.

## Stateless formulas

A formula is safe to parallelize when it's a pure function: its result depends only on its inputs, and it has no side effects. The engine treats formulas like these as stateless and uses concurrent row calculations for them automatically:

```python
from deephaven import empty_table

source = empty_table(100).update(["X = i", "Y = X * 2"])
```

## Stateful formulas

A formula that updates a global variable is stateful, because its result depends on the order in which rows are evaluated. Call [`with_serial`](../../reference/query-language/types/ConcurrencyControl.md#with_serial) on the Selectable so the engine processes its rows one at a time, in order.
````

I renamed the headings to "Stateless formulas" and "Stateful formulas" so they use the page's chosen terms (finding 3) instead of the vaguer "safe" and "unsafe". If you want to keep "Safe formulas" and "Unsafe formulas", the body text should still use "stateless" and "stateful" consistently.