# Style review: `parallel-excerpt.md`

**File:** `.claude/skills/deephaven-writing-style/evals/files/parallel-excerpt.md` (report only; nothing was edited)

**Category:** Concept guide. You describe it as a draft excerpt for a deephaven-core concept guide, and it explains how and why the engine parallelizes formulas, which is a concept guide's job. So it should read as an explanation, and it can use technical vocabulary once each term is defined. The same topic already has a concept page at `docs/python/conceptual/query-engine/parallelization.md`, which is a useful reference for terminology.

## Summary

The biggest problems are about **terminology**. The excerpt makes up its own phrases where standard terms exist, and it says two different properties mean the same thing. It also has two mechanical problems with how `with_serial` is written, one parenthetical that holds a config property, and one passive sentence.

## High priority: terminology

### 1. Made-up labels "across rows" and "across tables" (line 5, again on line 21)

> Deephaven runs formulas across rows and across tables. Across rows means ... across tables means ...

These are short invented labels that the reader has to decode, and they only make sense after reading the definitions. Someone who lands on line 21 ("skips splitting across rows") without reading the intro won't know what they mean. Use standard terms instead, such as **concurrent row calculations** (one formula's rows split into chunks and computed on multiple threads) and **concurrent table calculations** (independent tables updating at the same time). Then use the same term everywhere, including line 21.

Suggested rewrite:

> Deephaven parallelizes formulas in two ways. With concurrent row calculations, the engine splits a table's rows into chunks and evaluates each chunk on a separate thread. With concurrent table calculations, the engine updates independent tables at the same time.

(I wrote "thread" instead of "core" because the engine schedules work on threads. Please confirm that wording during the accuracy review.)

### 2. "Only does math on its inputs" should be "pure function" (line 9)

> A formula is safe to run in parallel when it only does math on its inputs and doesn't touch anything else.

This is an informal stand-in for a standard computer-science term. Say it directly: "A formula is safe to run in parallel when it's a **pure function**: its result depends only on its inputs, and it has no side effects such as changing a global variable." Also note that "only does math" is too narrow, because a pure formula can do string handling, comparisons, and so on.

### 3. "Stateless" and "thread-safe" treated as the same thing (lines 9 and 18)

> Stateless formulas like these are thread-safe, so ...
>
> A thread-safe function is also stateless, so you can treat the two words as meaning the same thing.

Line 18 is the most serious problem in the excerpt. It tells the reader outright that two different properties are the same thing. "Stateless" means the formula keeps and depends on no state between calls. "Thread-safe" means the code can run on several threads at once without corrupting data, and code that has state can still be thread-safe if it uses locks, atomics, and so on. So the claim on line 18 is also false, which is an accuracy problem as well as a style one. **Delete line 18.** Choose the term the engine uses, which is **stateless** (as in `QueryTable.statelessSelectByDefault` and the "stateless"/"stateful" wording in `parallelization.md`), and use it throughout. Only mention thread safety where you mean that specific property.

### 4. Three terms for one idea: "safe", "thread-safe", "stateless"

The heading says "Safe formulas," the body says "thread-safe," and the rest of the excerpt says "stateless." Once item 3 is fixed, choose one term and use it in the headings too. For example, rename the headings **Stateless formulas** / **Stateful formulas**, which match the engine's terms and `parallelization.md`. The current headings don't say what "safe" means (safe from what?).

### 5. "Tick" used without a definition (line 5)

"independent tables tick at the same time" uses "tick" without explaining it. This style guide lists it as jargon to define on first use. Either define it in plain words ("update when new data arrives") or link to the concept page on live and ticking tables. The rewrite in item 1 avoids the word altogether.

## Formatting and mechanical checks

### 6. Leading dot and empty parentheses on `with_serial` in prose (line 21)

> Use `.with_serial()` on the column ...

This breaks the method-name rule twice. Prose never uses a leading dot, and a bare method reference never gets empty parentheses. It should be `with_serial`. The `` `\.[a-z] `` search found only this one hit, and it isn't a file extension. The empty-`()` search also found only this one.

### 7. First mention of `with_serial` isn't linked (line 21)

`with_serial` has a reference entry at `docs/python/reference/query-language/types/ConcurrencyControl.md` (section `with_serial`). This is its first and only mention, and it has no link. Suggested fix: ``[`with_serial`](../reference/query-language/types/ConcurrencyControl.md#with_serial)`` (the relative path depends on where the page ends up). `empty_table` appears only inside the code block, so no link is needed for it.

### 8. "On the column" is vague (line 21)

In Python, `with_serial` is called on a `Selectable` (or a `Filter`), not on a column. `parallelization.md` says "use `with_serial` on a Selectable." "Column" isn't strictly wrong in plain English, but naming the object tells the reader exactly where to call the method. You could add a short code example (for example, `Selectable.parse("A = counter()").with_serial()`) rather than describing it only in prose.

## Prose quality

### 9. Parenthetical with a config property wedged into the sentence (line 21)

> (the engine then skips splitting across rows, controlled by `QueryTable.statelessSelectByDefault`)

The sentence reads fine without the parenthetical, and the parenthetical holds both a mechanism note and a config property name. Move it out. Put the mechanism in its own sentence, and put the property in a Configuration note or link it. There's also an accuracy flag for the next reviewer: `QueryTable.statelessSelectByDefault` (in `engine/table/.../QueryTable.java`) sets whether formulas are stateless **by default**. It doesn't control what `with_serial` does, so "controlled by" misstates how the two relate.

Suggested rewrite:

> If a formula updates a global variable, it's stateful. Call [`with_serial`](...) on its `Selectable` so the engine evaluates its rows one at a time, in order, instead of splitting them across threads.
>
> Formulas are stateless by default. To change that default, set the `QueryTable.statelessSelectByDefault` configuration property.

### 10. Passive voice (line 21)

"so that rows are processed one at a time" is passive, and the actor (the engine) is known and matters. Use "so the engine processes rows one at a time, in order." The rewrite above does this.

### 11. Minor: "isn't stateless" (line 21)

"Isn't stateless" is a double negative. Use "is stateful," the standard opposite term, which the engine and `parallelization.md` both use.

## Things that pass

- Headings are in sentence case and contain no links.
- No curly quotes, and no dashes to fix (there are no em dashes, and none are misused).
- No `[here]`, `[click here]`, or `[this page]` link text.
- Code follows PEP 8. It uses `snake_case` for `source`, imports a specific name (`from deephaven import empty_table`) instead of using `import *`, column names start with a capital (`X`, `Y`), and formulas have spaces around operators (`"Y = X * 2"`).
- The explanatory tone fits a concept guide.

## Notes

- **Related documentation section:** a published concept guide needs one at the end. It's missing here, which is expected for an excerpt. Just make sure the full page has one, for example linking to `parallelization.md`, `ConcurrencyControl.md`, and `how-to-guides/performance/formula-threads.md`.
- **Code block tags:** the code block has no tag. Since it creates a table, it will likely need `order=source` (or whatever the page's snapshot convention is) when it goes into the real doc.
- **Possible overlap:** `docs/python/conceptual/query-engine/parallelization.md` already covers stateless and stateful formulas and `with_serial`. Whether this excerpt should link to that page, merge into it, or stay separate is a question for a structure review, not this one.
