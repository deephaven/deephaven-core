# Style review: `parallel-excerpt.md`

**Category:** Concept guide. It's described as a draft excerpt for a deephaven-core concept guide, and its content ("How parallel formulas work") explains how the engine works rather than walking through a task. I've applied the concept-guide calibration: the tone is explanatory, and contractions are fine. Defined technical vocabulary is acceptable, but every term still has to be defined on first use.

I ran the required checks as literal searches:
- **Leading dot on a method name in prose:** 1 hit, on line 21.
- **Empty `()` after a method name in prose:** 1 hit, the same token on line 21. The plain regex missed it because the dot comes before the name, so I confirmed it by eye.
- **`[here]`, `[click here]` or `[this page]` link labels:** none.
- **Curly quotes, en/em dashes, future-tense "will":** none.

The headings are already in sentence case.

---

## Findings

### 1. Line 5: coined labels "across rows" and "across tables"
> Deephaven runs formulas across rows and across tables. Across rows means ... across tables means ...

These are terse labels the reader has to decode, which is exactly the case the style guide calls out. The draft defines them, but defining a coined label doesn't make up for skipping the standard term. Use **"concurrent row calculations"** and **"concurrent table calculations"** (or "row-level" and "table-level parallelism"). Keep those terms for the rest of the page.

The sentence also packs two separate ideas into one semicolon-joined sentence. Split them.

"Tick" is also jargon that isn't defined on first use. Either define it in plain language (for example, "tables that update at the same time") or link to the live/ticking table concept page.

Suggested rewrite:
> Deephaven parallelizes work in two ways. With concurrent row calculations, the engine splits a table into pieces and computes each piece on a separate core. With concurrent table calculations, independent tables update at the same time.

### 2. Line 9: a coined phrase where a standard term exists, and two terms treated as one
> A formula is safe to run in parallel when it only does math on its inputs and doesn't touch anything else. Stateless formulas like these are thread-safe, so the engine runs them across rows automatically:

- "Only does math on its inputs and doesn't touch anything else" is an ad-hoc description of a **pure function** or **stateless** formula, which is almost the guide's own example of what to avoid. Use the standard term and define it once. The existing `conceptual/query-engine/parallelization.md` uses "stateless" to mean "does not depend on any mutable external inputs or the order in which rows are evaluated".
- "Stateless" appears here without ever being defined. The sentence before it only implies what it means.
- "Stateless formulas ... are thread-safe" starts treating two different properties as if they were the same (see finding 3).
- "runs them across rows" repeats the coined label from line 5.
- The heading "Safe formulas" relies on the undefined word "safe". Consider "Stateless formulas", which fits the standard term.

Suggested rewrite:
> A formula is *stateless* when its result depends only on its input values: it doesn't read or change any external state, and it doesn't depend on the order in which rows are evaluated. By default, the engine treats formulas as stateless and uses concurrent row calculations for them:

### 3. Line 17: explicitly says two different terms mean the same thing
> A thread-safe function is also stateless, so you can treat the two words as meaning the same thing.

This breaks the terminology-consistency rule directly. The guide gives this exact pair as its example: "stateless" and "thread-safe" are different properties, and using them interchangeably tells the reader they're the same. A function can be thread-safe (for example, it guards shared state with a lock) and still be stateful and order-dependent. Delete the sentence and use "stateless" throughout.

The claim is also likely wrong on the facts, not just a style issue, so run it through `deephaven-core-accuracy-check`.

### 4. Line 21: method name formatting, a missing link, a parenthetical carrying config detail, and passive voice
> If a formula updates a global variable, it isn't stateless. Use `.with_serial()` on the column (the engine then skips splitting across rows, controlled by `QueryTable.statelessSelectByDefault`) so that rows are processed one at a time, in order.

- **Leading dot and empty parentheses in prose:** `.with_serial()` should be `with_serial`. The leading dot and the empty `()` belong only in code.
- **The first mention should link:** this is the only mention of `with_serial` in the excerpt, and it isn't linked. A suitable target exists: `docs/python/reference/query-language/types/ConcurrencyControl.md` has a `### with_serial` section. Link this mention.
- **Parenthetical carrying config detail:** the sentence reads fine without the parenthetical, which holds both a mechanism caveat and a configuration property name. Move `QueryTable.statelessSelectByDefault` into its own sentence or a Configuration section, or link to the parallelization concept guide, where it's already covered. The parenthetical also repeats the coined phrase "splitting across rows".
- **Passive voice:** in "rows are processed one at a time", the actor (the engine) is known and relevant. Rewrite it as active: "the engine processes rows one at a time, in order."
- **Standard term:** "it isn't stateless" is a roundabout way of saying **"it's stateful"**, which is the term the existing parallelization guide uses.
- **Terminology:** "on the column" is loose, since `with_serial` is called on a Selectable (or Filter) expression. Say "on the Selectable" or "on the column expression". That's also worth confirming in the accuracy pass.

Suggested rewrite:
> If a formula updates a global variable, it's stateful. Call [`with_serial`](../../reference/query-language/types/ConcurrencyControl.md#with_serial) on its Selectable so the engine processes rows one at a time, in order.
>
> To change the engine's default for all formulas, see the `QueryTable.statelessSelectByDefault` property in [Parallelization](./parallelization.md).

(Adjust the relative paths to wherever the page ends up.)

### 5. Page structure (only if this becomes a full page)
- There's no "Related documentation" section. That's expected in an excerpt, but a published concept guide needs one at the end. Good candidates are the Parallelization concept guide, the `ConcurrencyControl` reference and the formula-threads how-to.
- The "Unsafe formulas" section has no code example to match the one under "Safe formulas". That's a structural point, not a style one. Raise it with `deephaven-doc-structure-review` if you want it covered.

---

## No issues found
- **Code block:** snake_case variable (`source`); a specific import (`from deephaven import empty_table`, not `import *`); capitalized column names (`X`, `Y`); spaces around `=` in the formulas (`"X = i"`, `"Y = X * 2"`). An untagged block is acceptable here.
- **Headings:** sentence case, with no links.
- **Punctuation and links:** straight quotes only, no hyphen or en-dash parentheticals, and no non-descriptive link text.
- **Tone:** explanatory, which suits a concept guide. The contractions are acceptable because this isn't a reference page.

## Summary
The main problem is terminology. The draft invents labels ("across rows/tables", "only does math on its inputs") where standard terms exist, and line 17 explicitly treats "stateless" and "thread-safe" as the same. Fix those, correct `.with_serial()` to a linked `with_serial`, move the config property out of the parenthetical, and make the last clause active.

Line 17's claim and the "on the column" wording should also go through an accuracy check.