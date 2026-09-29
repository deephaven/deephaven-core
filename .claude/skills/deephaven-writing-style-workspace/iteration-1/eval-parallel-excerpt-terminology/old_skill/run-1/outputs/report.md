# Style review: `parallel-excerpt.md` ("How parallel formulas work")

**Category:** Concept guide (per the task framing; it would live under `docs/python/conceptual/`, like the existing `conceptual/query-engine/parallelization.md`). Tone profile: explanatory, narrative framing OK, contractions OK, and technical vocabulary is allowed *once defined*. No edits were made.

## Main problem: the core terms aren't defined or used consistently

The page is about when a formula can run in parallel, but its key words drift and get conflated. Readers can't build a mental model when the vocabulary keeps shifting.

1. **"Safe" / "stateless" / "thread-safe" / "parallel" are used interchangeably, and the page says so outright.** Line: "A thread-safe function is also stateless, so you can treat the two words as meaning the same thing." This is the most serious problem in the excerpt. It is a style problem (it tells the reader to merge two distinct terms) and a correctness problem too:
   - The existing concept guide defines *stateless* as "does not depend on any mutable external inputs or the order in which rows are evaluated" (`docs/python/conceptual/query-engine/parallelization.md:57`). It uses *thread-safe* as a separate property ("a Python stateful function that is not thread-safe", line 88).
   - A stateless formula is safe to parallelize, but a thread-safe function isn't necessarily stateless. A synchronized or atomic counter is thread-safe, yet its results depend on row order.
   - **Fix:** Delete that sentence. Define *stateless* once, in plain language, at first use. Say *thread-safe* is a related but different property, or leave it out. Then use one term (*stateless*) throughout. Retitle the headings "Stateless formulas" and "Stateful formulas" so they match the terms in the prose and in the `Selectable`/`with_serial` reference docs, instead of the informal "Safe"/"Unsafe".
   - The first sentence under "Safe formulas" defines "safe" informally ("only does math on its inputs and doesn't touch anything else"). The next sentence then switches to "Stateless formulas like these are thread-safe". Pick one term, define it, and keep it.
   - Because this is a claim about behavior, send it to `deephaven-core-accuracy-check` as well.

2. **"Across rows" / "across tables" is coined vocabulary.** These are good plain-language labels, but the rest of the page uses different words for the same ideas: "splitting across rows", "processed one at a time, in order", and "serial" (in the method name). Tie the terms together explicitly, for example: "Evaluating a column's rows in order, on one thread, is called *serial* evaluation." "Across tables" never comes up again, so either develop it or cut it from the intro.

3. **"Tick" is undefined jargon** ("independent tables tick at the same time"). A concept guide can use it, but only after defining it or linking to the definition, for example: "tables that update ([tick](../ticking-tables-link.md)) in the same cycle". The existing parallelization guide describes this as parallel processing of updates across independent nodes of the [DAG](../dag.md). Consider using or linking to that framing.

4. **An internal config property is dropped into a parenthetical without explanation.** In "(the engine then skips splitting across rows, controlled by `QueryTable.statelessSelectByDefault`)", the reader gets a configuration key with no definition and no link, in the middle of a sentence about something else. The parenthetical also muddles two concepts. The property sets the *default* statelessness for all selectables, while `with_serial` marks one column as serial. Neither controls the other (see `parallelization.md:59` and `docs/groovy/conceptual/query-table-configuration.md`). **Fix:** Take it out of the parenthetical. If the page mentions the property at all, give it its own sentence, for example: "To make every column stateful by default, set the `QueryTable.statelessSelectByDefault` configuration property to `false`." Link it to the query table configuration page. Send the exact semantics to accuracy check.

5. **"On the column" is loose terminology.** `with_serial` is a method on a `Selectable` (and `Filter`), not on a column. Say "Call `with_serial` on the column's `Selectable`" and add a short code example (for example, `Selectable.parse("A = counter()").with_serial()`, as in `parallelization.md:162`). Otherwise the reader can't tell where the method goes.

## Mechanical checks (required searches)

| Check | Result |
| --- | --- |
| `` `\.[a-z] `` (leading dot in prose) | **1 hit:** `` `.with_serial()` `` in "Unsafe formulas". It's a method, not a file extension. Remove the leading dot. |
| Backticked identifier + empty `()` in prose | **1 hit:** same `` `.with_serial()` ``. Remove the empty parentheses, so it becomes `` `with_serial` ``. |
| First mention of a linkable method is bare | **1 hit:** `with_serial` is mentioned only once, and that mention is bare. Link it to `../../reference/query-language/types/ConcurrencyControl.md#with_serial`, adjusting the relative path for where the page ends up. `update` and `empty_table` appear only in code, so no link is needed. |
| `[here]` / `[click here]` / `[this page]` | None. |
| Smart quotes | None. |
| Em/en dashes, hyphens used as dashes | None. |

**Combined fix for the last sentence:** "Use [`with_serial`](../../reference/query-language/types/ConcurrencyControl.md#with_serial) on the column's `Selectable` so that the engine evaluates its rows one at a time, in order."

## Prose quality

- **Passive voice:** "so that rows are processed one at a time, in order" — the actor, the engine, matters here. Suggested rewrite: "so that the engine evaluates rows one at a time, in order."
- **One idea per paragraph / sentence overload:**
  - The opening paragraph joins two definitions with a semicolon. Split it into two sentences, or into two short paragraphs if "across tables" stays.
  - The final sentence carries the instruction, a parenthetical about behavior, and a config key all at once. Split it as described in item 4.
- **Vague phrasing:** "doesn't touch anything else" is informal and imprecise. Say what matters: "doesn't read or modify any state outside its input columns, and doesn't depend on the order in which rows are evaluated."
- **"Automatically":** "the engine runs them across rows automatically" is fine. You could note that this is the default behavior, which ties into item 4.
- **Tone:** The conversational, explanatory tone and the contractions ("doesn't", "isn't") suit a concept guide. No change needed.

## Headings and structure (style-level only)

- The headings are in sentence case, with no links. That's fine, apart from the terminology change in item 1.
- There is no "Related documentation" section. This is an excerpt, but the finished concept guide needs one at the end. Candidates: the parallelization concept guide, the `ConcurrencyControl`/`Barrier` reference pages, and the formula threads how-to.

## Code

- `from deephaven import empty_table` is a targeted import. Good.
- Column names (`X`, `Y`) are capitalized, the variable (`source`) is snake_case, and the formula strings have spaces around operators (`"X = i"`, `"Y = X * 2"`). All fine.
- The code block has no tag. Consider `order=source`, per the code example tags.
- The "Unsafe formulas" section has no code, even though it's the section that introduces the API. Add an example that uses `with_serial` (see item 5).

## Handoffs (out of scope for style)

- **Accuracy (`deephaven-core-accuracy-check`):**
  - The claim that thread-safe equals stateless.
  - What `QueryTable.statelessSelectByDefault` actually controls, compared with `with_serial`.
  - Whether "each piece on its own core" and "independent tables tick at the same time" describe the engine accurately. The existing guide ties parallel processing of updates to the Update Graph Processor Thread Pool size.
  - Whether the `i`-based example suits a page about parallel evaluation.
