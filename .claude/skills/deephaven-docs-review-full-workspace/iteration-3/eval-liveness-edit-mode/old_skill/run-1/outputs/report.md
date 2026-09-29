Skills used: deephaven-docs-review-full (which runs ref-deephaven-doc-categories, deephaven-core-accuracy-check, deephaven-doc-structure-review, deephaven-core-accuracy-spot-check and deephaven-writing-style in that order)

# Liveness scopes concept guide: full review, fixes applied

**Category:** Concept guide (`docs/{python,groovy}/conceptual/`). The accuracy pass found the same wrong claims in the Groovy page, so I edited and reviewed both files.

I edited only the two working copies:
- `.../editmode-sandbox/old_skill-r1/python-liveness-scope-concept.md`
- `.../editmode-sandbox/old_skill-r1/groovy-liveness-scope-concept.md`

I did not run any of the code examples.

## Accuracy (step 1, both files unless noted)

1. **The "fixed" example didn't fix anything.** The scoped version of the crypto tree query never released the scope and never preserved anything, so it held on to every object just as the unscoped version did.
   - **Python:** I added `scope.preserve(crypto)` and `scope.preserve(combo_tree)` inside the `with` block, then `scope.release()` after it.
   - **Groovy:** I added `LivenessScopeStack.peek().manage(crypto)` and `LivenessScopeStack.peek().manage(comboTree)` after the `try` block, then `scope.release()`.
   - **Why this works:** `AbstractScriptSession.evaluateScript` puts the session's own scope on the stack while a script runs. So once the inner scope is gone, the session's scope is the "next outer" scope, and `preserve` and `peek().manage` hand the tables to it. `crypto` has to be kept too, because the page shows it as an open table.
2. **The "How liveness scopes work" section was rewritten from source.** The old text said:
   - cleanup only happens for "objects created purely for the GUI";
   - releasing a scope lets go of objects "no longer needed or are not refreshing";
   - the parents' count drives cleanup.

   The source shows something different:
   - New objects are managed by whatever scope is on top of the stack when they are created (`LivenessArtifact`).
   - The console session's scope keeps everything a script creates until the session closes.
   - When an object's count reaches zero it is destroyed and may become unusable.
   - Refreshing tables keep references to their parents (`BaseTable.addParentReference`).
   - Releasing a scope drops its references to everything it manages.

   The same false claims in "How to use a liveness scope" were removed.
3. **"More control over garbage collection" and "best practice… conserve memory" were overstated.** Liveness scopes control when objects stop updating; the JVM still frees the memory later. Both were reworded.
4. **Groovy only: `pop` does not release a scope.** A code comment and a paragraph both said it did. `LivenessScopeStack.pop` only takes the scope off the stack. I added `scope.release()` to the push/pop example and corrected the text.
5. **Python only: the create example was wrong.** `scope_from_method = liveness_scope()` does not create a scope. It returns a context manager, and the scope only exists inside a `with` block. I replaced it with a `with liveness_scope() as ...:` form.
6. **Python only: method list and wording.**
   - `manage` and `unmanage` act on *this* scope, not "the current scope".
   - `preserve` hands the object to the *next outer* scope and must be called while the scope is open.
   - `liveness_scope` is a function, not a method.
7. **Python only: broken examples.**
   - The class example returned an undefined variable (`some_ticking_table`); it now returns `ticking_table`.
   - The function example had a `return` outside any function; it is now wrapped in one.
8. **Both: wrong setup text.** The tree is grouped by `Instrument`, not "Sym". The tables left open are `crypto` and `combo_tree`/`comboTree`, not `crypto` and `data`.
9. **Groovy only: the methods list was incomplete.** I added `push`, `computeEnclosed` and `computeArrayEnclosed`, plus a pointer to `LivenessScope.release`, and fixed the "useful enclosing" typo.

## Structure (step 2)

- **Reordered sections:** "How liveness scopes work" now comes before "Why use a liveness scope?", because the reason only makes sense once the mechanism is explained.
- **Trimmed overlaps:**
  - The second intro paragraph repeated the first section, so I cut it down to orientation.
  - "Why use" is now a short paragraph instead of repeating the mechanism.
- **Python only: merged the "function or class" comparison.** It was spread across three places ("How to create", the two subsections, and a closing "To use the method or the class?" section). It now appears once, where both options are first named, and the closing section is gone.
- **Renamed headings:**
  - Python: "The method" is now "Use the `liveness_scope` function" and "The class" is now "Use the `LivenessScope` class".
  - Groovy: "Multiple LivenessScopes" is now "Nested liveness scopes".
  - No page links to either file with an anchor. The five inbound links, in both languages' `formula-threads.md` and `reference/engine/LivenessScope.md`, plus Python's `reference/engine/liveness-scope.md`, point at the page only.
- **Groovy only:** the developer-only paragraph was folded into the rewritten "How liveness scopes work".

## Re-verification (step 3)

- I re-checked every moved, merged or reworded paragraph against source.
- The "function or class" guidance from the deleted section still exists in the merged bullets.
- All internal link targets exist.

## Style (step 4)

- Removed future tense ("will") and "we/let's".
- Replaced hyphens used as dashes with em dashes.
- Removed empty parentheses from method names in prose (e.g. `peek`, `open` with no arguments).
- Changed first mentions of `LivenessScope` to link to its reference page, and removed repeated links.
- Changed `"A={a}"` to `"A = {a}"`.

## Not resolved

- **The demo uses static CSV data.** Liveness bookkeeping mostly affects refreshing objects; for example, `TreeTableImpl` only keeps a reference to its source if the source is refreshing. So the before-and-after comparison doesn't really show the benefit. I'd suggest switching to a ticking source such as `time_table`, but that is a change to the example's design and needs an author or SME decision.
- **The examples need to be run and snapshots regenerated.** This matters most for the two new `order=null` blocks: the release and preserve additions, and Python's new `with liveness_scope() as scope_from_function: pass` block.
- **External links not checked.** I could not confirm that the new javadoc anchors resolve (`#push(...)`, `#computeEnclosed(...)`, `LivenessScope.html#release()`).
- **The title is "How to use liveness scopes" on a concept guide.** I left it alone because other pages link to it using that wording.
- **Problems in other files that I was not allowed to edit:**
  - `docs/python/reference/engine/liveness-scope.md` uses `@liveness_scope` with no parentheses. That raises `TypeError`, because the function takes no arguments; it should be `@liveness_scope()`. Its Syntax and Returns sections also suggest that `liveness_scope()` returns a `SimpleLivenessScope`; it actually returns a context manager.
  - The module docstring in `py/server/deephaven/liveness_scope.py` has the same `@liveness_scope` bug, plus the `some_ticking_table` name mistake and a "manange" typo.
