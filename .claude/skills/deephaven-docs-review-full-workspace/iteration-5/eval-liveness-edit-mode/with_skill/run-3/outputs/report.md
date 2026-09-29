Skills used: deephaven-docs-review-full, which ran deephaven-core-accuracy-check, deephaven-doc-structure-review, deephaven-core-accuracy-spot-check and deephaven-writing-style, with ref-deephaven-doc-categories for calibration.

# Full review: liveness scopes concept guide (Python and Groovy)

**Category:** Concept guide (`docs/{python,groovy}/conceptual/`). In the Python sidebar it sits under "Advanced Python topics". Both files were in scope because the same wrong claims appear in each.

I edited both working copies. None of the example code was executed, so the two examples I changed need a run before merge (see AQ1).

## Editorial summary

The page's purpose is clear enough: clean up intermediate update-graph nodes as soon as you're done with them, instead of waiting for GC. But its main example never delivered that. The "with a scope" version opened a scope, never released it and never kept the result tables, so it did nothing the plain version didn't. In Groovy, the page also said popping a scope from `LivenessScopeStack` releases what it manages. That's wrong: `pop` only checks that the scope is on top and removes it (`popInternal`), and only `release()` drops the references.

**Verdict: needs revision.** I applied targeted fixes. The bigger changes below are recommendations only.

## Developmental notes (report only)

1. **The payoff is never shown.** Both demo examples use a static CSV, so nothing ticks and nothing visibly stops. Liveness matters most for refreshing tables. Recommendation: replace the demo with a small `time_table`-based example. That's a new example, so I didn't write it.
2. **"There are cases where a query can benefit" never names the cases** ("Why use a liveness scope?", last sentence). The source gives the real reason, and it should be stated plainly. `AbstractScriptSession.evaluateScript` runs every console script inside `LivenessScopeStack.open(queryScope, false)`, with the comment "retain any objects which are created in the executed code, we'll release them when the script session closes". So script-created intermediates otherwise wait for GC or for the session to end.
3. **The title "How to use liveness scopes" is a how-to title on a concept page.** I suggest something like "Liveness scopes". I didn't retitle it.
4. **Order:** the demo uses `LivenessScope` before "How to create a liveness scope" introduces it. Consider moving the demo after the create/use sections, or adding a one-line forward reference.

## Accuracy (fixed, both files unless noted)

1. **Groovy: `pop` does not release.** The push/pop example comment and the paragraph after it said popping releases the managed referents or makes them eligible for GC. I corrected both and added `scope.release()` to the example.
2. **The main demo example didn't do what its text said.**
   - **Python:** added `scope.preserve(crypto)` and `scope.preserve(combo_tree)` inside the `with scope.open():` block, then `scope.release()` after it.
   - **Groovy:** added `LivenessScopeStack.peek().manage(crypto)`, `LivenessScopeStack.peek().manage(comboTree)` and `scope.release()` after the try block.
   - Without keeping the two tables, `release()` would destroy tables the console still displays, because nothing else manages them. The Python binding's `preserve` pops this scope and manages the object in `LivenessScopeStack.peek()`, which is the session's query scope. Neither Groovy binding assignment nor Python globals manage variables.
   - I rewrote the lead-in so it describes what the example now does.
3. **"Two tables will open: `crypto` and `data`":** `data` is set to None/null. The existing snapshot confirms the outputs are `crypto` and `combo_tree` (`comboTree` in Groovy). Also, "grouped by Sym" is now "grouped by `Instrument`".
4. **"any objects that are no longer needed or are not refreshing are let go":** whether a table is refreshing isn't the test. Static tables are managed the same way (`LivenessArtifact` constructor calls `manageWithCurrentScope`). This now says what `release` does: it drops its references, and anything no longer referenced elsewhere is cleaned up.
5. **Wrong wording in the intro and "How liveness scopes work":**
   - Nodes are "cleaned up proactively", not "updated proactively".
   - Liveness scopes don't control GC. They control when cleanup happens.
   - "table, plot, or any other object" is now "or another query engine object". I confirmed `FigureWidget` is a liveness node.
   - The reference-count sentence now matches `onReferenceCountAtZero`: the object is destroyed and drops its references to its parents.
6. **Python examples:**
   - `liveness_scope()` example: `scope_from_method = liveness_scope()` holds a context manager, not a scope. It's now `with liveness_scope() as scope_from_function:`.
   - skip-test block, first example: `return` was outside a function, which is a syntax error. I wrapped it in `def make_joined_table():` and added the missing imports.
   - skip-test block, class example: `some_ticking_table` was undefined; it should be `ticking_table`.
   - Method bullets: `manage` and `unmanage` act on *this* scope, not "the current scope". `preserve` must be called while the scope is open, since otherwise the binding raises `DHError`. The "Choosing…" section now says the function's scope releases objects on exit unless they're preserved.
7. **"This is best practice because it allows Deephaven to conserve memory"** contradicted the page's own "In most cases, this is acceptable". It now says that releasing the scope cleans the objects up immediately.
8. **Groovy Methods list was missing `push`,** which the page itself uses. I added it.
   - Also not listed: `computeEnclosed` and `computeArrayEnclosed`. They're advanced, so I left them out; add them if you want the list complete.

## Structure

- **Renamed Python headings:**
  - "The method" is now "The `liveness_scope` function".
  - "The class" is now "The `LivenessScope` class".
  - "To use the method or the class?" is now "Choosing the function or the class".
  - `liveness_scope` is a function. I searched every doc page: none links to these anchors, and the inbound links (sidebars, `formula-threads.md`, the reference pages) point only at the page itself.
- **Not applied:**
  - The "How liveness scopes work" paragraph and the page intro overlap.
  - The Groovy "Methods" list re-explains `open()` and `open(scope, true)`, which are already covered above it.

## Examples

- The four `skip-test` blocks rely on placeholders (`some_ticking_source`, `other_ticking_table`, `key_cols`), so the docs snapshotter never tests them. Consider runnable versions.
- The two changed demo blocks are `order=null` and will run in the snapshotter. Their snapshot files will need regenerating.

## Style (applied)

- Removed future tense "will" in about 8 places across both files.
- Switched hyphen separators to spaced em dashes: 6 instances (the Groovy Methods list and one sentence).
- Removed about 6 repeated inline links to `LivenessScope` within the same paragraphs (Groovy).
- Moved the first `LivenessScope` link to the reference page. The absolute `https://docs.deephaven.io/core/pydoc` links were a minority form in the corpus: 88 uses against 756 for `/core/pydoc`.
- Removed empty parentheses from prose method references.

Not changed: the "Related documentation" link text is Title Case ("Table Update Model"), and the Groovy Methods labels keep `()` to tell the `open` overloads apart.

## Author queries

- **AQ1 [Demonstrating the problem, scoped examples]:** please run both edited examples in a live console. Check that `crypto` and `combo_tree`/`comboTree` still display after `release()`, and that nothing throws.
- **AQ2 ["How to use a liveness scope", para 1]:** is "reference counting instrumentation only cleans up objects created purely for the GUI" the intended claim? It fits the fact that script-created objects are held by the session scope, but a reader may not follow it.

## Couldn't fix (other pages)

- `docs/python/reference/engine/liveness-scope.md` has the same misleading `scope = liveness_scope()` syntax line, and calls the function a "Python method".
- The snapshot for the edited Python example under `docs/python/snapshots/` (and its Groovy counterpart) needs regenerating.

## Strengths

- The method-versus-class split in Python is a sensible way to organize the page.
- The Groovy try-with-resources examples and the nested-scope example are clear.
- The "Why use" section opens with a clear GC motivation.

Edited files:
- /private/tmp/claude-501/-Users-margaretkennedy-deephaven-core/608a4061-7881-4a50-8095-8d9cda8cc669/scratchpad/editmode-sandbox/i5-with_skill-r3/python-liveness-scope-concept.md
- /private/tmp/claude-501/-Users-margaretkennedy-deephaven-core/608a4061-7881-4a50-8095-8d9cda8cc669/scratchpad/editmode-sandbox/i5-with_skill-r3/groovy-liveness-scope-concept.md