Skills used: deephaven-docs-review-full, with deephaven-core-accuracy-check, deephaven-doc-structure-review, deephaven-core-accuracy-spot-check, deephaven-writing-style and ref-deephaven-doc-categories.

# Liveness scopes concept guide: full review and fixes

**Category:** Concept guide (`docs/{python,groovy}/conceptual/`).
**Verdict:** needs revision.

The page's main lesson was wrong in both languages. Neither "liveness scope" example ever released its scope, so the page claimed to free objects that the code actually kept alive. On top of that, the Groovy page said `LivenessScopeStack.pop` releases a scope. It doesn't. I fixed these in place, plus a set of smaller accuracy and style problems. I didn't restructure or retitle anything.

**None of the edited examples have been run.** They are the executed `order=null` blocks: Python lines 64–105 and 111–119, Groovy lines 50–81. They need a snapshotter run before merge.

## What I changed (both files unless noted)

**Accuracy fixes, highest impact first:**

1. **The "enclose it in a scope" example never released the scope.** A scope that is never released keeps everything it manages alive. That is the opposite of what the page claims.
   - Python now calls `scope.preserve(crypto)` and `scope.preserve(combo_tree)` inside the `with` block, then `scope.release()`.
   - Groovy now calls `LivenessScopeStack.peek().manage(...)` on `crypto` and `comboTree` after the `try` block, then `scope.release()`.
   - Source checked: `LivenessScope.release()`, `_BaseLivenessScope.preserve`, `LivenessScopeStack.open`/`peek`. Tree tables are liveness objects too (`HierarchicalTable extends LivenessReferent`).
2. **Groovy: `pop` does not release.** `LivenessScopeStack.popInternal` only removes the scope from the stack; `release()` is a separate call.
   - The code comment said pop "will release the scope's references." I corrected it and added `scope.release()` to the push/pop example.
   - The prose said pop makes the objects "eligible for garbage collection." I rewrote it to describe pop and release separately.
3. **Wrong description of what happens by default.** "Reference counting will only clean up objects created purely for the GUI" and "objects ... not refreshing are let go" were both wrong.
   - `AbstractScriptSession.evaluateScript` runs every console script inside the session's own scope, which doesn't release until the session closes.
   - Static tables are also managed (`LivenessArtifact` constructor).
   - I rewrote the passage to say that: script objects stay with the console session, and releasing a scope cleans up whatever nothing else references.
4. **Cleanup mechanics.** "Updated proactively" is now "cleaned up proactively." I rewrote the "parents' liveness count goes down" sentence: when an object's count reaches zero it is destroyed, stops updating, and drops its references to its parents (`ListenerImpl.destroy` → `removeUpdateListener`). "Much more control ... over garbage collection" is now "more control over when nodes ... are cleaned up."
5. **"A node can be a table, plot, or any other object."** No plot or figure class is a liveness object. It now reads "a table, a listener, or another query engine object."
6. **"Tree table grouped by Sym" and "Two tables will open: `crypto` and `data`."** There is no `Sym` column, and `data` is set to `None`/`null`. It now says grouped by `Instrument`, and the tables that open are `crypto` and `combo_tree` (Groovy: `comboTree`).
7. **"This is best practice"** contradicted the page's own "in most cases, this is acceptable." I replaced it with a plain statement of the benefit.
8. **Python only:**
   - `scope_from_method = liveness_scope()` returns a context manager, not a scope. The example now uses `with liveness_scope() as scope_from_function:`.
   - `liveness_scope` is a function, so I renamed the headings "The method" → "The function" and "To use the method or the class?" → "To use the function or the class?". No page links to either heading.
   - `manage`, `unmanage` and `preserve` now say "this scope" or "next outer scope" rather than "the current scope."
   - `release` no longer claims to "close all of its managed resources."
   - The `liveness_scope` description now mentions the `preserve` exception, so it no longer contradicts the example right below it.
   - `skip-test` snippets: I wrapped the snippet that had a top-level `return` (a SyntaxError) in a function, and fixed `return some_ticking_table` → `return ticking_table`.

**Style fixes:**
- Removed future-tense "will" in about 10 places.
- Groovy: hyphen changed to an em dash; "useful enclosing" → "useful for enclosing".
- "DAG" (never defined) → "update graph"; "Without liveness scope" → "Without a liveness scope".
- Python: two absolute pydoc links changed to the relative `/core/pydoc/...` form used across the rest of the docs (658 relative vs. 81 absolute).

**Links:** Every internal link target exists. No other page links to an anchor on either page, so the heading renames broke nothing.

## Recommendations I didn't apply (need an author decision)

- **Retitle.** The title "How to use liveness scopes" labels a Concept guide as a how-to.
- **Replace the demo data.** It uses static CSV tables, which never update. The problem liveness scopes solve (intermediate refreshing tables that keep updating) never shows. A ticking source such as a time table or function-generated table would teach the right lesson.
- **Merge duplicated explanations.** "How liveness scopes work" and the opening paragraph of "How to use a liveness scope" explain the same idea twice.
- **Decorator snippet** (Python lines 146–149) uses `npt`, `np` and `dhnp` without importing them.
- **Groovy link style.** It mixes absolute and relative javadoc links within the page.
- **Related documentation.** Link text is Title Case ("Execution Context", "Table Update Model"), and it isn't clear why the Execution Context page is listed.

## Things I couldn't resolve (author queries)

- **AQ1 [How to use a liveness scope, para 1]:** I read the original "GUI" claim as meaning objects the server exports to the web UI, which it releases when the client lets go of them (`SessionState` export `dropReference`). Is that what you meant?
- **AQ2 [Demonstrating the problem]:** With static tables, releasing the scope doesn't noticeably free memory, because `combo_tree` still holds strong references to its sources. Should the demo switch to ticking data (see recommendations)?
- **AQ3:** Please run the edited executed examples through the snapshotter.

## Problems on other pages (I didn't edit these)

- `docs/python/reference/engine/liveness-scope.md`: the decorator example uses `@liveness_scope` with no parentheses. Because it's a `contextlib.contextmanager` that takes no arguments, that raises a TypeError when the decorated function is called. It should be `@liveness_scope()`.
- `py/server/deephaven/liveness_scope.py` (module docstring, which feeds the Pydoc page):
  - the same `@liveness_scope` decorator bug;
  - `return some_ticking_table` where it should be `ticking_table`;
  - a typo, "manange" for "manage".

## Keep in any revision

- The Python/Groovy side-by-side structure and the nested try-with-resources example.
- The Groovy "Methods" list, which matches the `LivenessScopeStack` javadoc.

Edited files:
- `/private/tmp/claude-501/-Users-margaretkennedy-deephaven-core/608a4061-7881-4a50-8095-8d9cda8cc669/scratchpad/editmode-sandbox/i4-with_skill-r3/python-liveness-scope-concept.md`
- `/private/tmp/claude-501/-Users-margaretkennedy-deephaven-core/608a4061-7881-4a50-8095-8d9cda8cc669/scratchpad/editmode-sandbox/i4-with_skill-r3/groovy-liveness-scope-concept.md`