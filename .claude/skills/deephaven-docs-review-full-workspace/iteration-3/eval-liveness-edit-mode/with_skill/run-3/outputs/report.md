Skills used: deephaven-docs-review-full, which ran deephaven-core-accuracy-check, deephaven-doc-structure-review, deephaven-core-accuracy-spot-check (the re-verify step), deephaven-writing-style and ref-deephaven-doc-categories.

# Liveness scope concept guide: full review and fixes

**Category:** Concept guide (`docs/{python,groovy}/conceptual/`). I edited both files in place. Nearly every problem in the Python page also appeared in the Groovy page, so both got the same fixes, plus some Groovy-only ones.

## Editorial summary

The page had the wrong mental model at its core, and its main example didn't show the problem it said it solved. The worst errors:
- **Groovy `pop` claim:** the Groovy page said `LivenessScopeStack.pop` releases a scope's references. It doesn't.
- **Scope never released:** both "with scope" examples opened a scope and never released it, so they showed no benefit.
- **Unsupported GUI claim:** both pages said reference counting "will only clean up objects created purely for the GUI." Nothing in the source supports this.
- **Static example data:** the example used static CSV tables. Liveness matters most for ticking tables, which keep updating until they're cleaned up.

Verdict before edits: **needs revision.** After edits, both pages are ready for technical review, with the open items listed below.

## What I changed

### Accuracy, both files

1. **The pages now say what liveness scopes are for.** I rewrote the intro, "Why use a liveness scope?" and "How liveness scopes work." Tables and other query engine objects are reference counted, and when the count reaches zero the object is destroyed. A destroyed table stops listening for updates and gives up its references to its parents. Refreshing children hold references to refreshing parents.
   - Sources: `LivenessArtifact` (each new object is managed by `LivenessScopeStack.peek()` when it's created), `ReferenceCountedLivenessReferent.onReferenceCountAtZero` → `destroy()`, `BaseTable.addParentReference`, `ListenerImpl.destroy` → `parent.removeUpdateListener`.
   - In the console, the script session's own scope manages new objects. `AbstractScriptSession.evaluateScript` runs each script inside `LivenessScopeStack.open(queryScope, false)` and releases that scope only when the session closes.
   - Without a user scope, a discarded ticking table keeps updating until garbage collection. The update graph holds its sources through a `WeakReference` (`BaseUpdateGraph.UpdateSourceRefreshNotification`), and `TimeTable.destroy` removes the source right away.
2. **Removed the wrong claims:**
   - "only clean up objects created purely for the GUI."
   - "objects that are no longer needed or are not refreshing are let go."
   - "control ... over garbage collection."
   - "a node can be ... any other object."
   - "the memory freed." Releasing a scope destroys objects; the JVM still reclaims the memory.
3. **Replaced the demonstration pair.** The originals were static CSV tree tables, and the text claimed "grouped by Sym" and that "Two tables will open: `crypto` and `data`." Both statements were wrong. The new pair is a small `time_table`/`timeTable` query:
   - without a scope, the tables keep ticking until GC;
   - inside a `LivenessScope`, they stop ticking when `release` is called.

   The new "with scope" examples actually call `release`.

### Accuracy, Python only

4. **`liveness_scope()` example.** The old creation example assigned `scope_from_method = liveness_scope()`. That returns a context manager that hasn't been entered, not a scope (`liveness_scope` is a `@contextlib.contextmanager` generator). It now uses `with liveness_scope() as ...`, and the text says that calling it on its own doesn't open a scope.
5. **`LivenessScope` example bug.** It returned `some_ticking_table`, which was never defined (a `NameError`). It now returns `ticking_table`.
6. **`liveness_scope` example.** It had a top-level `return` and was missing imports. It's now wrapped in a function, with the numpy/`dhnp` imports added.
7. **Method descriptions made precise:**
   - `preserve` makes the next scope out manage the object, and must be called on the innermost open scope (it pops itself).
   - Exiting `LivenessScope.open` doesn't release the scope (`open` only pops).
   - `release` drops the scope's references.

### Accuracy, Groovy only

8. **Push/pop example.** The comment on `LivenessScopeStack.pop(scope)` said it releases references, and the paragraph after it said popping makes objects "eligible for garbage collection." `popInternal` only removes the scope from the stack. I fixed both and added `scope.release()`.
9. **New example for keeping a result.** It uses `LivenessScopeStack.peek()` and `outerScope.manage(snap)`. Groovy has no `preserve`, and the page never showed how to keep a result. This is the same thing Python's `preserve` does internally.
10. **Methods list:**
    - added `push`, which the page uses but didn't list;
    - `pop` now notes that it doesn't release the scope;
    - fixed "This is useful enclosing" → "useful for enclosing";
    - fixed the comment "managed by the scope created above" in the `open()` example, where no scope is created above.

### Structure

- **Python:** renamed "The method" / "The class" / "To use the method or the class?" to "The `liveness_scope` function" / "The `LivenessScope` class" / "Choose the function or the class." `liveness_scope` is a function, and the page switched between "method" and "function." The function-vs-class comparison now also appears where both are first named, under "How to create a liveness scope."
- **Groovy:** renamed "Multiple LivenessScopes" → "Nested liveness scopes."
- **Both:** trimmed the "How liveness scopes work" text that repeated the intro.
- I scanned the whole docs corpus for inbound anchor links to either page (`liveness-scope-concept.md#...`). There are none, so the renames break nothing.

### Style

- Replaced " - " separators with em dashes.
- Removed empty parentheses from method names in prose (`peek()` → `peek`).
- Made try-with-resources spacing consistent.
- Normalized the Groovy javadoc links to `/core/javadoc/...`.
- In Python, the first mention of `LivenessScope` now links to the reference page instead of the pydoc.
- Mechanical greps (dot-prefixed methods, empty parens in prose, `[here]` links, curly quotes) are all clean.

## What I couldn't resolve

- **AQ1, untested examples.** I couldn't run the new examples. The `order=null` blocks are the two demo pairs, the Python creation example, and the Groovy `peek`/`snapshot` example. They rely on APIs I verified (`time_table`/`timeTable`, `ii` on an append-only time table as used in `table-types.md`, `last_by`, `snapshot`, `LivenessScopeStack.peek`/`open`), but they need a snapshotter run.
- **AQ2, "until the session closes."** Please confirm this is the right way to describe the console session scope. It's built with weak reachability (`LivenessScope()` → `enforceStrongReachability=false`), so the page says objects stay "until they're garbage-collected or the session closes."
- **AQ3, title.** The title "How to use liveness scopes" reads like a how-to guide on a concept-guide page. I left it unchanged; renaming it is your call.

## Fixes needed on other pages (not edited)

- **`@liveness_scope` without parentheses doesn't work.** `docs/python/reference/engine/liveness-scope.md` uses it in its decorator example, and so does the `py/server/deephaven/liveness_scope.py` module docstring. With `@contextlib.contextmanager`, the bare form raises `TypeError` when the decorated function is called; you need `@liveness_scope()`. The same docstring also has the `return some_ticking_table` bug and the "manange" typo.
- **`docs/python/reference/engine/liveness-scope.md` Syntax block** shows `scope = liveness_scope()`, which has the same problem as fix 4 above.

## Strengths kept

- The Python function-vs-class framing and the `make_table_and_scope` replace-and-release pattern.
- The Groovy progression from push/pop to try-with-resources to anonymous scope to nesting.

Files edited:
- /private/tmp/claude-501/-Users-margaretkennedy-deephaven-core/608a4061-7881-4a50-8095-8d9cda8cc669/scratchpad/editmode-sandbox/with_skill-r3/python-liveness-scope-concept.md
- /private/tmp/claude-501/-Users-margaretkennedy-deephaven-core/608a4061-7881-4a50-8095-8d9cda8cc669/scratchpad/editmode-sandbox/with_skill-r3/groovy-liveness-scope-concept.md