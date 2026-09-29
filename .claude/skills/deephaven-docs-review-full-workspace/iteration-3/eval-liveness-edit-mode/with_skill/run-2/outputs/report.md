Skills used: deephaven-docs-review-full, which ran deephaven-core-accuracy-check, deephaven-doc-structure-review, deephaven-core-accuracy-spot-check and deephaven-writing-style, with ref-deephaven-doc-categories for calibration.

# Full review: liveness scopes concept guide (Python + Groovy)

**Category:** Concept guide (`docs/{python,groovy}/conceptual/`). I edited both working copies.

## Editorial summary
Before this pass, the page taught the wrong lesson: the before-and-after example that shows the problem and its fix didn't show the problem. It used static CSV tables, where liveness does almost nothing, and the "fixed" version never released its scope. It also got several mechanics wrong, most seriously the Groovy claim that `pop` releases a scope. I rewrote both pages around a correct model, a runnable ticking example, and an earlier comparison of the two approaches.

**Developmental verdict (before edits): needs restructuring.**

## What I changed (both files unless noted)

**Accuracy fixes (each verified against source):**
1. **The example that shows the problem was replaced.**
   - The old static CSV tree-table example set `data`/`combo` to `None` and wrapped the query in a scope that was never released (`LivenessScope()` + `scope.open()` in Python, `open(scope, false)` in Groovy). That freed nothing.
   - It was also wrong about details: it said "grouped by Sym" (the key is `Instrument`), and "Two tables will open: `crypto` and `data`" (it's `crypto` and `combo_tree`, per the page's own snapshot).
   - The new pair uses a `time_table` source. In the first block, a throwaway `last_by` table exists only to take a `snapshot`. In the second, the same query runs inside a scope, and `preserve` keeps the snapshot (Groovy uses `LivenessScopeStack.peek().manage`).
   - Source basis:
     - `AbstractScriptSession.evaluateScript` runs every console script under the session's scope, and that scope is released only in `destroy`.
     - Parents hold child listeners through `WeakSimpleReference`, and `LivenessScope()` defaults to weak references. So an unreferenced ticking table keeps updating until GC.
     - `QueryTable.snapshotInternal` builds its result on a static `emptyTable(1)`, so the snapshot doesn't hold on to the `last_by` table.
2. **"Cleaned up immediately without waiting for GC" / "more control … over garbage collection".** Rewritten. Reaching zero calls `destroy()`, which removes the node's listener from its parent and releases its references to the parent (`BaseTable.ListenerImpl.destroy`, `TimeTable.destroy`). Memory is still reclaimed by GC.
3. **Groovy: "pop … will release the scope's references" / "making … eligible for garbage collection".** This was wrong. `pop` only removes the scope from the stack (`LivenessScopeStack.popInternal`). The example now calls `scope.release()`, and the `open(scope, false)` example shows a release step.
4. **"Reference counting … will only clean up objects created purely for the GUI".** This overstated absolute is gone, replaced by the console-session-scope explanation.
5. **Python `scope_from_method = liveness_scope()`.** Removed. Outside a `with` block or decorator, that call returns a context manager and opens no scope (`@contextlib.contextmanager`).
6. **Python class example.** It returned `some_ticking_table`, a variable that was never defined. It's now a runnable example that returns `filtered`.
7. **Python "method" example.** It had a `return` outside any function and missing imports. It's now a runnable decorator example.
8. **Python `manage`/`unmanage`.** Changed "the current scope" to "this scope": the methods act on `self.j_scope`, not the top of the stack. I also noted that `preserve` must be called while the scope is open, because it pops `self` and raises an error otherwise.
9. **New caveat: releasing destroys managed objects even when a variable still refers to them.** Verified: liveness counts are separate from variable references.
10. **Groovy methods list.** Added `push`, which the page already used but never listed. I also fixed the grammar error copied from the javadoc ("useful enclosing").

**Structure:**
- Retitled "How to use liveness scopes" to "Liveness scopes", since this is a concept page; `sidebar_label` is unchanged.
- The intro now orients the reader instead of repeating the "Why" section.
- New order: How liveness works → Why use a liveness scope? → Create and use a liveness scope.
- Python: the function-vs-class comparison moved up to where both are first named (it was at the end). Headings are now "The `liveness_scope` function" and "The `LivenessScope` class", and "Scope methods" became its own subsection. The page now says "function" throughout; it previously said both "method" and "function".
- Groovy: try-with-resources comes first. Manual push/pop is its own subsection. The NOTE callout became prose.
- Re-verify step: every rewritten claim above was checked. No other page links to an anchor on these pages, so the heading renames are safe. All relative links resolve.

**Examples:**
- The problem/fix, decorator and class examples are now `ticking-table order=null` and meant to be runnable, where before they were `skip-test` blocks with placeholder code. The manual Groovy push/pop example stays `syntax`-style `skip-test` on purpose.

**Style:**
- Fixed hyphens used as dashes, and "will" future tense (about 6 instances).
- Removed empty parentheses from method names in prose (about 5).
- Related-documentation labels now match page titles: "Table Update Model" became "Incremental update model", and "Execution Context" became "Execution context".
- First mentions of `lastBy`/`last_by`, `snapshot` and `to_numpy` now link to their reference pages.

## Couldn't resolve / author queries
- **AQ1 [all new code blocks]:** I didn't execute them. Please run the snapshotter on both pages. The old snapshot JSONs for the removed CSV blocks (`docs/python/snapshots/...`) will be stale.
- **AQ2 [Why use a liveness scope?, para 1]:** Please have an SME confirm we want to tell readers that console code runs inside a session-owned scope. It's accurate (`AbstractScriptSession`), but it exposes an internal detail.
- **AQ3 [Groovy methods list]:** Should `LivenessScopeStack.computeEnclosed` be documented? It exists and does what the Groovy fix example does by hand. I left it out.

## Out-of-scope follow-ups (other files, not edited)
- `docs/python/reference/engine/liveness-scope.md:69`, `docs/python/reference/engine/LivenessScope.md:50` and `docs/groovy/reference/engine/LivenessScope.md:55` use the link text "How to use Liveness Scopes". Update it to "Liveness scopes".
- `docs/python/reference/engine/liveness-scope.md` has a decorator example written `@liveness_scope` without parentheses. Because `liveness_scope` takes no arguments, that form fails when the function is decorated.
- The pydoc module docstring in `py/server/deephaven/liveness_scope.py` has the same bare `@liveness_scope`, plus:
  - `return` outside a function
  - the undefined `some_ticking_table`
  - the typo `scope.manange`
- `docs/groovy/how-to-guides/data-indexes.md:335` opens `LivenessScopeStack.open(scope, false)`, and the scope never seems to be released in the part I read.

## Strengths kept
- The before-and-after way of teaching (the problem, then the scope that fixes it).
- The clear split in Python between the `liveness_scope` function and the `LivenessScope` class.
- The Groovy nested-scope example and the javadoc-linked methods list.

Edited files:
- `/private/tmp/claude-501/-Users-margaretkennedy-deephaven-core/608a4061-7881-4a50-8095-8d9cda8cc669/scratchpad/editmode-sandbox/with_skill-r2/python-liveness-scope-concept.md`
- `/private/tmp/claude-501/-Users-margaretkennedy-deephaven-core/608a4061-7881-4a50-8095-8d9cda8cc669/scratchpad/editmode-sandbox/with_skill-r2/groovy-liveness-scope-concept.md`