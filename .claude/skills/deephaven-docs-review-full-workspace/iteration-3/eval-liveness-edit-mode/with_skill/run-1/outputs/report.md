Skills used: `deephaven-docs-review-full`, which ran `ref-deephaven-doc-categories`, `deephaven-core-accuracy-check`, `deephaven-doc-structure-review`, `deephaven-core-accuracy-spot-check` and `deephaven-writing-style`.

# Full review: liveness scopes concept guide (Python and Groovy)

**Category:** Concept guide (`docs/{python,groovy}/conceptual/`). Both working copies are edited. I haven't run any of the new examples; they are checked against source only.

## Editorial summary
The page was accurate in places, but it gave the reader the wrong picture of what a liveness scope does. Its demonstration never released the scope, so it showed nothing. It used static CSV tables, which don't show the real problem. And in Groovy it said that popping a scope releases the objects it manages, which it doesn't. The page also never stated its main point: releasing a scope cleans up unneeded ticking tables straight away instead of waiting for the JVM garbage collector, and you use `preserve` to keep the objects you want.

**Verdict before edits: needs restructuring.** I rewrote both pages around a single example: a ticking table used only to take a snapshot, shown first without a scope and then with one.

## Developmental notes
1. **Purpose and main point were missing.** The title said "How to use", the intro only listed contents, and the benefit was never stated. The new intro says what a scope does and when you need one, and the title is now "Liveness scopes". The sidebar label is unchanged, and no page links to an anchor on this page, so the heading renames break nothing.
2. **The picture of how it works was wrong.**
   - The old text said nodes are "updated proactively" (the feature is about cleanup, not updating).
   - It said scopes give "control over garbage collection" (they don't).
   - It said a node can be "any other object" (only liveness referents take part).
   - It described cleanup as flowing from parent to child, when a child keeps its parents alive.
   - The new "How Deephaven tracks liveness" section replaces all of this with a model checked against source.
3. **The explanation came in the wrong order.** It was split across "Why", "How it works" and "How to use", with the creation methods explained twice. Both pages now run in this order: how it works, the problem, the fix, then the API.

## Accuracy (all checked against source)
1. **The demonstration example was defective (Python and Groovy).**
   - The scope was opened and never released.
   - The tables were static CSV reads, so nothing was ticking.
   - If the scope had been released, it would have destroyed `combo_tree` and `crypto`, because nothing preserved them.
   - The prose said "grouped by Sym" (the column is `Instrument`) and "Two tables will open: `crypto` and `data`" (`data` is set to `None`).
   - I replaced it with a time table plus `snapshot`. In source, `TimeTable.destroy()` calls `registrar.removeSource(refresher)`, and the update graph holds its sources through a `WeakReference`. So an unreferenced ticking table keeps updating until garbage collection, and releasing its scope stops it at once.
2. **Groovy: `pop` does not release.** The code comment "This will release the scope's references" and the claim that `pop` makes objects "eligible for garbage collection" were both wrong. `popInternal` only calls `stack.pop()`, and releasing happens only in `PopAndReleaseOnClose` or through `LivenessScope.release()`. I fixed the prose and the example, which now calls `scope.release()`.
3. **Groovy: `open()` and `open(scope, true)` destroy results nobody kept.** The examples said "Your query here" with no warning. Neither session manages script variables when they're assigned, so anything created in the block dies when it closes. I added a warning and a pattern that works: take `enclosing = LivenessScopeStack.peek()` before the block, then call `enclosing.manage(snap)` inside it. This works because `evaluateScript` opens the session's query scope on the same thread.
4. **Python: `scope_from_method = liveness_scope()` does not create a scope.** `liveness_scope` is a `@contextlib.contextmanager`; the scope only exists once it is bound with `with ... as scope`. I removed that example.
5. **Python snippets that couldn't run.**
   - A `return table` sat outside any function.
   - `npt`, `np` and `dhnp` were never imported.
   - `return some_ticking_table` referred to a variable that doesn't exist.
   - I replaced them with complete examples tagged `ticking-table order=null`.
   - The decorator form `@liveness_scope()`, with parentheses, was already correct and is kept.
6. **Python method list.**
   - `manage` and `unmanage` act on *this* scope, not "the current scope".
   - `preserve` has to be called while this scope is on top of the stack.
   - The pydoc warns about managing an object twice; I added that.
   - The claims that `open` and `release` exist only on the class are confirmed.
7. **The old "only clean up objects created purely for the GUI" sentence** is replaced with the two cases:
   - Objects a client requests are managed by that client's session exports (`SessionState.ExportObject` in source) and cleaned up when the client releases them.
   - Objects a script creates are held only weakly by the script session: `ScriptSessionQueryScope extends LivenessScope` with the default weak constructor.
8. **Groovy method list** was missing `push` (now added), and it now says `pop` doesn't release. The javadoc anchor links are external, so check them by hand.
9. **Links:** every internal target exists. No page links to an anchor on this page.

## Structure
- I merged "How to create", "The method", "The class" and "To use the method or the class?" into one section. It now opens with a side-by-side comparison of the function and the class, followed by one subsection each.
- Groovy is now ordered: tracking, the problem, using a scope, three ways to open one, nested scopes, then a methods list.
- I moved the orphaned note for developers in Groovy's "How it works" into the intro.
- Re-check of what the restructure touched: every claim that moved or was reworded was spot-checked against the source above. No caveat was dropped; the "prefer try-with-resources" note survives and now gives a reason.

## Examples
- Each concept now has an example that shows it, and the with-scope and without-scope versions are a matched pair.
- The Groovy push/pop example ran as written, so I changed `skip-test` to `order=null`.

## Style
- Headings are now sentence case with no backticks.
- Empty parentheses are removed from method names in prose (four in Groovy). I kept three on purpose:
  - `liveness_scope()`, because the parentheses matter: `@liveness_scope` without them fails.
  - `open()` and `peek()` in the methods list, where the parentheses name the overload being described.
- The mechanical searches (method names with a leading dot, curly quotes, em-dash spacing, `[here]` link text) are clean.

## Author queries
- **AQ1** [How Deephaven tracks liveness, client bullet]: is "cleaned up when you close a table in the UI" the right example of the web UI releasing an export?
- **AQ2**: none of the new examples have been run. Please run them through the snapshotter. The old snapshot JSONs for this page under `docs/python/snapshots/` will be out of date.

## Couldn't resolve: other files need changes (not edited)
- `docs/python/reference/engine/liveness-scope.md`:
  - It uses `@liveness_scope` without parentheses, which raises a TypeError when the function is defined.
  - "Returns: A `SimpleLivenessScope`" is misleading, because the function returns a context manager that yields one.
- `py/server/deephaven/liveness_scope.py` module docstring has the same problems:
  - `@liveness_scope` without parentheses.
  - `return table` outside a function.
  - `return some_ticking_table`, which refers to a variable that doesn't exist.
  - The typo `manange`.
- The link text "How to use Liveness Scopes" no longer matches the new title in:
  - `docs/python/reference/engine/LivenessScope.md:50`
  - `docs/python/reference/engine/liveness-scope.md:69`
  - `docs/groovy/reference/engine/LivenessScope.md:55`

## Strengths kept
- The core contrast between reference counting and garbage collection.
- Groovy's list of `LivenessScopeStack` methods, which now includes `push`.
- The comparison of the function and the class, which now comes before the detail instead of after it.

Files are in `/private/tmp/claude-501/-Users-margaretkennedy-deephaven-core/608a4061-7881-4a50-8095-8d9cda8cc669/scratchpad/editmode-sandbox/with_skill-r1/`:
- `python-liveness-scope-concept.md`
- `groovy-liveness-scope-concept.md`