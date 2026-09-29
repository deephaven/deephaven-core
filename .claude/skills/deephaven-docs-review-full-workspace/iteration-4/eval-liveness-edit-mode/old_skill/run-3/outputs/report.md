Skills used: deephaven-docs-review-full, which ran deephaven-core-accuracy-check, deephaven-doc-structure-review, deephaven-core-accuracy-spot-check and deephaven-writing-style, calibrated by ref-deephaven-doc-categories.

# Liveness scopes concept guide: full review, fixes applied

**Category:** Concept guide (`docs/{python,groovy}/conceptual/`). I edited both working copies:
- `.../editmode-sandbox/old_skill-r3/python-liveness-scope-concept.md`
- `.../editmode-sandbox/old_skill-r3/groovy-liveness-scope-concept.md`

I did not run any examples. Everything below was checked against source only.

## Accuracy (step 1)

**Both files**
- **Wrong description of the first example.** The page said the tree table was "grouped by Sym" and that "Two tables will open: `crypto` and `data`." The code groups by `Instrument` and sets `data`/`combo` to `None`/`null`, so the tables left open are `crypto` and `combo_tree` (`comboTree` in Groovy). Fixed.
- **The "with a scope" example never released its scope.** As written it showed no benefit at all.
  - Python now calls `scope.preserve(crypto)` and `scope.preserve(combo_tree)` inside the `with` block, then `scope.release()`. `_BaseLivenessScope.preserve` pops the scope, manages the object in `LivenessScopeStack.peek()`, and pushes the scope back.
  - Groovy now calls `LivenessScopeStack.peek().manage(...)` after the `try` block, then `scope.release()`.
- **The "How it works" and "Why" sections didn't match the source.** I rewrote them:
  - They said liveness scopes let nodes be "updated proactively" and give "much more control over... garbage collection."
  - They claimed cleanup "will only clean up objects created purely for the GUI."
  - They described the parent/child direction backwards.
  - What the source shows:
    - When a referent's count reaches zero, `destroy()` runs immediately. For a table, the listener's `destroy` calls `parent.removeUpdateListener(this)`, and the node drops the references it manages (`ReferenceCountedLivenessNode.onReferenceCountAtZero`), which cascades to parents.
    - Memory is still reclaimed by GC. `LivenessScope()` "Will only enforce weak reachability" on the objects it manages.
    - `AbstractScriptSession.evaluateScript` opens the session's own `queryScope`, commented "retain any objects which are created in the executed code, we'll release them when the script session closes." The page now says this.
    - A table manages its parent only when that parent is refreshing, so the page now notes that destroying a node matters most for refreshing tables.
- **"Conserve memory" / "deleted and the memory freed" overstated what a scope does.** Reworded: releasing a scope detaches and destroys nodes, and GC still frees the memory.
- **"A node can be a table, plot, or any other object" was too broad.** Narrowed to liveness referents such as tables and tree tables (`HierarchicalTable extends ... LivenessReferent`).

**Python only**
- **The "How to create" snippet was misleading.** `scope_from_method = liveness_scope()` returns a context manager, not a scope, because `liveness_scope` is a `@contextlib.contextmanager` that yields a `SimpleLivenessScope`. It now shows `with liveness_scope() as ...:` and `LivenessScope()` followed by `.release()`.
- **Methods list:**
  - `manage` and `unmanage` act on *this* scope, not "the current scope".
  - `preserve` must be called while the scope is at the top of the stack; its docstring raises if it isn't.
  - `release` "releases", rather than "closes", managed resources.
- **Class example returned an undefined name.** It returned `some_ticking_table`; it now returns `ticking_table`.
- **Function example wasn't valid Python.** It had a top-level `return` and was missing the `dhnp`/`np`/`npt` imports. I wrapped it in a function and added the imports.
- **"To use the method or the class?" misstated the behavior.** It said the scope "stops managing any objects outside of the scope." It now says the scope releases what it manages on exit unless you `preserve` it.

**Groovy only**
- **Popping a scope does not release it.** The push/pop snippet's comment and the prose after it said `LivenessScopeStack.pop(scope)` releases the scope's references and makes artifacts eligible for GC. In `LivenessScopeStack.java`, `pop` only removes the scope from the stack; `PopAndReleaseOnClose` is what calls `release()`. I added `scope.release()` to the snippet and fixed the comments and prose.
- **Wrong code comment.** "managed by the scope created above" was on an anonymous-scope example; fixed.
- **Methods list was missing methods.** Added `push`, which the page already uses, and `computeEnclosed`/`computeArrayEnclosed`, which are the remaining public static methods. Added the must-be-top-of-stack rule to `pop`.

## Structure (step 2)
- **Title didn't match the category.** "How to use liveness scopes" reads like a how-to guide, so both pages are now titled "Liveness scopes". `sidebar_label` is unchanged.
- **Mechanism before use.** "How liveness scopes work" now builds the mental model the rest of the page relies on: reference count, destroy cascade, then the scope stack and the console session's scope. The misplaced "only GUI objects" paragraph at the top of "How to use..." is gone; its corrected content lives in "How it works".
- **Terms defined earlier.** "GC" and "liveness referent" are now defined at first use. "Query update graph", "update propagation graph" and "DAG" are unified as "update graph".
- **Headings.** Python "The method" → "The function". Groovy "Multiple LivenessScopes" → "Nested liveness scopes".

## Re-verification (step 3)
- I spot-checked every reworded paragraph against the sources above.
- No caveats were dropped: the try-with-resources note and the "use with/decorator only" rule are both still there.
- There are no anchor links into either page anywhere in `docs/`. The only inbound links point at the file, so the heading renames break nothing.

## Style (step 4)
- **Future tense and "we".** Removed ("we will run", "will open").
- **Hyphen as a dash.** The one hyphen used as a dash is now an em dash.
- **Empty parentheses in prose.** Removed (`LivenessScopeStack.open()` → "`open` with no arguments", link text `peek()` → `peek`, `scope.release()` → "its `release` method").
- **Backticks.** Class names in the NOTE are now in backticks.
- **Link targets.** The first `LivenessScope` mentions in the example prose now link to the reference pages (`../reference/engine/LivenessScope.md` exists for both languages) instead of mixed pydoc/javadoc URLs.
- **Related-docs link text** is now sentence case, and matches the target title where one exists ("Incremental update model").

## Couldn't resolve (needs another page or a decision)
1. **The demonstration examples use static CSV tables.** Liveness scopes do little for static tables (no listeners to remove), so the examples are correct but weak demonstrations. I'd suggest switching to a ticking source such as `time_table`; that needs an author/SME decision and a snapshot-test run.
2. **Two new `order=null` examples haven't been run.** The Python `preserve` + `release` version and the Groovy `peek().manage` + `release` version are both source-verified. They need a snapshot/CI run.
3. **Inbound link text is now stale.** Three pages still use the link text "How to use Liveness Scopes", which no longer matches the new title:
   - `docs/python/reference/engine/LivenessScope.md:50`
   - `docs/python/reference/engine/liveness-scope.md:69`
   - `docs/groovy/reference/engine/LivenessScope.md:55`
4. **`docs/python/reference/engine/liveness-scope.md` has two errors.**
   - It uses `@liveness_scope` without parentheses (line ~61). With a `contextlib.contextmanager` function this raises `TypeError` at decoration time; it should be `@liveness_scope()`.
   - Its syntax line `scope = liveness_scope()` has the same context-manager-vs-scope problem I fixed here.
5. **The source docstring in `py/server/deephaven/liveness_scope.py` has the same bugs.** It has the `@liveness_scope` decorator and `return some_ticking_table` bugs, plus the typo `scope.manange`. This is a code follow-up.
6. **Process note:** I edited the Python working copy with Write (a full-file replacement) rather than Edit; I made the Groovy changes with Edit.