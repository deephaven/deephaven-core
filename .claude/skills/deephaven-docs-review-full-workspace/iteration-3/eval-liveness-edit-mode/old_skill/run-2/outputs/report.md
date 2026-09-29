Skills used: `deephaven-docs-review-full`, which ran `ref-deephaven-doc-categories`, `deephaven-core-accuracy-check`, `deephaven-doc-structure-review`, `deephaven-core-accuracy-spot-check` and `deephaven-writing-style`.

# Liveness scopes concept guide: full review and fixes

**Category:** Concept guide (`docs/{python,groovy}/conceptual/`). Several accuracy fixes also applied to the Groovy page, so I ran all the steps on both files.

I edited both working copies:
- `/private/tmp/claude-501/-Users-margaretkennedy-deephaven-core/608a4061-7881-4a50-8095-8d9cda8cc669/scratchpad/editmode-sandbox/old_skill-r2/python-liveness-scope-concept.md`
- `/private/tmp/claude-501/-Users-margaretkennedy-deephaven-core/608a4061-7881-4a50-8095-8d9cda8cc669/scratchpad/editmode-sandbox/old_skill-r2/groovy-liveness-scope-concept.md`

I haven't run any of the examples. I checked the code against source only, and none of the snapshot tests have been re-run.

## Accuracy (step 1)

**Both pages:**
- **"Garbage collection" claims were overstated.** The pages said liveness scopes give "much more control over … garbage collection". The pydoc module docstring describes control "over when to clean up unreferenced nodes in the query update graph instead of solely relying on garbage collection". I reworded to match that.
- **Unsupported claims about the GUI.** Both pages said reference counting "will only clean up objects created purely for the GUI", and one also said cleanup happens "when objects in the GUI are not needed". Nothing in source supports either. I replaced them with what `AbstractScriptSession.evaluateScript` actually does: each command runs with the session's own scope open and not released (`LivenessScopeStack.open(queryScope, false)`, commented "we'll release them when the script session closes").
- **Other wording fixes:**
  - "updated proactively" is now "cleaned up proactively".
  - "parents' liveness count" is now a node's reference count dropping to zero. That matches `ReferenceCountedLivenessNode.onReferenceCountAtZero`, which calls `destroy` and drops the node's own references.
- **The "grouped by Sym" / "Two tables will open: `crypto` and `data`" text was wrong.** The tree is keyed by `Instrument`, and the tables that open are `crypto` and `combo_tree` (`comboTree` in Groovy). The existing Python snapshot JSON confirms this.
- **The "enclosed" example never released its scope, so it freed nothing.**
  - In Python, the scope was never released. In Groovy it was opened with `open(scope, false)`.
  - Script variables aren't managed by the session: Python globals, and in Groovy `setVariable` just writes to the binding map. So releasing the scope as the example was written would also have destroyed the tables it displays.
  - I fixed both:
    - **Python:** it now calls `scope.preserve(crypto)` and `scope.preserve(combo_tree)` inside the block, then `scope.release()`.
    - **Groovy:** it saves `outerScope = LivenessScopeStack.peek()` first, calls `outerScope.manage(...)` on the kept tables, and opens with `open(scope, true)`.
  - I checked that the tables and the tree table extend `LivenessArtifact`, and that `LivenessManager.manage` is a public default method.

**Python only:**
- **`scope_from_method = liveness_scope()` doesn't create a scope.** `liveness_scope` is a `@contextlib.contextmanager`, so that line only returns a context manager. The example now uses `with liveness_scope() as scope_from_function:`.
- **Method list:** `manage` and `unmanage` act on *this* scope (`self.j_scope`), not "the current scope". I also added that `preserve` requires the scope to be on top of the stack, because source pops `self.j_scope` and raises an error otherwise.
- **The class example returned an undefined name.** It returned `some_ticking_table`; it now returns `ticking_table`.
- **The `liveness_scope` example wasn't valid Python.** It had a `return` outside any function and was missing imports. It's now wrapped in a function with the imports, still `skip-test`. The decorator form `@liveness_scope()` (with parentheses) is correct.
- **Closing section:** "stops managing any objects outside of the scope" now says it releases what it manages on exit, except objects passed to `preserve`.

**Groovy only:**
- **`pop` does not release a scope.** `LivenessScopeStack.popInternal` only removes it from the stack. Both the code comment ("This will release the scope's references…") and the prose ("making the query artifacts … eligible for garbage collection") said otherwise. I added `scope.release()` to the push/pop example and corrected both.
- **The Methods list was incomplete.** I checked it against the public static methods of `LivenessScopeStack` and added `push` and `computeEnclosed`/`computeArrayEnclosed`.
- **Other Methods list fixes:**
  - The `open(scope, true)` entry is now described with its general `releaseOnClose` parameter.
  - The `pop` entry now says the scope must be on top of the stack.

## Structure (step 2)

- **Opening overlap:** on both pages, the intro's second paragraph repeated the first section. I merged it into "Why use a liveness scope?", and moved the sentence about reference counting into "How liveness scopes work".
- **Function-vs-class explanation repeated three times (Python).** The overlapping explanations were in "How to create", "The class" and the closing section. The comparison is now stated once as two bullets in "How to create", and the "The class" section no longer repeats it.
- **Groovy duplication:** the developer paragraph inside "How liveness scopes work" repeated the release explanation. It's now a separate, shorter paragraph that introduces the two classes.
- **Renamed headings:**
  - Python: "The method" → "The `liveness_scope` function", "The class" → "The `LivenessScope` class", "To use the method or the class?" → "Choose between the function and the class".
  - Groovy: "Multiple LivenessScopes" → "Multiple liveness scopes", "Methods" → "`LivenessScopeStack` methods".

## Re-verification (step 3)

- I checked every reworded or moved claim against the source listed above.
- **Inbound links:** I searched every doc page. Five pages link to these two, and none uses an anchor, so the heading renames break nothing.
- **Outbound links:** all internal link targets exist (`../reference/engine/LivenessScope.md`, `liveness-scope.md`, `execution-context.md`, `table-update-model.md`).

## Style (step 4)

- Replaced "we"/"let's" and future-tense "will" with present, active wording.
- Replaced a hyphen used as a dash with an em dash, and the " - " separators in the Groovy method list with " — ".
- Removed empty parentheses from method names in prose (`scope.release()`, `open()`, `peek()`).
- "The method" now says "function" when it means `liveness_scope`.
- The first mention of `LivenessScope` now links to the reference page instead of pydoc or javadoc.
- The mechanical checks for leading-dot methods, `[here]`-style link text and curly quotes find nothing.

## Couldn't resolve (needs another page or a decision)

- **Two sources show the decorator without parentheses.** `docs/python/reference/engine/liveness-scope.md` and the module docstring in `py/server/deephaven/liveness_scope.py` both show `@liveness_scope`. The function is a no-argument `contextmanager`, so it needs to be `@liveness_scope()`. The docstring also has a typo, `scope.manange`.
- **The same reference page's Syntax block** shows `scope = liveness_scope()`, which gives back a context manager rather than a scope.
- **Title:** the front matter says "How to use liveness scopes", which reads like a how-to guide for a concept page. I left it alone because changing it affects the sidebar and the link text on other pages.
- **Mixed javadoc link styles in Groovy:** the page uses both `https://deephaven.io/core/javadoc/...` and `/core/javadoc/...`. Both are common across the Groovy docs, so I left them.
- **Why the enclosed example is weak:** because the kept tree depends on `data` and `combo`, releasing the scope frees little in practice. A version where the retained result doesn't depend on the intermediates would show the benefit better; that needs an author's call.