Skills used: `deephaven-docs-review-full`, which ran `ref-deephaven-doc-categories`, `deephaven-core-accuracy-check`, `deephaven-doc-structure-review`, `deephaven-core-accuracy-spot-check` and `deephaven-writing-style`.

# Full review: liveness scopes concept guide (Python and Groovy)

**Category:** Concept guide (`docs/{python,groovy}/conceptual/`). The Groovy sibling was in scope from the start and had most of the same defects, so both files were edited. I fixed things in place and didn't restructure. Nothing was run: the edited examples are checked against source only, not executed.

## Editorial summary

The page is mostly accurate about the API. Its biggest problem is that **the scoped version of the main example never releases the scope**. It shows no benefit over the unscoped version, even though the page says the scope "can manage these objects and release them." The Groovy page adds a worse error: it says `LivenessScopeStack.pop(scope)` releases the scope's referents, and it doesn't. Both are fixed.

**Verdict: needs revision.** The page is also shaped like a how-to (its title is "How to use liveness scopes," and it has "How to create" and "How to use" sections). The sections that explain *why* repeat each other. I've listed a restructure below but didn't apply it.

## What I changed

### Accuracy (both files unless noted)

1. **The scoped example now does what its text says.**
   - The original opened a `LivenessScope` and never released it. Everything stayed managed, exactly as in the unscoped version.
   - I added `scope.preserve(crypto)`, `scope.preserve(combo_tree)` and `scope.release()` to the Python example.
   - In Groovy, I added `LivenessScopeStack.peek().manage(crypto)`, `.manage(comboTree)` and `scope.release()` after the try block.
   - Why the preserve step is needed: during script evaluation the enclosing scope is the session's query scope (`AbstractScriptSession.evaluateScript` runs `LivenessScopeStack.open(queryScope, false)`). Python's `preserve` manages the object in `_JLivenessScopeStack.peek()` after popping itself. Releasing without preserving would also release the displayed tables.
2. **The Groovy pop/release claim was wrong.** The source shows `pop` only calls `popInternal`, and releasing is a separate step (`PopAndReleaseOnClose.close()` calls `pop(scope); scope.release();`).
   - I rewrote the code comment "This will release the scope's references..." and added `scope.release()` to that syntax example.
   - I also fixed the prose that said popping makes the artifacts "eligible for garbage collection."
   - The `pop` entry in the Methods list now says the scope must be at the top of the stack and that popping doesn't release it (javadoc: "Must be the current top of the stack").
3. **The demo description was wrong.** "Grouped by Sym" is now "grouped by `Instrument`" (the query sets `Parent = Instrument`). "Two tables will open: `crypto` and `data`" is now `crypto` and `combo_tree` (`comboTree` in Groovy). `data` is set to null, and the snapshot files record `crypto` and `combo_tree`/`comboTree` as the outputs.
4. **Python's "How to create" example didn't create a scope.** `scope_from_method = liveness_scope()` returns an un-entered context manager. `liveness_scope` is a `@contextlib.contextmanager`, and the scope is created and pushed only on entry. The example now uses `with liveness_scope() as scope_from_function:`.
5. **Broken code in the Python skip-test examples:**
   - `return table` sat at module level, which is a syntax error. It's now wrapped in `def get_table():`.
   - `return some_ticking_table, scope` used an undefined name. It's now `ticking_table`.
   - I added the missing imports (`liveness_scope`, `LivenessScope`, `dhnp`, `np`, `npt`).
6. **Wrong mental model of garbage collection.**
   - "Much more control over the query update graph and over garbage collection" is now "control over when nodes in the query update graph are cleaned up." A scope controls when things are released, not when the JVM collects garbage.
   - "Updated proactively" is now "cleaned up proactively."
   - Python "control when the objects are deleted and the memory freed" now says "control when those objects are released."
7. **"When the scope is released, any objects that are no longer needed or are not refreshing are let go"** now says the scope lets go of every object it manages that nothing else still depends on. Release is not about whether a table is refreshing (`LivenessScope.release()` just decrements the reference count).
8. **Python method wording:**
   - `preserve` "keeps the object live in the next scope outside this one" (matches the source docstring).
   - `manage` works on "this scope" rather than "the current scope." The code uses `self.j_scope`, not the top of the stack.
   - The closing section now says the function-created scope releases objects when the block or decorated function exits, except those passed to `preserve`. That matches `liveness_scope()`'s `finally: _pop(...); scope._release()`. The old text said it "stops managing any objects outside of the scope."
9. **Groovy code comment:** "managed by the scope created above" is now "managed by an anonymous scope." No scope is created above in that snippet.
10. **Groovy Methods list completeness:** I added `LivenessScopeStack.push(scope)`. The page uses it but didn't list it. The link anchor matches the signature `push(LivenessManager)`.

### Terminology and style

- **Python "method" is now "function"** for `liveness_scope`, including the headings "The function" and "To use the function or the class?". No page links to those anchors: I searched every `.md` file under `docs/` and both `sidebar.json` files, and the only inbound links point at the page itself.
- **Future-tense "will" changed to present** in about 10 sentences across both files.
- I replaced the hyphen in "stack - that is" with an em dash (Groovy).
- I changed "useful enclosing" to "useful for enclosing" (Groovy).
- I changed `f"A={a}"` to `f"A = {a}"` (Python).

## Couldn't resolve (author queries)

- **AQ1** [both, "How to use", para 1]: "Deephaven's reference counting instrumentation will only clean up objects created purely for the GUI." I couldn't find source for "only... purely for the GUI." Please confirm or rewrite.
- **AQ2** [both, "How liveness scopes work", para 1]: "allow cleanup to happen immediately when objects in the GUI are not needed... works automatically for all users." It's unclear what is automatic and what the GUI has to do with it. An SME should confirm.
- **AQ3** [both, intro]: "A node in the update graph can be a table, plot, or any other object." Only `LivenessReferent`s are managed, so "any other object" overstates it. Are plots actually referents?
- **AQ4** [both, "Demonstrating the problem"]: "This is best practice because it allows Deephaven to conserve memory." Is memory the benefit worth stating, or is it stopping unneeded tables from updating?

## Recommended, not applied (these would be rewrites)

- **Retitle and reshape.** "How to use liveness scopes" and the "How to create" / "How to use" sections are how-to framing on a concept guide. Consider a concept title and lead with the mental model (scopes, the stack, manage/preserve/release).
- **Merge the overlapping explanations.** The intro paragraph 2, "Why use a liveness scope?" and "How liveness scopes work" each re-explain reference counting versus garbage collection. "Why" says "there are cases" without naming one; the demo that names it comes later. Consider folding the demo's motivation into "Why."
- **Move the function-versus-class comparison earlier (Python).** "How to create" already contrasts the function and the class, the subsections repeat it, and "To use the function or the class?" repeats it again at the end. Put one side-by-side comparison where both are first named.
- **Merge the Groovy "Methods" list into the sections that introduce each method.** It repeats `open`, `pop` and `push`, which were explained earlier.
- **Use a ticking example.** The demo reads a static CSV, while the page says liveness matters for refreshing graphs.
- **Replace the placeholders.** The skip-test Python examples still use `some_ticking_source`, `other_ticking_table` and `key_cols`, so the snapshotter doesn't test them. Consider runnable versions, for example with `time_table`.
- **Minor, optional:**
  - The Groovy methods list uses " - " separators.
  - Link styles are mixed (absolute `https://docs.deephaven.io/...` and `https://deephaven.io/...` alongside root-relative `/core/...`).
  - `LivenessScopeStack.computeEnclosed` is not mentioned.

## Other pages (not edited, out of scope)

- `docs/python/reference/engine/liveness-scope.md`: its decorator example uses `@liveness_scope` without parentheses. For a `contextlib.contextmanager` that doesn't open a scope; this page's `@liveness_scope()` is the correct form. Its Syntax block `scope = liveness_scope()` has the same problem as fix 4.
- The pydoc module docstring in `py/server/deephaven/liveness_scope.py` also shows `@liveness_scope` without parentheses, and misspells `scope.manange`.

## Strengths to keep

- Showing the query first without a scope and then with one is the right teaching shape. It works now that the scope is actually released.
- Python's explicit `open`/`release` "class-only" markers.
- Groovy's progression from push/pop, to try-with-resources, to anonymous scopes, to nesting.

**Files edited:**
- `/private/tmp/claude-501/-Users-margaretkennedy-deephaven-core/608a4061-7881-4a50-8095-8d9cda8cc669/scratchpad/editmode-sandbox/i5-with_skill-r2/python-liveness-scope-concept.md`
- `/private/tmp/claude-501/-Users-margaretkennedy-deephaven-core/608a4061-7881-4a50-8095-8d9cda8cc669/scratchpad/editmode-sandbox/i5-with_skill-r2/groovy-liveness-scope-concept.md`