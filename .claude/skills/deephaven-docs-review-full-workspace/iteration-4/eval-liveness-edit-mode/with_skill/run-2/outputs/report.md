**Skill used:** `deephaven-docs-review-full`. It runs `ref-deephaven-doc-categories`, `deephaven-core-accuracy-check`, `deephaven-doc-structure-review`, `deephaven-core-accuracy-spot-check` and `deephaven-writing-style` in turn.

# Full review: Liveness scope concept guide (Python + Groovy)

**Category:** Concept guide (`docs/{python,groovy}/conceptual/`). The Groovy sibling was brought into scope because it shares the same wrong claims.

## Editorial summary
Both pages had real accuracy errors, and I fixed them in place. The most serious: the Groovy page said `LivenessScopeStack.pop` releases a scope, but it only pops it. Several descriptions were also wrong, along with two broken code snippets. The bigger issue is one I couldn't fix with small edits. The "Demonstrating the problem" examples use static CSV tables and never release the scope, so they don't show the problem or the fix. **Verdict: needs revision.** I made targeted fixes and left the restructuring below as recommendations, not rewrites.

## What I changed

### Both pages
- **"How liveness scopes work", rewritten to match the source.**
  - The old text said nodes are "updated proactively", mixed up the engine's automatic reference counting with the scopes users create, and gave a muddled parent/child model.
  - The new text: when a node's reference count reaches zero, the engine destroys it (it stops updating right away) and GC frees the memory later. A scope holds a reference to each node created while it is open. A refreshing child holds its parents, so parents are released in turn.
  - Checked against `ReferenceCountedLivenessReferent.onReferenceCountAtZero` → `destroy()`, `BaseTable` listener `destroy()` → `removeUpdateListener`, `LivenessArtifact.manageWithCurrentScope`, and `BaseTable.addParentReference` (which manages the parent only when `notDynamicOrIsRefreshing`).
- **"Why use a liveness scope?"**
  - Old: scopes give "control over … garbage collection". New: control over when nodes leave the update graph.
  - Also fixed "Without liveness scope" and replaced the undefined "DAG" with "update graph".
- **First demo text**
  - "grouped by Sym" is now "grouped by `Instrument`".
  - "Two tables will open: `crypto` and `data`" is now `crypto` and `combo_tree` (Groovy: `comboTree`). The docs snapshot file confirms these are the two outputs.
- **Dropped "This is best practice because it allows Deephaven to conserve memory".** Memory is still reclaimed by GC. The replacement says the engine can clean the objects up without waiting for GC.
- **Added one sentence after the scoped example** saying the scope holds the tables until it is released. Neither example releases its scope, so without this the reader is misled.
- **Style:** removed future-tense "will", and "how to use … once created" now uses present tense.

### Python only
- **"How to create" example.** `scope_from_method = liveness_scope()` doesn't create or open a scope. `liveness_scope` is a `@contextlib.contextmanager`, so calling it only returns a context manager, and the scope is pushed on `__enter__`. I changed the example to `with liveness_scope() as scope_from_function:`.
- **The function's `skip-test` example** had a `return table` outside any function. I wrapped it in `def get_joined_table():`.
- **The class's `skip-test` example** returned `some_ticking_table`, which is never defined. It now returns `ticking_table`. I also changed `f"A={a}"` to `f"A = {a}"`.
- **Methods list**
  - `manage` and `unmanage` act on *this* scope, not the "current" one.
  - `preserve` now says it hands the object to the next scope out and must be called while the scope is open. The source pops itself first, which requires the scope to be on top of the stack.
  - `release` now "releases the scope's references to all the objects it manages". The old wording, "closes … all of its managed resources", overstated it (Java `LivenessScope.release` javadoc).
- **"The method" section**
  - It now says the scope releases what it manages when the block exits or the decorated function returns. This is from the `finally: _pop(...); scope._release()` code in `liveness_scope`.
  - The closing section's vague "stops managing any objects outside of the scope" is now "releases them when the scope closes, except objects passed to `preserve`".
- **Headings.** `liveness_scope` is a module-level function, and the page's own intro already calls it one.
  - "The method" is now "The `liveness_scope` function".
  - "The class" is now "The `LivenessScope` class".
  - "To use the method or the class?" is now "To use the function or the class?"
  - The corpus-wide inbound scan found no links to anchors on this page. The only inbound links are bare page links from both `formula-threads.md` files, `python/reference/engine/liveness-scope.md`, and both `LivenessScope.md` reference pages.
- **Links.** Changed the `https://docs.deephaven.io/core/pydoc/...` `LivenessScope` links to the reference page `../reference/engine/LivenessScope.md`, which exists. The corpus mostly uses root-relative pydoc links (756 vs 88).

### Groovy only
- **Fixed the `pop` error in two places.** In `LivenessScopeStack.popInternal`, pop only removes the scope from the stack.
  - The code comment said pop "will release the scope's references". It now says pop doesn't release it, and the snippet adds `scope.release()`.
  - The prose said popping made artifacts "eligible for garbage collection". It now says you pop, then call `release`.
- **Methods list**
  - Added the missing `push(scope)` entry (the page uses it).
  - The `pop` entry now says the scope must be on top of the stack and that popping doesn't release it.
  - Fixed "useful enclosing" to "useful for enclosing".
  - Replaced the hyphen separators with em dashes.
  - Wrapped `SafeCloseable` in backticks.
  - Changed `peek()` to `peek`. I kept `open()` because it tells the two overloads apart.
- **Anonymous-scope snippet:** the comment "managed by the scope created above" is now "managed by an anonymous scope".
- **Other small fixes**
  - Hyphen-as-dash changed to an em dash in "Multiple LivenessScopes".
  - "This automatically opens" the scope is now "makes it the current scope".
  - The long first paragraph of "How liveness scopes work" is split in two.

## What I couldn't resolve (recommendations and author queries)

### Examples: highest impact, not edited
Replacing examples is beyond "fix, not rewrite", and I can't run new code.
- **The "Demonstrating the problem" examples, in both languages, demonstrate neither the problem nor the fix.**
  - `read_csv`/`readCsv` output is static. Static parents aren't managed by their children (`addParentReference` skips non-refreshing parents), and nothing ticks, so a scope makes no visible difference.
  - The scoped versions never release the scope, and Groovy passes `open(scope, false)`.
  - Suggested fix: use a ticking source (for example `function_generated_table`, as the reference pages do). Preserve the tables you keep (`crypto`, `combo_tree`), then release the scope so the intermediate tables stop ticking.
- **Python function/class examples use placeholders** (`some_ticking_source`, `other_ticking_table`, `key_cols`, `npt`, `dhnp`) under `skip-test`, so they are never tested. A runnable version would be better.

### Structure: recommendations only
- **The page is titled "How to use liveness scopes"** (how-to phrasing) but is a concept guide. Consider retitling.
- **Intro paragraph 2 overlaps "How liveness scopes work".**
- **"Why use a liveness scope?" says "there are cases" but never names them.** State the key message plainly and early. For example: objects your console code creates stay managed by the session's scope (`AbstractScriptSession.evaluateScript` opens `queryScope`). To stop intermediate tables ticking early, create them in your own scope, preserve what you keep, and release the rest.
- **Suggested order:** how it works → create → use (function/class, preserve/release) → a worked example. At the moment the example comes before the release mechanism is explained.
- **Groovy's "Methods" section repeats the earlier sections.** Consider merging it into them, or making it a short reference table.

### Author queries
- **AQ1** [How to use a liveness scope, para 1, both pages]: "reference counting instrumentation only cleans up objects created purely for the GUI". Is this accurate? Script-created objects are kept by the session query scope until the session closes. I couldn't confirm the "only … GUI" framing, and "or are not refreshing are let go" is unclear.
- **AQ2** [Intro, both]: "A node … can be a table, plot, or any other object". Are plots or figures liveness referents? "Any other object" overstates it.

### Other pages (not edited, per the rules)
- **`docs/python/reference/engine/liveness-scope.md`**
  - The decorator example uses `@liveness_scope` without parentheses. With a `contextmanager` that raises a `TypeError` at decoration time; it needs `@liveness_scope()`.
  - Its Syntax and Returns sections present `scope = liveness_scope()` as returning a `SimpleLivenessScope`. It actually returns a context manager that *yields* one.

## Strengths
- The Python page explains the function-vs-class distinction clearly.
- The Groovy page covers try-with-resources and nested scopes well.

Edited files:
- /private/tmp/claude-501/-Users-margaretkennedy-deephaven-core/608a4061-7881-4a50-8095-8d9cda8cc669/scratchpad/editmode-sandbox/i4-with_skill-r2/python-liveness-scope-concept.md
- /private/tmp/claude-501/-Users-margaretkennedy-deephaven-core/608a4061-7881-4a50-8095-8d9cda8cc669/scratchpad/editmode-sandbox/i4-with_skill-r2/groovy-liveness-scope-concept.md