---
name: deephaven-core-accuracy-spot-check
description: >
  Quick, scoped accuracy check for one isolated deephaven-core (Community) doc edit — one code snippet, one changed sentence, one paragraph addition, one claim. **Use this skill when:** someone says "spot-check," "quick check," "verify just this one," "is this code correct," "check if this parameter/method name is right," or asks about a single isolated change. This skill verifies the changed lines against source, plus the claims in the sentences and clauses directly around them, and does not review the rest of the page. **Do NOT use for:** a change touching multiple sections or several independent claims, full file reviews (use deephaven-core-accuracy-check), new docs, substantial rewrites, style/formatting issues (use deephaven-writing-style), reorganization (use deephaven-doc-structure-review), or Enterprise/deephaven-ent docs.
allowed-tools: Read, Grep, Glob, Edit, Skill, Bash(git diff *)
---

> [!IMPORTANT]
> **Verification is mandatory, even for a one-line check.** Identify the authoritative source,
> read it, quote the relevant text, and only then say whether the claim is accurate. Don't accept
> a claim at face value because the surrounding doc already reads as trustworthy.

1. **Scope the change.** Use `git diff` (or the specific snippet/paragraph the user points to) —
   don't re-check the whole file. If you can't tell what changed from the input given, ask.

2. **Verify each changed claim or code snippet against source.** Use the same source map as
   `deephaven-core-accuracy-check` (engine/server code, `py/server/deephaven/`, configuration
   properties under `Configuration/` and `props/`, gRPC definitions under `proto/`, etc. — see
   that skill's "Technical accuracy review" step for the full path list). Search source first;
   never correct an example from memory.

   **Verify the whole sentence, not just the changed words.** If the edit rewords one clause of a
   sentence or one sentence of a paragraph, check the claims in the clauses around it too. They are
   not in the diff, but a reader takes the sentence as one claim, and the unchanged clause is often
   the wrong one. Checking that a neighbor still agrees with your edit is not enough. List each
   claim the neighboring sentences make and verify it against source on its own, the same way you
   verify the changed claim, and report any that source does not support.

   **Placement gate:** a verified-true fix can still be the wrong fix. This applies only to
   narrative pages: Concept guides (`conceptual/`) and Tutorials (`getting-started/crash-course/`).
   On Reference pages and configuration pages (such as `conceptual/query-table-configuration.md`),
   the precise property, default, or threshold is the content, so fix it inline. On a narrative
   page, if correcting the claim would add a property name, default, or threshold to the
   narrative, don't paste it inline — propose rewriting the sentence at the section's level of
   abstraction and putting the precise detail in the page's Configuration section or a link to the
   configuration reference (see `deephaven-core-accuracy-check`'s **Placement of configuration
   detail**). A caveat that isn't configuration detail (a version, environment, or platform restriction) stays if the reader needs it at that point, as its own sentence (see `deephaven-writing-style`'s hedging rule); otherwise cut it. If you can't verify the claim, raise an author query rather than
   hedging the sentence.

3. **Apply basic style to changed lines only.**
   
   > Skip this step entirely if invoked from `deephaven-docs-review-full` — that orchestrator runs a full `deephaven-writing-style` pass afterward.
   
   For standalone spot-checks, apply these rules to the changed lines only (don't scan the whole file):
   - Method names in prose: no leading dot, no parentheses (`update`, not `.update()`)
   - Active voice preferred
   - Straight quotes only (`"`, not `“` or `”`)
   - Em dashes with spaces (` — `)
   - Proper noun capitalization (Deephaven, RowSet, ColumnSource, etc.)
   
   **If a style fix changes what a sentence claims** (not just formatting), re-verify the reworded claim against source before applying it.

4. **Quick duplicate check — escalate if needed.**
   
   Before finishing, do a quick check for duplicate claims:
   - Does this claim appear elsewhere in this file?
   - Does a cross-language sibling exist (`docs/groovy/...` ↔ `docs/python/...`)? If so, does it repeat the claim?
   - Is this part of an enumerated "N ways/paths" list?
   
   **If duplicates exist:** Flag them and recommend `deephaven-core-accuracy-check` for the full file.
   
   **If no duplicates found:** Report that you checked and the change is isolated — no escalation needed.

5. **Report only what was checked.** State the change, the source that confirms or refutes it (quoted
   briefly), and the verdict. No full checklist, no unrelated-section commentary.
