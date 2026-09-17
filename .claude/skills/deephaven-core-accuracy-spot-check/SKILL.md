---
name: deephaven-core-accuracy-spot-check
description: >
  Quick, scoped accuracy check for a small deephaven-core (Community) doc edit — one code snippet, one changed sentence, one paragraph addition. **Use this skill when:** someone says "spot-check," "quick check," "verify just this one," "is this code correct," "check if this parameter/method name is right," "small edit," "small fix," "one line change," "one paragraph," asks about an isolated change, or mentions reviewing a PR diff where only a few lines changed. This skill verifies ONLY the changed lines against source. **Do NOT use for:** full file reviews (use deephaven-core-accuracy-check), new docs, substantial rewrites, style/formatting issues (use deephaven-writing-style), reorganization (use deephaven-doc-structure-review), or Enterprise/deephaven-ent docs.
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
   that skill's step 4 ("Technical accuracy review") for the full path list — use the heading, not
   the step number, since renumbering there has already gone stale once). Search source first;
   never correct an example from memory.

3. **Apply basic style to changed lines only.**
   
   > Skip this step entirely if invoked from `deephaven-docs-review-full` — that orchestrator runs a full `deephaven-writing-style` pass afterward.
   
   For standalone spot-checks, apply these rules to the changed lines only (don't scan the whole file):
   - Method names in prose: no leading dot, no parentheses (`update`, not `.update()`)
   - Active voice preferred
   - Straight quotes only (`"`, not `"`)
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
