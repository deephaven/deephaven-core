---
name: deephaven-core-accuracy-spot-check
description: Fast, targeted technical-accuracy check for a small addition or edit to deephaven-core (Community) documentation — one paragraph, one code snippet, one changed claim. Verifies only the changed lines against source; deliberately skips the full-file completeness sweep, cross-language duplicate-claim search, and exhaustive enumeration re-derivation that deephaven-core-accuracy-check performs. Use deephaven-core-accuracy-check instead for a new doc, a substantial rewrite, or an end-to-end PR review — this skill will under-check those.
allowed-tools: Read, Grep, Glob, Edit, Bash(git diff *)
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
   that skill's step 3 for the full path list). Search source first; never correct an example from
   memory.

3. **Apply style locally.** Run the relevant `deephaven-writing-style` checks against the changed
   lines only (backticks, link wording, active voice, proper nouns) — not the file-wide mechanical
   grep sweep.

4. **Escalate rather than chase, when the change isn't actually isolated.** If the changed claim
   also appears elsewhere in this file, in its cross-language sibling doc, or is part of an
   enumerated "N ways/paths" list, that's out of scope for a spot check — flag it and recommend
   `deephaven-core-accuracy-check` for the full file instead of trying to hunt every duplicate
   from here.

5. **Report only what was checked.** State the change, the source that confirms or refutes it (quoted
   briefly), and the verdict. No full checklist, no unrelated-section commentary.
