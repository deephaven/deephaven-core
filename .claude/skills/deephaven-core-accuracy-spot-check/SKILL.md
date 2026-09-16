---
name: deephaven-core-accuracy-spot-check
description: Fast, targeted technical-accuracy check for a small addition or edit to deephaven-core (Community) documentation — one paragraph, one code snippet, one changed claim. Verifies only the changed lines against source; deliberately skips the full-file completeness sweep, cross-language duplicate-claim search, and exhaustive enumeration re-derivation that deephaven-core-accuracy-check performs. Use deephaven-core-accuracy-check instead for a new doc, a substantial rewrite, or an end-to-end PR review — this skill will under-check those.
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

3. **Apply style locally.** Don't invoke `deephaven-writing-style` as a full pass — its mechanical
   verification section is a file-wide grep sweep by nature, and running it here would mean
   checking the whole file, defeating the point of a scoped spot check. Instead, apply the
   specific prose rules that matter for a small edit directly to the changed lines only: bare
   method names in prose (no leading dot, no parentheses), descriptive link text, active voice,
   proper noun capitalization, straight quotes, em dashes. If the changed lines add a genuinely
   new backticked method reference that has an appropriate reference page or pydoc/javadoc anchor
   to link to, do the one check that rule actually requires even at this scope: confirm this isn't
   the identifier's first occurrence in the file elsewhere (a first occurrence needs a link, but
   only when a suitable target actually exists); if it might be, recommend a `deephaven-writing-style`
   pass for this one check rather than guessing — step 4's escalation goes to
   `deephaven-core-accuracy-check`, which doesn't do style/prose checks and can't resolve this
   specific uncertainty. If applying any of these style fixes changes the wording of a technical claim (not
   just its formatting or phrasing) — an active-voice rewrite can subtly change what a sentence
   asserts — re-verify the reworded claim against source before applying it, the same as step 2
   would; a style fix is not exempt from being wrong about facts.

4. **Escalate rather than chase, when the change isn't actually isolated.** If the changed claim
   also appears elsewhere in this file, in its cross-language sibling doc, or is part of an
   enumerated "N ways/paths" list, that's out of scope for a spot check — flag it and recommend
   `deephaven-core-accuracy-check` for the full file instead of trying to hunt every duplicate
   from here.

5. **Report only what was checked.** State the change, the source that confirms or refutes it (quoted
   briefly), and the verdict. No full checklist, no unrelated-section commentary.
