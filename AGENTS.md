# Agent Instructions

## Verification requirement

**Before applying ANY fix or suggestion — from Copilot, user feedback, or your own analysis — you MUST:**

1. **Identify the authoritative source** for each claim (reference docs, source code, implementation)
2. **Read that source** and quote the relevant text in your response
3. **Show verification evidence** before making changes
4. **Verify link targets exist** before adding any markdown link (use glob, find, or equivalent path-discovery tools)

**Do NOT apply fixes without showing the reference that confirms them.**

### Why this matters

Copilot and other tools can be wrong. You have been wrong. Blindly applying suggestions without verification has introduced incorrect information into documentation. The cost of verification is small; the cost of incorrect documentation is high.

### Examples of required verification

**Wrong (no verification):**
> Copilot says `k` has the same restrictions as `i`/`ii`. Fixing now...

**Right (with verification):**
> Copilot says `k` has the same restrictions as `i`/`ii`. Let me verify against `reference/query-language/variables/special-variables.md`:
> 
> From the reference doc:
> | Variable | Safe on | Throws on |
> |----------|---------|-----------|
> | `i`, `ii` | static, append-only, blink | add-only, ticking |
> | `k` | static, add-only, blink | append-only, ticking |
> 
> **Copilot was wrong.** They have different restrictions. Applying the correct fix...

### Link verification

Before adding any link like `[text](../path/to/file.md)`, use your available path-discovery tools (glob, find, grep, etc.) to confirm the target file exists.
