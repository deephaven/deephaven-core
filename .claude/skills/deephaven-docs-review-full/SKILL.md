---
name: deephaven-docs-review-full
description: Run a complete deephaven-core (Community) documentation review — technical accuracy, structural organization, and prose style — in one pass, in the order that keeps one dimension from silently undoing another. Use this for a new doc, a substantially rewritten doc, or before merging a doc PR, instead of remembering to invoke deephaven-core-accuracy-check, deephaven-doc-structure-review, and deephaven-writing-style separately and in the right order. For a single small edit, use deephaven-core-accuracy-spot-check instead — this skill is overkill for a one-paragraph change.
allowed-tools: Read, Grep, Glob, Edit, Bash(git diff *), Bash(awk *), Bash(git log *)
---

# Full deephaven-core documentation review

This skill doesn't duplicate any checklist — it sequences the three existing review skills so a
full pass always covers all three dimensions, and so a later step's edits get re-checked against
the earlier steps rather than assumed still valid.

## 0. Identify the doc's category

Read `ref-deephaven-doc-categories` and determine which of the four categories this doc is. Carry
that forward — the accuracy and structure skills below both calibrate to it.

## 1. Accuracy first

Invoke `deephaven-core-accuracy-check` on the doc. Fix facts before reorganizing: there's no
point building a clean structure around a wrong claim, and it's easier to verify claims against
source while they're still in their original location and context.

## 2. Structure second

Invoke `deephaven-doc-structure-review`. This may move, merge, or cut prose that was just
verified in step 1 — that's expected and fine, but it's exactly why step 3 exists.

## 3. Re-verify what structure touched

For every section the structure pass moved, merged, or rewrote a transition around:

- Re-run `deephaven-core-accuracy-spot-check` on that section only — a merge can combine two
  previously-separate claims into one that's subtly wrong even though both originals were correct
  individually. Escalate to a full `deephaven-core-accuracy-check` re-pass only if the merge
  touched an enumerated list or a claim repeated elsewhere in the file.
- Note anywhere a caveat, exception, or cross-language distinction looks like it got dropped in
  the move — flag it even if you can't immediately tell whether it survived elsewhere.

Do not skip this step under time pressure. It's the step that catches the compounding defect a
structural edit introduces into content nobody re-reads afterward.

## 4. Style last

Invoke `deephaven-writing-style` over the whole doc. Run it last because both the accuracy fixes
(step 1) and the structural moves (steps 2-3) introduce or relocate prose that hasn't had a
dedicated style pass yet — running style first would mean re-doing it.

## 5. Report

One consolidated list, grouped by dimension (Accuracy / Structure / Style), each finding citing
which step surfaced it and its location in the doc. Note the doc's category from step 0 at the
top of the report so a reviewer can sanity-check severity calls (e.g. an orphaned aside flagged
harder because the doc is a Reference guide).
