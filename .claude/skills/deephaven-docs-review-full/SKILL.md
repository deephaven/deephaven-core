---
name: deephaven-docs-review-full
description: >
  Run a complete deephaven-core (Community) documentation review — technical accuracy, structural organization, and prose style — in one pass, in the order that keeps one dimension from silently undoing another. **Use this skill when:** someone says "full review," "review this doc," "is this ready for production," "review before merge," "check this PR," "comprehensive review," "I wrote a new doc," "I rewrote this doc," or wants all three dimensions (accuracy + structure + style) checked together. Also use for new docs, substantially rewritten docs, or before merging a doc PR. **Do NOT use for:** single small edits (use deephaven-core-accuracy-spot-check instead), Enterprise/deephaven-ent docs, or when only one dimension is requested.
allowed-tools: Read, Grep, Glob, Edit, Skill, Bash(git diff *), Bash(awk *)
---

# Full deephaven-core documentation review

This skill doesn't duplicate any checklist — it sequences the three existing review skills so a
full pass always covers all three dimensions, and so a later step's edits get re-checked against
the earlier steps rather than assumed still valid.

**Report by default; edit only if asked.** Like `deephaven-doc-structure-review`, this skill
produces one consolidated report unless the user has explicitly asked for fixes to be applied.
"Review this doc" or "run a full review" means report only — nothing below should be read as
license to rewrite the document on its own. If the user does ask for fixes, apply them in the
order below (accuracy, then structure, then style), since that's the order that keeps one
dimension from undoing another.

## 0. Identify the doc's category

Read `ref-deephaven-doc-categories` and determine which of the four categories this doc is. Carry
that forward — the accuracy, structure, and style skills below all calibrate to it. Not every doc
fits one of the four: check that file's "Pages outside the four categories" section first (the
site's `intro.md` landing page and contributor-facing tooling docs such as `docs/README.md`) —
don't assume the doc or the categorization is broken just because it doesn't fit. For an
out-of-taxonomy page, skip category-specific calibration in every step below (1, 2, and 4) but
still run all three checks; the generic accuracy, structural, and prose-quality rules that aren't
category-conditional still apply.

## 1. Accuracy first

Invoke `deephaven-core-accuracy-check` on the doc. Facts before reorganizing: there's no point
building a clean structure around a wrong claim, and it's easier to verify claims against source
while they're still in their original location and context. Per the report-by-default rule above,
this step reports issues; only apply the fixes it finds if the user asked for edits.

If a fix is applied and it corrects a shared, substantive claim in the cross-language sibling too
(`deephaven-core-accuracy-check`'s own cross-language-consistency check may have already edited
both files) — track that sibling as a second doc in scope. Run steps 2 **through 4** on it as
well (not just 2 and 4 — a structural edit to the sibling needs the step-3 re-verification just as
much as the originally-requested file does), not just the originally-requested file; a sibling
edited by the accuracy pass but never structurally or style-reviewed is exactly the kind of
half-finished pass this skill exists to prevent.

## 2. Structure second

Invoke `deephaven-doc-structure-review` as the middle step of this orchestrator — its own
instructions know to skip its standalone full accuracy/style re-run in that case and defer to
this workflow's steps 3-4 instead, so don't expect or trigger that separately here. This step may
move, merge, cut, reorder, or rename sections that were just verified in step 1 — that's expected
and fine, but it's exactly why step 3 exists.
Note everywhere content was moved, merged, cut, reordered, renamed, **or reworded in place**
(rewritten without changing location) — step 3 needs the complete list, since a rewrite that
changes a claim without moving its section would otherwise never reach the spot-check, and a
deleted caveat or a renamed-away section can invalidate an accuracy finding just as easily as a
literal move can.

## 3. Re-verify what structure touched

For every section from step 2's list, handle it by what happened to it:

- **Moved, merged, or reworded** (surviving prose): re-run `deephaven-core-accuracy-spot-check`
  on it as the middle step of this orchestrator — its own instructions know to skip its local
  style step in that case and defer to this workflow's step 4 instead, so don't expect or trigger
  that separately here. Run it per its own scope (one paragraph, one snippet, one changed claim),
  not as a single call covering the whole section. If the section contains more than one claim or paragraph, call
  it once per claim/paragraph rather than handing it the whole section at once; a merge can
  combine two previously-separate claims into one that's subtly wrong even though both originals
  were correct individually, and a single oversized spot-check call is exactly the under-checking
  that scope exists to prevent. Escalate to a full `deephaven-core-accuracy-check` re-pass
  whenever any of those spot checks recommends escalating (its own criteria: the claim also
  appears elsewhere in the file, in the cross-language sibling, or is part of an enumerated list)
  — don't restate or narrow that criteria here; defer to the spot check's judgment.
- **Cut** (no surviving text): there's nothing left to hand the spot check — don't force this
  through the bullet above. Instead check whether the cut section contained a caveat, exception,
  or claim that existed *only* there and is now gone entirely from the doc; that's the next
  bullet's job, not a spot-check call.
- **Any caveat, exception, or cross-language distinction that was near content step 2 touched**
  (moved, merged, cut, or renamed): check the rest of the document first for where it may have
  landed or been restated, and flag it as dropped only if you can't find it after that check. If
  you're still not sure after checking, say so explicitly ("possibly dropped, unconfirmed — verify
  against the pre-edit version") rather than stating it as a confirmed finding — a false "this was
  dropped" claim costs a reviewer real time chasing content that's actually still there.
- **Links and anchors**: neither the spot check nor step 4's style pass validates that a link's
  *target* still resolves or that a heading's *anchor fragment* is still correct after an edit —
  `deephaven-writing-style` checks link wording and first-mention linking, but not target/anchor
  resolution. A heading rename, section move, merge, or cut can silently break an internal link
  or an anchor fragment (`#some-heading`) even when every claim in the doc remains correct. For
  any section step 2 moved, renamed, merged, or cut (not just moved/renamed): re-check that links
  *within* it still resolve from wherever it ended up (or, if cut, that nothing else in the doc
  still assumes it exists), and search **all** doc pages — not just this doc and its
  cross-language sibling — for links pointing *to* it, since any page in the corpus can link to
  any other (e.g. `conceptual/query-table-configuration.md` links to
  `query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where`, a
  completely unrelated file). This corpus-wide inbound-link scan is mandatory and has no
  substitute — `deephaven-core-accuracy-check`'s own internal-link step only checks links inside
  the document being reviewed, not other pages' inbound links to it, so escalating to it does not
  cover this. If more than a couple of links or anchors were affected, run the corpus-wide scan
  *and* escalate to a full `deephaven-core-accuracy-check` re-pass for the document's own
  within-doc links — the two checks are complementary, not alternatives.

Do not skip this step under time pressure. It's the step that catches the compounding defect a
structural edit introduces into content nobody re-reads afterward.

## 4. Style last

Invoke `deephaven-writing-style` over the whole doc (both files, if step 1 put a cross-language
sibling in scope). Run it last because both the accuracy fixes (step 1) and the structural moves
(steps 2-3) introduce or relocate prose that hasn't had a dedicated style pass yet — running style
first would mean re-doing it. Per the report-by-default rule above, this step reports style
issues; if the user asked for edits to be applied here too, prefer a fix that only changes
formatting, wording style, or phrasing — if a style fix would also change what a sentence
technically claims (not just how it's worded), re-verify that reworded claim against source before
applying it, the same way step 1 would have. A style pass is not exempt from being wrong about
facts just because it isn't the accuracy step.

## 5. Report

One consolidated list, grouped by dimension (Accuracy / Structure / Style), each finding citing
which step surfaced it and its location in the doc. Note the doc's category from step 0 at the
top of the report so a reviewer can sanity-check severity calls (e.g. an orphaned aside flagged
harder because the doc is a Reference guide). If a cross-language sibling was pulled into scope by
step 1, report on both files, not just the one the user originally pointed at.
