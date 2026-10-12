---
name: deephaven-docs-review-full
description: >
  Run a complete deephaven-core (Community) documentation review the way a technical-book editor would: a developmental pass (purpose, audience, scope), then accuracy, structure, examples, and style, in an order that keeps one from undoing another, ending in a prioritized editorial report with author queries. **Use this skill when:** someone asks for a "full review," "comprehensive review," or "editorial review" of a doc, asks to "edit this like an O'Reilly editor," asks if a doc "is ready for production," or wants accuracy, structure, and style checked together, for a new doc, a substantially rewritten doc, or a doc PR that touches more than a small, isolated edit. **Do NOT use for:** a single small edit such as a one-line fix or one snippet (use deephaven-core-accuracy-spot-check), working through existing PR or Copilot review comments (use deephaven-docs-address-review-comments), non-documentation code review, Enterprise/deephaven-ent docs, or when only one dimension is requested.
allowed-tools: Read, Grep, Glob, Edit, Skill, Bash(git diff *), Bash(awk *)
---

# Full deephaven-core documentation review

This skill sequences the existing review skills so a full pass always covers every dimension,
and so a later step's edits get re-checked against the earlier steps rather than assumed still
valid. It owns only what no other skill does: the developmental pass, the examples pass, and the
prioritized report. Steps are referred to by name elsewhere (e.g. "the re-verify step"), not by
number, so renumbering here doesn't strand cross-references.

The ordering principle, borrowed from technical-book editing: **a structural problem outranks a
comma.** Work from the biggest questions to the smallest, and weight the report the same way. A
page that doesn't tell the reader what they'll be able to do gains little from perfect backticks.

**Report by default; edit only if asked.** Like `deephaven-doc-structure-review`, this skill
produces one consolidated report unless the user has explicitly asked for fixes to be applied.
"Review this doc" or "run a full review" means report only — nothing below should be read as
license to rewrite the document on its own. If the user does ask for fixes, apply them in the
order below (accuracy, then structure, then examples, then style), since that's the order that
keeps one dimension from undoing another. The developmental pass never edits.

**"Apply the fixes" means fix, not rewrite.** When edits are requested, correct each finding in
place with the smallest change that makes the page right, and keep the page's existing outline,
examples, and title. Findings that call for more than that are recommendations, not edits: report
them and let the author decide. That includes a "needs restructuring" verdict, reordering or
merging sections, replacing an example with a new one, and retitling the page. Two reasons: an
author reviewing a PR can check a targeted fix but not a rewrite, and new or replaced examples
can't be verified without running them. Do the full restructure only if the user explicitly
asks for one ("restructure," "rewrite," "reorganize this page").

The line is between *repairing* what the page has and *replacing* it:

- **Targeted — apply these:** fixing broken code inside an existing example (a syntax error, an
  undefined name, a missing import, a wrong method call);
  adding the one or two lines an existing example needs to do what its own text says (for example, a
  missing cleanup or close call); correcting a wrong sentence, heading term, or code comment;
  deleting a duplicated sentence; moving one misplaced paragraph. A reviewer can check each of
  these against the original in a glance.
- **Rewrite — recommend, don't apply:** replacing an example with a different scenario or data
  source, adding a new example section, reordering or merging sections, retitling, or rewriting
  a section wholesale.

When a fix is targeted, don't defer it just because the example it touches has bigger problems
too; make the repair, and put the bigger change in the report.

**Audit and overhaul fixes are the explicit exception.** When the task is to fix a page from a
docs audit (for example a DOC-1560 page issue) or to overhaul a page, the request covers the whole
page, and the fix-not-rewrite limit above does not apply:

- For audit-driven work, start from the complete list of recorded findings for the page (a standalone
  overhaul of an unaudited page starts from the whole-page review instead). This skill can't read Jira, so
  the caller supplies the list: the page issue's checklist plus any readability or "Found during"
  comments and linked readability issues. If you weren't given it, ask for it before editing; a
  fresh review is not a substitute, because it won't reproduce every recorded minor and readability
  finding.
- Apply every recorded finding, of every severity: wrong, misleading, hard to follow, minor, and
  readability. Verify each finding first, as for any other claim: an audit can be stale or wrong,
  so record a disproved finding as not a problem, with the source, rather than applying it. Don't fix one severity now and leave the rest for a later pass; a page that is
  patched in one place and wrong two paragraphs down still sends readers the wrong way, and every
  later reviewer re-finds the open items.
- Restructure, replace an example, or rewrite a section where the findings cluster or the
  section can't be made clear by patching. Keep every accurate fact. This skill doesn't run
  examples: list each new or changed example as needing a snapshot run (`docs/updateSnapshots`), or
  run it yourself if the caller's environment allows, and fix any failure.
- Keep what is accurate. Full scope means fixing everything that is wrong, unclear, or badly
  organized, not rewording everything. Leave correct sentences, simple lists, accurate bullets and
  notes, needed vocabulary, correct code comments, and TODOs for work you didn't do as the author
  wrote them. Every change should trace to a finding or a verified problem, so a reviewer can see
  why it was made. Don't rename variables, tables, or headings, or reword a correct note into a
  different form, unless a finding calls for it; in the report, give the reason for every change you
  list.
- Keep the terms readers search for. A word the API, its parameters, or its developers use (for
  example "include" for the columns a join adds)
  is how readers find the page and match it to the code. Define such a term where it first
  appears rather than replacing it with a plainer synonym. A term the page itself defines (often in
  italics) stays word for word, even when you correct its
  definition.
- Edit only the pages in scope. When a wrong claim lives on another page, in a reference page
  that a different PR or ticket owns, or in source code such as a docstring, don't edit it there:
  other work may be in flight on that page, and source changes need their own review. List it as
  labeled follow-up work with the file, the wrong text, and the fix.
- Treat the recorded findings as a floor, not the scope. The audit missed things; fix what you
  find while working through the page, in the sibling language's page too unless the caller
  limited the task to one language or file.
- When the edits are done, review the finished page, not the diff: run the accuracy, structure,
  examples, and style steps again over the whole page, fix what they find, and repeat until a round finds
  nothing wrong, misleading, or hard to follow. Apply the minor and style findings from that last
  round too, without starting another round. Then do one adversarial read, assuming problems
  remain, before handing the page to a human or a bot reviewer.

## 0. Identify the doc's category

Read `ref-deephaven-doc-categories` and determine which of the four categories this doc is. Carry
that forward — the accuracy, structure, and style skills below all calibrate to it. Not every doc
fits one of the four: check that file's "Pages outside the four categories" section first (the
site's `intro.md` landing page and contributor-facing tooling docs such as `docs/README.md`) —
don't assume the doc or the categorization is broken just because it doesn't fit. For an
out-of-taxonomy page, skip category-specific calibration in every step below but still run every
check; the generic rules that aren't category-conditional still apply.

## 1. Developmental pass (big picture)

Read the whole page once as its target reader (per its category — for a Concept guide, a
developer who knows Python or Groovy basics but not this feature) before checking anything
line by line. Answer, in writing:

- **Purpose:** What will the reader be able to *do* after reading this page? State it in one
  sentence. If you can't, that's the top finding.
- **Key message:** What must the reader walk away with, and is it stated plainly and early? A
  page can be accurate throughout and still never say its main point (e.g. "Deephaven
  parallelizes queries for you; most queries need no changes — the controls below are for the
  exceptions").
- **Mental model:** What model of the system will the reader form from this page, and is it the
  right one? Write down the page's core claims as the reader would summarize them. Hand that
  list to the accuracy step as its first targets: a wrong mental model (a whole category of
  operations classified wrongly) is the most expensive error a page can make, and it's easy to
  miss by checking sentences one at a time.
- **Audience fit:** Concepts used before they're introduced; terms the reader won't know;
  internal jargon; explanations that talk down.
- **Progression:** Does the page open with motivation, build one idea on the last, and close with
  a summary or next step? Would a reader know from the headings alone why to read each section?
- **Scope:** Tangents, implementation notes, or configuration detail that belong on a reference
  page, in a Configuration section, or on another page.

End with a verdict: **ready for technical review**, **needs revision**, or **needs
restructuring**. If it's "needs restructuring," still run the accuracy step in full (wrong claims
matter regardless of structure), but report structure and style findings as patterns with one
or two examples each rather than line by line — line-level edits on text that's about to be
reorganized are wasted effort for the author. In edit mode, the verdict doesn't license a
rewrite: apply the targeted fixes and put the restructuring plan in the report (see **"Apply the
fixes" means fix, not rewrite** above). Audit and overhaul fixes are the exception: restructure as
**Audit and overhaul fixes are the explicit exception** above describes.

This pass is report-only. It doesn't replace `deephaven-doc-structure-review`: that skill checks
specific organizational patterns; this pass asks whether the page is doing the right job at all.

## 2. Accuracy

Invoke `deephaven-core-accuracy-check` on the doc, starting from the core claims the
developmental pass wrote down. Review the whole page, not the diff: unchanged sentences next to an edit are in scope, and the accuracy check builds a claim ledger so coverage is visible. Facts before reorganizing: there's no point building a clean
structure around a wrong claim, and it's easier to verify claims against source while they're
still in their original location and context. Per the report-by-default rule above, this step
reports issues; only apply the fixes it finds if the user asked for edits. Claims the accuracy
check can't verify go into the report's author queries, not into hedged prose.

If a fix is applied and it corrects a shared, substantive claim in the cross-language sibling too
(`deephaven-core-accuracy-check`'s own cross-language-consistency check may have already edited
both files) — track that sibling as a second doc in scope. Run every later step on it as
well (not just structure and style — a structural edit to the sibling needs the re-verify step just
as much as the originally-requested file does), not just the originally-requested file; a sibling
edited by the accuracy pass but never structurally or style-reviewed is exactly the kind of
half-finished pass this skill exists to prevent.

## 3. Structure

Invoke `deephaven-doc-structure-review` as the middle step of this orchestrator — its own
instructions know to skip its standalone full accuracy/style re-run in that case and defer to
this workflow's re-verify and style steps instead, so don't expect or trigger that separately
here. In a report, this step may recommend moving, merging, cutting, reordering, or renaming sections.
When applying fixes, make only the targeted structural fixes allowed under **"Apply the fixes" means
fix, not rewrite** above (for example, delete a duplicate or move one paragraph), and report larger
restructuring instead, unless the user asked for a rewrite or the task is an audit or overhaul fix
(see **Audit and overhaul fixes are the explicit exception** above). Either way, content verified in the accuracy
step can change here, which is why the re-verify step exists.
Note everywhere content was moved, merged, cut, reordered, renamed, **or reworded in place**
(rewritten without changing location) — the re-verify step needs the complete list, since a rewrite that
changes a claim without moving its section would otherwise never reach the spot-check, and a
deleted caveat or a renamed-away section can invalidate an accuracy finding just as easily as a
literal move can.

## 4. Re-verify what structure touched

For every section from the structure step's list, handle it by what happened to it:

- **Moved, merged, or reworded** (surviving prose): re-run `deephaven-core-accuracy-spot-check`
  on it as the middle step of this orchestrator — its own instructions know to skip its local
  style step in that case and defer to this workflow's style step instead, so don't expect or trigger
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
- **Any caveat, exception, or cross-language distinction that was near content the structure step touched**
  (moved, merged, cut, or renamed): check the rest of the document first for where it may have
  landed or been restated, and flag it as dropped only if you can't find it after that check. If
  you're still not sure after checking, say so explicitly ("possibly dropped, unconfirmed — verify
  against the pre-edit version") rather than stating it as a confirmed finding — a false "this was
  dropped" claim costs a reviewer real time chasing content that's actually still there.
- **Links and anchors**: neither the spot check nor the style step validates that a link's
  *target* still resolves or that a heading's *anchor fragment* is still correct after an edit —
  `deephaven-writing-style` checks link wording and first-mention linking, but not target/anchor
  resolution. A heading rename, section move, merge, or cut can silently break an internal link
  or an anchor fragment (`#some-heading`) even when every claim in the doc remains correct. For
  any section the structure step moved, renamed, merged, or cut (not just moved/renamed): re-check that links
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

## 5. Examples

For each code example (the structure review's structure map lists them), check what no other
step does. The accuracy check has verified its API use and claimed behavior against source, but it
doesn't run snippets. The docs snapshotter runs runnable examples when snapshots are regenerated
(`docs/README.md`), so an example edited in this review hasn't been run until then — say so in the
report rather than assuming it works. Blocks marked `syntax` or `skip-test` are never run
(`docs/snapshotter/README.md`), so nothing will validate them later; check those by reading. Then check:

- **Does it illustrate the concept its lead-in names?** An example introduced as "a formula with
  side effects" that has none, or a barrier example where the barrier isn't what makes the output
  correct, teaches the wrong lesson even when it runs. (The accuracy check's **Example-necessity
  check** covers whether the demonstrated API is load-bearing; this asks whether the example is
  the right one for the reader.)
- **Is every major concept shown, not just described?** Flag long conceptual stretches with no
  example, and concepts that would be clearer as a wrong-then-right pair (the unsafe query and its
  corrected form, with both outputs).
- **Does the traced result teach the concept?** The accuracy check has already traced each
  example's data through the code. Reuse those values and judge whether the output visibly shows
  what the lead-in says it will, to this page's reader. Trace an example the accuracy check skipped.
- **Do blocks that share a `test-set` agree on settings?** `docker-config` needs to appear on only one
  block in a set; the other blocks inherit it (`docs/snapshotter/README.md`). Flag a block that names a
  different `docker-config` from the rest of its set, which is an error, but not a block that omits it.
- **Is it executable and tested?** A `syntax` or `skip-test` block where a runnable one would work
  isn't validated by the docs snapshotter; flag it unless the page has a reason.
- **Is it readable in one view?** Short enough to follow, with realistic names, and with comments
  that explain *why* rather than restating the code.

If the user asked for edits and you change an example's code or its comments, re-verify the
changed example with `deephaven-core-accuracy-spot-check` before moving on, as in the re-verify
step.

## 6. Style last

Invoke `deephaven-writing-style` over the whole doc (both files, if the accuracy step put a
cross-language sibling in scope). Run it last because both the accuracy fixes and the structural
moves (plus their re-verification) introduce or relocate prose that hasn't had a dedicated style pass yet — running style
first would mean re-doing it. Per the report-by-default rule above, this step reports style
issues; if the user asked for edits to be applied here too, prefer a fix that only changes
formatting, wording style, or phrasing — if a style fix would also change what a sentence
technically claims (not just how it's worded), re-verify that reworded claim against source before
applying it, the same way the accuracy step would have. A style pass is not exempt from being wrong about
facts just because it isn't the accuracy step.

## Applying fixes: the placement gate

When the user asked for edits, every fix — from any step — passes one question before it's
applied: **does this fix belong here, or somewhere else?** A fix that adds a property name, a
default, or a threshold to Concept-guide or Tutorial narrative is usually true and still wrong for
the page; put the detail in a Configuration section or behind a reference link, and make the
narrative sentence correct at its own level of abstraction (see `deephaven-core-accuracy-check`'s
**Placement of configuration detail**). A hedge or caveat that isn't configuration detail (a
version, environment, or platform restriction) stays only if the reader needs it at that point, as
its own sentence; otherwise cut it. This matters most when
working through an external reviewer's comments over several rounds: each individually-correct
caveat passes validation while the page as a whole gets harder to read. After each round, re-read
every section that changed from top to bottom and consolidate what has accumulated. If a
requested fix is correct but belongs elsewhere, say so in the reply rather than applying it
inline.

The gate applies to prose you write yourself, not only to fixes a finding suggested. Before
finishing, read every sentence you added or rewrote and look for asides and caveats of your own:
em-dash or parenthetical qualifications ("— here, the console session's scope"), "which can
take…", "usually," "in most cases," "unless something else…". Keep one only if a reader of that
section would get something wrong without it; otherwise cut it, or move it to where the detail
belongs. A replacement sentence should be no longer or more qualified than it needs to be to be
correct.

When the edits come from a set of review comments rather than from this review, triage
them with `deephaven-docs-address-review-comments` first — it sorts each one into apply,
redirect, decline, or ask before anything changes.

## 7. Report

Return one review document, prioritized — biggest issue first within every section, not document
order. Note the doc's category at the top so a reviewer can sanity-check severity calls (e.g. an
orphaned aside flagged harder because the doc is a Reference guide). If a cross-language sibling
was pulled into scope by the accuracy step, report on both files.

```
## Editorial summary
2–4 sentences: overall assessment, the single biggest issue, and the developmental verdict
(ready for technical review / needs revision / needs restructuring).

## Developmental notes
Numbered, highest impact first. Each: what, where, why it matters to the reader, suggested fix.

## Accuracy
Findings from the accuracy and re-verify steps, highest impact first (a wrong mental model or
misclassified category before a wrong parameter name).

## Structure
Findings from the structure step.

## Examples
Findings from the examples step.

## Style
Patterns with one or two examples each and a count, not one row per instance. Line-level rows
only for findings that aren't part of a pattern.

## Author queries
Questions the review couldn't resolve from source — mostly technical claims that need an SME.
Format: AQ1 [heading, para N or line N]: question

## Coverage
Factual or behavioral claims in prose, headings, lists, tables, captions, and code comments (the
accuracy check's claim ledger): how many rows, how many were verified against source, and how many
are author queries. Include the ledger table itself, one row per claim, so a reader can see which
sentences were checked. A page with no ledger was not fully reviewed. In edit mode, report the ledger for the finished
page: update it after the edits, so it drops removed claims and includes rewritten ones.

## Strengths
1–3 specific things that work and should be kept, so a revision doesn't remove them.
```

Each finding names the step that surfaced it and its location (heading plus line number). Be
specific: "unclear" isn't a finding — say what is unclear, and to which reader. Don't rewrite
whole sections in the report; show one example of the fix and let the author apply the pattern.
Tone toward the author: collegial and candid.
