---
name: deephaven-docs-address-review-comments
description: >
  Work through reviewer comments on a deephaven-core (Community) documentation PR — GitHub Copilot or other bot reviews, or human reviewers — without letting a stream of individually-correct suggestions degrade the page. **Use this skill when:** someone asks to "address," "respond to," "fix," "resolve," "go through," or "handle" review comments, Copilot comments, bot suggestions, or PR feedback on a doc, or pastes review comments and asks what to do with them. It verifies every comment against source, triages each one (apply / redirect / decline / ask) before any edit, applies only what belongs at the page's level of abstraction, re-reads the changed sections as a whole, and drafts a reply for every comment. **Do NOT use for:** a fresh review with no comments (use deephaven-docs-review-full), review comments on code rather than docs, or Enterprise/deephaven-ent docs.
allowed-tools: Read, Grep, Glob, Edit, Skill, Bash(git diff *), Bash(git log *), Bash(gh pr view *), Bash(gh api *)
---

# Addressing review comments on deephaven-core docs

A review comment is a claim about the doc plus a proposed change. Both parts need checking, and
they fail independently. Bot reviewers such as Copilot are often right about the facts and wrong
about the fix: they see one sentence, find it imprecise, and ask for the missing property name,
default, condition, or caveat to be added right there. Each such fix is true. Ten rounds of them
turn a concept guide or tutorial into a page nobody can follow — the failure this skill exists to
prevent. Human reviewers make the same mistake less often but not never.

So the job is not "make every comment go away." It is: fix what is wrong, put detail where it
belongs, decline what would make the page worse, and say why for each one.

**Report by default.** Unless the user asked for the fixes to be applied, produce the triage and
proposed replies (step 5) and stop. Never post replies or resolve threads on GitHub unless the user
explicitly asks — replying is outward-facing.

## 1. Gather and orient

- Get every comment with its location: from the PR (`gh api repos/<owner>/<repo>/pulls/<n>/comments`,
  plus review bodies from `.../reviews`), or from what the user pasted. Note which comments are
  from bots and which review round each belongs to — how many rounds this page has already had
  matters in step 3.
- Read `ref-deephaven-doc-categories` and identify the page's category. Placement rules differ:
  in a Concept guide or Tutorial, configuration names, defaults, thresholds, and edge-case
  conditions stay out of the narrative; in a Reference guide or configuration page they *are* the
  content.
- Read the whole page once before reading any comment closely, and write down in one sentence
  what the reader should be able to do after reading it, and at what level of detail the page
  works. Every triage decision below is measured against that sentence, not against the single
  sentence a comment points at.

## 2. Verify each comment's premise

For every comment, check the factual claim it makes against source, the same way
`deephaven-core-accuracy-spot-check` does (quote the source; never accept the claim because a bot
or a reviewer stated it confidently). Record one of: **true**, **false**, **true but the doc
already handles it** (the doc says it elsewhere, or says it correctly at a lower resolution), or
**unverifiable** (becomes an author query).

A comment can be true about the engine and still not describe a defect in the doc. "Sorts also
parallelize, at a different threshold" is true; a how-to paragraph about `update` that doesn't
mention sorting is still correct.

## 3. Triage every comment before editing anything

Put each comment in exactly one bucket:

- **Apply** — the doc is wrong at its own level of detail: a false or contradicted claim, code
  that won't run or doesn't show what the text says, a broken link or anchor, a house-style rule
  violation (see `deephaven-writing-style`). Fix it at the page's level of abstraction. This
  often means making the sentence *less* specific, not more.
- **Redirect** — the premise is true, but the detail the comment asks for belongs somewhere else:
  a Configuration section at the end of the page, the configuration reference
  (`conceptual/query-table-configuration.md`), the method's reference page, or a different doc.
  Make the narrative sentence correct at its resolution, and put or link the detail where it
  belongs. Don't move it into a separate sentence in the same paragraph — that is still
  injection (see `deephaven-core-accuracy-check`'s **Placement of configuration detail**).
- **Decline** — the premise is false, or the change makes the page worse for its reader: a
  hedge or caveat on a sentence that is already correct at its resolution; detail that serves an
  edge case the page isn't about; a change that conflicts with another comment, an earlier
  round's decision, or the page's purpose; a change that makes an example slower or harder to
  run for no teaching gain.
- **Ask** — it needs a decision from the author or an SME (what the page should promise, whether
  a behavior is intended, which of two conflicting comments wins). Write an author query.

Default rules for sorting:

- In a Concept guide or Tutorial, a comment that asks to "qualify," "clarify," "mention," "note
  that," "specify," or name a property, default, threshold, or condition is a **redirect or
  decline candidate by default**. Apply it only if the sentence is false at its current level of
  detail, and then fix it at that level.
- A comment that asks for more precision about *how* something works, when the page's job is
  *what the reader should do*, is usually a redirect to the reference page.
- Check comments against each other and against earlier rounds. Contradictory requests (make an
  example large enough to actually parallelize / make it small enough for the docs build) are a
  signal that neither literal fix is right — find the version that serves the reader, or ask.
- **Late-round bar:** from the third round of comments on the same page onward, apply only what
  is flatly wrong. Everything else is a redirect, decline, or ask unless the user says otherwise.
- **Pile-up signal:** if several comments land on the same section, the section probably has a
  structural problem that patching won't fix. Say so and recommend `deephaven-doc-structure-review`
  for that section instead of applying the patches one by one.

## 4. Apply (only if asked) — in order, then re-read

If the user asked for edits:

1. Apply the **Apply** items first, verifying each rewritten sentence with
   `deephaven-core-accuracy-spot-check`. Run the duplicate-claim sweep from
   `deephaven-core-accuracy-check` (step 8) for Apply items only — the same wrong claim often
   appears in a table, a summary, a code comment, or the cross-language sibling. Don't propagate
   a redirected or declined suggestion to other sentences.
2. Then make the **Redirect** moves (create or extend the Configuration section, add the
   reference links).
3. Then style (`deephaven-writing-style`) on the lines you changed.
4. **Coherence gate:** re-read every section you changed from top to bottom, not just the
   flagged lines. Ask whether it still reads as though it was written once. If hedges,
   parentheticals, property names, or repeated pointers to the same setting have accumulated —
   from this round or earlier ones — consolidate before finishing. If a Python page changed,
   check whether the Groovy sibling needs the same Apply fixes (and only those).

## 5. Report and replies

Return:

```
## Page purpose
One sentence: what the reader should be able to do, and at what level of detail.

## Triage
| # | Location | Comment (short) | Premise | Decision | Reason | Proposed text / destination |
One row per comment. Premise = true / false / true-but-handled / unverifiable.

## Proposed edits
For Apply and Redirect items: the replacement text or the moved detail, per comment.

## Draft replies
One reply per comment. Apply: what changed. Redirect: where the detail went and why it isn't
inline. Decline: the reason, citing the page's purpose or the source. Ask: the question.
Collegial and brief; no defensiveness.

## Author queries
AQ1 [location]: question

## Section-level notes
Pile-ups, conflicts between comments, or sections that need a structural pass instead of patches.
```

If fixes were applied, add what changed and the result of the coherence gate.
