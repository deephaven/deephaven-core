---
name: deephaven-doc-structure-review
description: Critique the structure, organization, and readability of a deephaven-core (Community) documentation page — will a new reader find it easy to follow, are concepts introduced in a sensible order, is content duplicated or interleaved oddly? Use this when a doc "feels hard to follow," when asked to review organization/flow/readability specifically (not facts or prose style), when drafting or substantially restructuring a long conceptual guide, or when a reviewer's feedback is about the document's shape rather than its content. Complements `deephaven-core-accuracy-check` (is it true?) and `deephaven-writing-style` (is each sentence/heading/example styled correctly?) — this skill asks whether the document as a whole is organized so a reader can follow it.
allowed-tools: Read, Grep, Glob, Edit
---

# Deephaven documentation structure review

This skill is about the *shape* of a document, not its content or prose. A page can be
100% technically accurate and perfectly styled sentence-by-sentence and still be hard to
follow because concepts arrive in the wrong order, the same idea is explained twice in
different places, or a warning that matters is buried where a skimming reader won't see it.
Run `deephaven-core-accuracy-check` and `deephaven-writing-style` separately for fact-checking
and prose-level style — don't duplicate their checklists here, and don't let a structural
rewrite silently break a technical claim or introduce a style violation (re-run those skills
after a structural edit that moves or merges prose).

## 1. Build the structure map before reading prose in depth

- Extract the full heading outline with `grep -n '^#' <file>` and note each heading's depth
  (`#`, `##`, `###`, ...), not just its text. Do this first — most of the patterns below are
  visible from the outline alone, before you've read a single paragraph closely.
- List every code example in the doc (fenced blocks) with a one-line note on what concept or
  scenario each one demonstrates.
- List every callout/admonition (`> [!NOTE]`, `> [!WARNING]`, `> [!IMPORTANT]`, etc.) with a
  one-line note on what it says.
- Note where any glossary, "Key concepts," or terminology-definition section sits relative to
  the start of the document.

You'll cross-reference all three lists against the prose in the checks below — build them once,
up front, rather than re-deriving them per check.

## 2. Named structural pitfalls to check

- **Terminology introduced before it's defined:** Scan the opening sections for domain-specific
  terms (jargon, product-specific concepts, internal names) used in running prose before any
  definition or glossary entry appears. A term used casually in an intro or overview section,
  then formally defined only in a glossary buried mid-document, leaves an early reader with no
  anchor for it. Fix: move the definition earlier (ideally to first use), or add a forward
  reference ("see **Key concepts** below") the first time the term appears.

- **Split or duplicated core-concept explanations:** Check whether the *same* underlying concept
  gets explained twice at different points in the document — once briefly or implicitly early,
  then fully much later. If a reader has to hold the same idea in mind across two separated
  explanations, that's a defect even if neither explanation is individually wrong. Fix: merge
  into one explanation at first substantive use; later mentions should link back, not re-explain.

- **Near-verbatim repeated examples:** Using your example list from step 1, cluster examples by
  the underlying scenario they illustrate (e.g., multiple "shared counter" examples, multiple
  "cache" examples). Two or more examples with the same structural setup teaching the same point
  is a red flag — the reader is shown the same lesson repeatedly with cosmetic variation instead
  of the concept building progressively. Fix: consolidate into one canonical worked example,
  reused via cross-references or callbacks ("using the same `counter` example from above...")
  rather than restated fresh each time.

- **Duplicated callouts with drifting wording:** Using your callout list from step 1, check for
  the same warning or breaking-change notice appearing more than once. Repetition for emphasis
  can be fine, but if the wording or level of detail differs between copies, it reads as though
  two different things happened. Fix: state it once, in the most prominent relevant location;
  other mentions should link to it rather than restate it with variations.

- **Topic interleaving / broken continuity:** Check whether the doc introduces a topic at a high
  level, detours into a different (even if related) topic, and only then returns to finish the
  first topic in depth — e.g., "how to control X" → "how X executes internally" → "how to
  control X in detail." This forces a reader following one thread to context-switch away and
  back. Fix: keep all sections about one continuous topic contiguous; move the detour either
  before the topic starts or after it's fully wrapped up.

- **Heading depth mismatch for parallel concepts:** From your heading outline, check whether
  conceptually parallel ideas sit at the same nesting depth throughout the document. If concepts
  A/B/C appear as sibling `###` headings in one section, and A/B/C get re-covered later as `####`
  headings nested under a *different* `###`, the document effectively has two competing outlines
  of the same ideas — confusing when scanning a table of contents. Fix: either fold the second
  pass into the first section, or make the depths consistent.

- **Compare/contrast arriving too late:** When a document introduces two related-but-distinct
  mechanisms (e.g., two ways to control the same behavior), a side-by-side comparison is most
  useful right where both are first named — not only after each has already been explained
  separately in full much later. Fix: add a brief compare/contrast at first joint mention; a
  fuller comparison later is fine as reinforcement, not as the reader's only orientation.

- **Orphaned asides:** A subsection covering an advanced or niche topic with no clear transition
  from what precedes it, and no connection to a nearby reference/summary table that would
  naturally include it. If deleting the sentence before it wouldn't be missed, the section is
  probably misplaced or needs an explicit bridge sentence explaining why it's here. Fix: either
  connect it explicitly to the surrounding narrative, fold a pointer to it into the nearest
  reference table, or move it to where it's actually relevant.

- **Intro/first-section overlap:** Check whether the opening paragraph and the very next named
  section explain the same basic idea in near-identical terms before any new information is
  introduced. An introduction should preview and orient the reader, not restate the first
  section's content. Fix: trim the intro to orientation only ("this page covers X, Y, Z") and
  let the first section do the actual explaining.

- **Length and repeated-example fatigue:** Long documents (rough guideline: 300+ lines) with
  multiple code examples covering the same underlying scenario (see the near-verbatim-examples
  check above) risk losing readers before they reach the summary. Treat any doc matching both
  conditions as a consolidation candidate even if no single example is individually flagged.

- **Closing-section and summary placement:** Confirm a closing summary (commonly "Key
  takeaways" in this doc set) and a "Related documentation" section exist, per
  `deephaven-writing-style`'s page-structure rule. Separately — this is the single highest-
  leverage restructuring move for a long conceptual doc — check whether a quick-reference or
  summary table that currently appears near the end could be promoted earlier as a short preview,
  so the reader has an orientation map before working through the detailed walkthrough.

## 3. Report

Output a clear, concise bullet list, one bullet per finding, each naming the specific pattern
(from the list above or a new one you observed), citing the concrete location(s) in the document
(heading names and/or line numbers — build these from your step-1 outline, don't estimate), and
proposing a specific fix (merge X into Y, move section Z before W, promote the table at line N
to appear after the intro) rather than a vague "this feels redundant." Order findings by how much
they'd actually confuse a first-time reader, not by document order.

Do not rewrite the document as part of this review unless asked to — a structural critique is a
report first. If the user then asks you to apply the restructuring, do it as an explicit,
reviewable diff, and re-run `deephaven-core-accuracy-check` and `deephaven-writing-style` on the
result before considering it done: moving and merging prose is exactly the kind of edit that can
quietly drop a caveat, break a cross-reference, or introduce a style violation.
