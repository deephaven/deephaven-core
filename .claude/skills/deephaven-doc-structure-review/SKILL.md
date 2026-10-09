---
name: deephaven-doc-structure-review
description: >
  Critique the structure, organization, and readability of a deephaven-core (Community) documentation page — will a new reader find it easy to follow, are concepts introduced in a sensible order, is content duplicated or interleaved oddly? **Use this skill when:** drafting, restructuring, or outlining a crash course, concept guide, how-to, or overview; someone says a doc "feels hard to follow" or asks to "review organization/flow/readability" or "is this outline right"; or a reviewer's feedback is about the document's shape rather than its content. Complements `deephaven-core-accuracy-check` (is it true?) and `deephaven-writing-style` (is each sentence styled correctly?) — this skill asks whether the document as a whole is organized so a reader can follow it. Calibrates checks by doc category via `ref-deephaven-doc-categories`. **Do NOT use for:** fact-checking (use accuracy-check), prose style (use writing-style), or Enterprise/deephaven-ent docs.
allowed-tools: Read, Grep, Glob, Edit, Skill, Bash(awk *)
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

## 0. Identify the doc's category

Read `ref-deephaven-doc-categories` and determine which of the four categories (Tutorial —
Crash Course only, How-to guide, Concept guide, Reference guide) this doc is — check that file's
"Pages outside the four categories" section first if it doesn't obviously fit one (e.g.
`intro.md`, or a contributor-facing tooling README); for an out-of-taxonomy page, skip the
category weighting below entirely and apply only the generic structural checks in **Named structural pitfalls to check**. For a
page that does fit one of the four, category changes how severely several checks below should
weigh:

- **Tutorial** (Crash Course): treat any branch or "if you want X instead" aside as a bigger
  defect than elsewhere — the category's whole point is one linear path for every reader.
- **Concept guide**: the category most exposed to split/duplicated explanations and topic
  interleaving, since it's usually the longest and most narrative. Weight those checks up.
- **Reference guide**: an orphaned aside or a gap in an enumerated list matters more here than in
  a concept guide — a reference reader is scanning for one fact, not reading linearly. This does
  not apply to individual `reference/community-questions/*` Q&A pages, which are one question and
  one answer per page, not an enumerable reference — weight those like a how-to guide instead.
  `cq-index.md` itself is the exception to that exception (see `ref-deephaven-doc-categories`) and
  stays on the normal enumerable/reference weighting — a missing or stale card entry there is a
  real gap, not exempt just because it lives in the same directory as the Q&A pages.
- **How-to guide**: branching and offered alternatives are expected and not a defect by
  themselves; judge flow by whether the reader can still complete their own goal, not by whether
  every reader follows the identical path.

## 1. Build the structure map before reading prose in depth

- **State the page's purpose in one sentence first:** what will the reader be able to *do* (or
  understand well enough to act on) after reading it? If you can't write that sentence from the
  intro and headings alone, that's the top finding — every other structural check is secondary
  to a page with no clear goal. Keep the sentence in view; several checks below ask whether a
  section serves it.

- Extract the full heading outline — but not with a naive `grep -n '^#'`: this doc set's fenced
  code blocks contain column-1 `#`-prefixed comments (Python) that a bare grep misreads as
  headings, and some pages nest a ` ``` ` example inside a ` ```` ` outer fence (e.g.
  `docs/snapshotter/README.md`). This awk one-liner tracks the opening fence's backtick count and
  only closes on a line with at least that many, which handles plain and nested column-1 backtick
  fences — but this doc set also contains fence styles it does *not* recognize: indented fences
  (`docs/python/how-to-guides/debugging/embedded-setup.md:119`,
  `docs/groovy/how-to-guides/install-use-plugins.md:46`) and blockquoted fences
  (`docs/python/how-to-guides/use-uris.md:186`), plus `~~~` fences it was never meant to catch.
  It's a fast first pass, not full coverage:
  `` awk 'match($0,/^`{3,}/){len=RLENGTH; if(!f){f=1;delim=len} else if(len>=delim){f=0}; next} f{next} /^#/{print NR": "$0}' <file> ``.
  Treat its output as a draft, not ground truth — skim the file once yourself against the
  extracted outline, and if the doc uses an indented, list-nested, blockquoted, or `~~~` fence
  anywhere, fall back to reading it directly rather than trusting the script's output for that
  section. Note each real heading's depth (`#`, `##`, `###`,
  ...), not just its text. Do this first — most of the patterns below are
  visible from the outline alone, before you've read a single paragraph closely.
- List every code example in the doc (fenced blocks) with a one-line note on what concept or
  scenario each one demonstrates.
- List every callout/admonition (`> [!NOTE]`, `> [!WARNING]`, `> [!IMPORTANT]`, etc.) with a
  one-line note on what it says.
- Note where any glossary, "Key concepts," or terminology-definition section sits relative to
  the start of the document.
- Build a **term list** for the whole page: every domain term, mode name, setting name, or label
  that isn't plain English (for example "barrier," "implicit barriers," "stateless mode"),
  with the line of its first use and the line where it is defined or linked to the page that defines it, or "never" (reserved for terms that are neither defined on the page nor linked). Do this from the
  prose of every section, not only the opening ones. The check under **Terminology introduced
  before it's defined** uses this list.

You'll cross-reference all four lists against the prose in the checks below — build them once,
up front, rather than re-deriving them per check.

## 2. Named structural pitfalls to check

- **Terminology introduced before it's defined:** Using your term list, check every term on the
  page, not only the ones in the opening sections. Compare each term's first use with the
  earliest definition, glossary entry, or link to the page that defines it. A term defined or
  linked at its first use is fine. A term whose definition or link comes only after its first
  use leaves the reader with no anchor at that point. A term with no definition and no link
  anywhere on the page is the stronger finding: the reader has to leave the page or guess.
  Watch for these shapes:
  - A feature name used as if the reader already knew it ("When implicit barriers are enabled,
    …") with no sentence saying what it is or what enabling it means.
  - A label reused with a different meaning than the page gave it earlier (a page that defines
    "stateless" as rows independent of each other, then labels a configuration option "stateless
    mode").
  - A pronoun phrase with no antecedent ("this setting" when no earlier sentence names a setting).
  - A section that introduces several new terms in a few sentences. Rank it by how many reads
    it takes to get its point: a section a reader would have to read five times is a top
    finding, however accurate it is.

  Fix: define the term at or before its first use, in a plain sentence that says what it is and
  what the reader would observe (not only where its setting lives); or link to the page that
  defines it. Don't send the definition to a glossary the reader may never open. If the
  definition needs a configuration property or default, keep the property in the Configuration
  section and define the behavior in the narrative (see **Level of abstraction**).

- **Section can't be read on its own:** People skip around, so each section should make sense to
  a reader who lands on it from the table of contents or a heading link. This applies to Concept
  guides, Reference guides, and how-tos that readers dip into. A Crash Course chapter and an
  explicitly sequential how-to may rely on steps the reader has just completed, so for those only
  flag an opener that leaves the reader unable to tell what it refers to. For each section, cover
  the earlier sections and read only its heading and first paragraph. If you find yourself
  asking "the same as what?", the section fails. The usual cause is an opener that points back
  without saying to what: "The same applies to …", "As above, …", "This also …", "Likewise, …",
  "Again, …".

  A section may build on what immediately precedes it, because that is a natural reading order,
  but only if the opener **names** what it builds on (by heading name or link), says in a clause
  what it takes from there, and the section still makes its own point. "Building on the
  [counter example](#example-a-counter-needs-serialization), consider two columns that share a
  counter" passes, because it names the example and restates the setup. "The same applies to
  filters." fails even when the section it points to is directly above, because the reader
  can't tell what "the same" is without finding it. A pointer to something that is *not*
  adjacent passes by the same test: it names the target (a link helps but isn't required) and
  restates what it needs from it in a clause. Don't list a pointer that passes as a defect just
  because the example it names is several sections up; flag only pointers that fail the test
  ("as above," "the same," "this" with no named target).

  Fix: rewrite the opener to state its own subject in one sentence ("Deephaven parallelizes
  string-based filters in `where` by default, so construct a serial `Filter` when …"). Deleting
  the sentence is only right if the section reads on its own afterward, so check that first.
  Don't require a section to repeat its prerequisites: one clause of context plus a link is
  enough, and restating a whole earlier section is the "duplicated explanation" pitfall below.

- **Split or duplicated core-concept explanations:** Check whether the *same* underlying concept
  gets explained *in full* twice at different points in the document — once briefly or implicitly
  early, then fully much later, with neither occurrence acknowledging the other. If a reader has
  to hold two independent full explanations of the same idea in mind, that's a defect even if
  neither is individually wrong. This is distinct from a short, explicitly-labeled preview
  followed by the one full explanation, or a callback that reinforces an already-given
  explanation rather than re-teaching it from scratch — those are legitimate, and the
  early-preview and compare/contrast-as-reinforcement guidance elsewhere in this skill depends on
  telling the two apart. Fix: merge redundant full explanations into one at first substantive use;
  later mentions should link back or explicitly recap, not silently re-explain.

- **Near-verbatim repeated examples:** Using your example list from the structure map, cluster examples by
  the underlying scenario they illustrate (e.g., multiple "shared counter" examples, multiple
  "cache" examples). Two or more examples with the same structural setup teaching the same point
  is a red flag — the reader is shown the same lesson repeatedly with cosmetic variation instead
  of the concept building progressively. Fix: consolidate into one canonical worked example,
  reused via cross-references or callbacks ("using the same `counter` example from above...")
  rather than restated fresh each time.

- **Duplicated callouts with drifting wording:** Using your callout list from the structure map, check for
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

- **Heading depth mismatch for duplicated (not overview/detail) coverage:** Heading depth
  expresses local parent/child structure, not a document-wide semantic rank — an overview section
  naming A/B/C as `###` siblings, followed later by a legitimately deeper "Deep dive" section that
  nests full treatments of A/B/C as `####`s, is a normal and fine outline; don't flag that. The
  actual pitfall is when the *same level of explanation* for A/B/C is given twice — once as
  siblings in one section, again as siblings nested under a different, unrelated parent later —
  which is two competing outlines of the same content, not an overview-then-detail structure, and
  is confusing when scanning a table of contents. Fix: fold the duplicate pass into the first
  section, or make explicit (in heading text or a lead-in sentence) that the second occurrence is
  deliberately deeper detail rather than a repeat.

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

- **Parent/child terminology continuity:** When an intro or parent section enumerates the things
  its children cover ("Deephaven parallelizes in two ways: across tables and within a table"),
  the child headings must use the same terms, in the same order, as that enumeration. A child
  heading that renames the concept ("Concurrent row calculations" under a parent that said
  "within a single table"), or a parent paragraph that never introduces a split its children
  then rely on, is an effective terminology change the reader has to reconcile on their own.
  Compare every enumeration in the intro and in each parent paragraph against the structure-map outline.
  Fix: rename the headings or rewrite the enumeration so the two match exactly; if the children
  genuinely subdivide further, say so in the parent paragraph.

- **Level of abstraction:** For each section, ask "would a reader expect to find this here?"
  given the page's category and the section's own heading. Implementation or configuration
  detail inside a conceptual explanation is the common case: a Python-GIL/free-threading note
  under "How parallelization works > Within a single table," or a property name and default in
  parentheses in the middle of an explanation of what the engine does. A parenthetical is often
  the tell — the author already sensed it didn't belong in the thought. Fix: move configuration
  values into one Configuration section at the end of the page (or link to the configuration
  reference, `conceptual/query-table-configuration.md`); move environment or version caveats
  to a prerequisites note or the section they actually govern; leave a one-line pointer behind
  only if the reader needs it at that point. This check is `deephaven-core-accuracy-check`'s
  **Placement of configuration detail** seen from the structure side — accuracy decides what's
  true, this check decides where it goes.

- **Category consistency in tables and lists:** Every row of a table and every item in a list
  should be the same *kind* of thing — all general categories, or all concrete examples, or all
  operations, not a mix. A list of "reasons a formula needs serial execution" that mixes a general
  category ("depends on the order in which rows are evaluated"), a single specific example ("a global
  counter"), and an item that doesn't meet the list's criterion at all (a row-local calculation
  that reads only its own row's inputs) forces the reader to work out what the list is actually about. For each
  table or list, name the category its items share, then check each item against it. Fix: lift
  specific examples to the category they illustrate (or move them into an example column), and
  cut items that don't meet the list's criterion.

- **List contents match the enclosing section:** A table or list sitting under a section heading
  should contain only items that belong to that section's scope. A quick-reference table under
  "Within a single table" that also lists across-table behavior, or a "What gets parallelized"
  list under a heading about one operation, reads as though the section's scope is wider or
  narrower than its heading says. Fix: move the list up to the parent that actually spans its
  contents, or split it so each part sits under the section it describes.

- **Closing-section and summary placement:** This is about structural placement, not the
  "Related documentation" requirement itself — that's `deephaven-writing-style`'s rule (with its
  own exemptions), don't re-derive it here. A closing summary (commonly "Key takeaways" in this
  doc set) isn't required by that rule either, but its presence or absence on a long conceptual
  page is still a useful structural signal to note. Separately — this is the single highest-
  leverage restructuring move for a long conceptual doc — check whether a quick-reference or
  summary table that currently appears near the end could be promoted earlier as a short preview,
  so the reader has an orientation map before working through the detailed walkthrough.

## 3. Report

Output a clear, concise bullet list, one bullet per finding, each naming the specific pattern
(from the list above or a new one you observed), citing the concrete location(s) in the document
(heading names and/or line numbers — build these from your structure-map outline, don't estimate), and
proposing a specific fix (merge X into Y, move section Z before W, promote the table at line N
to appear after the intro) rather than a vague "this feels redundant." Order findings by how much
they'd actually confuse a first-time reader, not by document order.

Do not rewrite the document as part of this review unless asked to — a structural critique is a
report first. If the user then asks you to apply the restructuring, do it as an explicit,
reviewable diff, and — **when invoked standalone** — re-run `deephaven-core-accuracy-check` and
`deephaven-writing-style` on the result before considering it done: moving and merging prose is
exactly the kind of edit that can quietly drop a caveat, break a cross-reference, or introduce a
style violation.

**When invoked as the middle step of `deephaven-docs-review-full`**, skip that re-run: the
orchestrator's own re-verify step (targeted spot-check re-verification) and style step already cover it,
in a more scoped and correctly-ordered way than re-running the full accuracy and style skills
here would. Running the full re-run here too would duplicate the style step and pre-empt the re-verify step with a
full accuracy pass before the orchestrator's lighter, targeted one — say so in your output
("structural edits applied; deferring re-verification to the orchestrator's re-verify and style steps") rather
than silently doing the full re-run.
