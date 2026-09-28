---
name: ref-deephaven-doc-categories
description: Reference — the four deephaven-core documentation categories (Tutorial, How-to guide, Concept guide, Reference guide), which repo directory each lives in, and the tone/structure expectations tied to each. Loaded by deephaven-writing-style, deephaven-doc-structure-review, deephaven-core-accuracy-check, and deephaven-docs-review-full to calibrate their checks to the doc's actual category. Not invoked directly — there is no scenario where a human asks for this skill by itself.
user-invocable: false
---

# Deephaven documentation categories (Community/Core)

Identify a doc's category before applying any category-dependent check — tone, structure, and
depth expectations all depend on it. This is the single source of truth for the categories;
`deephaven-writing-style`, `deephaven-doc-structure-review`, `deephaven-core-accuracy-check`, and
`deephaven-docs-review-full` read this file rather than each defining categories independently.
`deephaven-core-accuracy-spot-check` is deliberately not a consumer — it's scoped to one small,
already-isolated edit and stays category-agnostic on purpose; don't add a category-identification
prerequisite to it.

## The four categories

**Tutorial** — `docs/{python,groovy}/getting-started/crash-course/*` only. The Crash Course is
the single tutorial among these four categories: a fixed, ordered sequence where every reader
follows the same path to the same outcome. No other page under `docs/{python,groovy}` is a
tutorial, regardless of how step-by-step it reads — see **Common misclassification** below. (This
doesn't claim anything about contributor-facing tooling docs outside the four categories — see
**Pages outside the four categories** — some of which are step-by-step walkthroughs in their own
right; they're simply out of scope for this taxonomy, not classified as non-tutorials by it.)
- Tone: conversational, first-person narrative, can be playful, still professional.
- Structure: strict linear sequence. A reader should be able to go start to finish without
  branching; a structural review should treat a branch or "if you want X instead" aside as a
  bigger defect here than it would be in a how-to guide.

**How-to guide** — `docs/{python,groovy}/how-to-guides/*`, plus the `getting-started` pages that
are *not* part of the Crash Course (`quickstart.md`, `pyclient-quickstart.md`,
`jupyter-quickstart.md`, `docker-install.md`, `pip-install.md`, `launch-build.md`,
`production-application.md`). These follow the same step-by-step shape as the Crash Course, but
the reader supplies their own "ingredients" (their own data, tables, use case) and can reach a
different outcome — that's what makes them how-to guides, not tutorials. In `sidebar.json` these
fill essentially every **User Guide** subsection: Create/import/export tables, Work with tables,
Query strings, Python, Plot, GUI, Client APIs, Plugins, Development tools, Best practices and
troubleshooting, System setup and admin (informally "the admin guide" — authentication,
configuration, cloud deployment), Cloud deployment — including at least one placed inside
**Architectural details** itself: `how-to-guides/initialization-and-updates.md` sits there
alongside the conceptual live/ticking-table pages in both `docs/python/sidebar.json` and
`docs/groovy/sidebar.json` (search for its `"label"` entry — don't cite an exact line range here,
sidebar.json entries shift as pages are added, and a stale range is worse than none), presumably
for discoverability rather than as a category override (same rationale as the Concept-guide
co-location exception below). Don't treat
Architectural details as an Architectural-details-only zone — check the directory, not the
sidebar cluster, the same way you would for any other placement exception here.
- Tone: conversational, first-person narrative; can offer advice, tips, or alternatives.
- Structure: goal-oriented; branching ("if you want X instead, do Y") is expected and fine here.

**Concept guide** — `docs/{python,groovy}/conceptual/*`. Broadens understanding of a higher-level
idea; explains how or why with contextual explanation. In `sidebar.json` these cluster heavily
under **User Guide → Architectural details** (Deephaven's design, Patterns of use, The Table API,
Incremental update model, Live DAG, Table types, Column types, Deephaven Vectors, Core API design,
Servers & clients, What is Barrage?) — but that clustering is a strong tendency, not the
definition, and two kinds of exception are common enough to expect, not treat as anomalies:
  - **Overview pages under `conceptual/`** are concept guides by directory even when their sidebar
    placement puts them in a different section entirely rather than the Architectural details
    cluster — e.g. "Deephaven Overview" (`conceptual/deephaven-overview.md`, a top-level sidebar
    entry outside User Guide entirely) and "Table operations overview"
    (`conceptual/table-operations-overview.md`, filed under Work with tables). Don't over-trust
    the word "overview" itself as the signal, though — it points the wrong way as often as the
    right one: `how-to-guides/overview-kafka.md` is titled "Kafka Overview" but is a how-to guide
    by directory and content (it walks through using Kafka, not the concept of streaming), and
    `getting-started/crash-course/overview.md` ("Crash Course Overview") is the Crash Course's own
    landing chapter — a Tutorial page by directory, not a concept guide, despite the title. Check
    the directory and what the page actually does, not just its title.
  - **Concept guides co-located with their practical companions for discoverability.**
    `conceptual/query-engine/parallelization.md` and `conceptual/query-engine/engine-locking.md`
    sit under **Best practices and troubleshooting → Performance** in the sidebar, alongside
    how-to pages on the same topic (garbage collection, formula threads, tracking processing
    time) — because a reader hunting for performance help wants both together. That placement is
    for discoverability, not a category override: both pages still explain internal engine
    mechanism ("how/why parallelization works"), so they take the concept-guide tone and
    structure profile, not their how-to neighbors' profile.
- Tone: explanatory; analogy and narrative framing are appropriate.
- Structure: builds a mental model over the course of the page. This is the category most exposed
  to the structural pitfalls in `deephaven-doc-structure-review` (split explanations, topic
  interleaving, intro/first-section overlap) because it's typically the longest and most
  narrative-heavy category — weight those checks accordingly.

**Reference guide** — `docs/{python,groovy}/reference/*`. Purely informative — describes without
advice or extraneous tips. JavaDocs, PyDocs, and other API documentation are reference guides.
- Tone: dry, formal, third-person, no contractions. **Exception:** the 50+ *individual Q&A pages*
  under `reference/community-questions/*` (not the directory's own index page — see below) are a
  conversational Q&A format, not API documentation. The question isn't reliably in any one
  field — `sidebar_label` usually carries it ("Does it make any difference if I separate table
  operations or chain them together?"), but `title` doesn't always: `chained-operations.md`'s
  `title` is the same question, while `why-do-my-python-type-hints-error.md`'s `title` is just
  "Why" (truncated) and the actual question is in its body's opening `<em>` tag instead. Identify
  these pages by directory and the Q&A shape of their content — a single question answered
  conversationally, often with an opening emphasized question — not by assuming any specific
  front-matter field always holds it. The body doesn't have to open with a first-person question
  either: `chained-operations.md` does (its body opens "I have a query in which..."), but
  `custom-data-sources.md` opens with a direct answer ("Yes, you can integrate...") and
  `display-objects-python.md` opens with explanatory prose — all still take the how-to guide's
  conversational tone profile, not the dry/formal one, even though they live under `reference/`.
- Structure: enumerable and scannable. A reference reader is scanning for one specific fact, not
  reading linearly — an "orphaned aside" or a missing entry in an enumerated list is a bigger
  defect here than the same issue would be in a concept guide. This does not apply to individual
  `community-questions/*` Q&A pages, which are one question and one answer per page, not an
  enumerable reference.
  - **Exception to the exception:** `community-questions/cq-index.md` ("FAQ Overview") is the
    directory's own index page — a category-card landing page, not a Q&A. It stays on the normal
    Reference-guide profile: dry/formal tone, and it *is* an enumerable index (check it for
    missing or stale entries like any other reference index), not "one question and one answer."
    Every skill that carries this community-questions exception must carry this carve-out too —
    don't restate the wildcard without it.

## Pages outside the four categories

Almost every page under `docs/{python,groovy}` fits Tutorial, How-to guide, Concept guide, or
Reference guide by directory. Two kinds of page fall outside all four — a doc-review skill that
can't classify a doc into one of them should check here before assuming the doc or the
classification is broken:

- **The site landing page**, `docs/{python,groovy}/intro.md` (sidebar label "Introduction") — not
  under `crash-course/`, `how-to-guides/`, `conceptual/`, or `reference/` at all. Treat it as the
  "landing page" that `deephaven-writing-style`'s Related-documentation exemption already refers
  to: no tone/structure calibration from this file applies to it.
- **Contributor-facing tooling docs that live under `docs/` but aren't published** to
  deephaven.io — e.g. `docs/README.md`, `docs/snapshotter/README.md`,
  `docs/tools/autoimport/README.md`. These document the doc-build tooling itself for repo
  contributors, not a deephaven.io reader (see **Audience calibration** below); no tone/structure
  calibration from this file applies to them either.

This list isn't necessarily exhaustive — a future page could be added outside `docs/{python,groovy}`
entirely (a new top-level landing page) or as new contributor tooling documentation. When in
doubt, the test is the same as classifying any other page: does it live under one of the four
category directories *and* serve a deephaven.io reader? If not, it's out of taxonomy, whether or
not it's specifically named here.

## Common misclassification

"Tutorial" gets used loosely in conversation for any step-by-step doc. In this doc set it is
reserved for the Crash Course specifically. A getting-started quickstart, an install guide, or a
walkthrough under `how-to-guides/` is a **how-to guide**, not a tutorial. Classify by directory
first, and if that's ambiguous, by outcome: does every reader end up in the same place (tutorial)
or does the reader supply their own goal/data (how-to guide)? — not by the presence of numbered
steps, which both categories use.

**Sidebar section is a navigational grouping, not the category signal.** `sidebar.json` groups
pages by where a reader would look for them, which sometimes cuts across category (see the
Parallelization/Engine locking exception above). Classify a page by its directory
(`conceptual/` vs. `how-to-guides/` vs. `reference/` vs. `crash-course/`) and what it actually
does, then use the sidebar only to note which User Guide subsection it's discoverable from — never
let the subsection name override the directory-based category.

## Audience calibration

All *published* deephaven-core docs under `docs/{python,groovy}` are public/external — this doc
set has no internal-only tier. Concept and Reference pages can assume more from the reader (using
internal/technical vocabulary once it's been defined) than the Crash Course, which is written for
a first-time user with zero context. Define jargon on first use in every category; how much you
can lean on that definition later depends on the category above.

This external-audience assumption does not extend to the contributor-facing tooling docs listed
under **Pages outside the four categories** above — those are written for repo contributors, not
a deephaven.io reader.
