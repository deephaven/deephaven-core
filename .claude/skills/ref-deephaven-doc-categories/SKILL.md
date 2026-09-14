---
name: ref-deephaven-doc-categories
description: Reference — the four deephaven-core documentation categories (Tutorial, How-to guide, Concept guide, Reference guide), which repo directory each lives in, and the tone/structure expectations tied to each. Loaded by deephaven-writing-style, deephaven-doc-structure-review, and deephaven-core-accuracy-check to calibrate their checks to the doc's actual category. Not invoked directly — there is no scenario where a human asks for this skill by itself.
user-invocable: false
---

# Deephaven documentation categories (Community/Core)

Identify a doc's category before applying any other doc skill's checks — tone, structure, and
depth expectations all depend on it. This is the single source of truth for the categories;
`deephaven-writing-style`, `deephaven-doc-structure-review`, and `deephaven-core-accuracy-check`
read this file rather than each defining categories independently.

## The four categories

**Tutorial** — `docs/{python,groovy}/getting-started/crash-course/*` only. The Crash Course is
the single tutorial in this doc set: a fixed, ordered sequence where every reader follows the
same path to the same outcome. No other page in the repo is a tutorial, regardless of how
step-by-step it reads — see **Common misclassification** below.
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
fill essentially every **User Guide** subsection other than Architectural details: Create/import/
export tables, Work with tables, Query strings, Python, Plot, GUI, Client APIs, Plugins,
Development tools, Best practices and troubleshooting, System setup and admin (informally "the
admin guide" — authentication, configuration, cloud deployment), Cloud deployment.
- Tone: conversational, first-person narrative; can offer advice, tips, or alternatives.
- Structure: goal-oriented; branching ("if you want X instead, do Y") is expected and fine here.

**Concept guide** — `docs/{python,groovy}/conceptual/*`. Broadens understanding of a higher-level
idea; explains how or why with contextual explanation. In `sidebar.json` these cluster heavily
under **User Guide → Architectural details** (Deephaven's design, Patterns of use, The Table API,
Incremental update model, Live DAG, Table types, Column types, Deephaven Vectors, Core API design,
Servers & clients, What is Barrage?) — but that clustering is a strong tendency, not the
definition, and two kinds of exception are common enough to expect, not treat as anomalies:
  - **Overview pages**, which count as concept guides even when they introduce a different section
    entirely rather than sitting in Architectural details — e.g. "Deephaven Overview" (site intro),
    "Crash Course Overview," "Table operations overview" (filed under Work with tables). Don't
    over-trust the word "overview" itself as the signal, though: `how-to-guides/overview-kafka.md`
    is titled "Kafka Overview" but is a how-to guide by directory and content (it walks through
    using Kafka, not the concept of streaming) — check the directory and what the page actually
    does, not just its title.
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
- Tone: dry, formal, third-person, no contractions.
- Structure: enumerable and scannable. A reference reader is scanning for one specific fact, not
  reading linearly — an "orphaned aside" or a missing entry in an enumerated list is a bigger
  defect here than the same issue would be in a concept guide.

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

All deephaven-core docs under `docs/` are public/external — this doc set has no internal-only
tier. Concept and Reference pages can assume more from the reader (using internal/technical
vocabulary once it's been defined) than the Crash Course, which is written for a first-time user
with zero context. Define jargon on first use in every category; how much you can lean on that
definition later depends on the category above.
