---
name: deephaven-writing-style
description: >
  Deephaven's documentation style guide for deephaven-core — proper noun capitalization, Python/Groovy code formatting conventions, backtick usage, code example tags, and prose quality standards (active voice, clarity, jargon/audience calibration). **Use this skill when:** someone asks to check style, review formatting, fix prose issues, check capitalization, review code examples for naming conventions, check for passive voice, or asks about em dashes, backticks, link text, or method names in prose. Also use when writing or editing any deephaven-core doc, tutorial, how-to guide, README, or API reference — not just when explicitly asked about "style." Pair with deephaven-core-accuracy-check and deephaven-doc-structure-review for a full review, or use deephaven-docs-review-full to run all three together. **Do NOT use for:** accuracy/fact-checking (use deephaven-core-accuracy-check), document organization (use deephaven-doc-structure-review), or Enterprise/deephaven-ent docs.
---

# Deephaven documentation style guide (Community/Core)

These standards apply to deephaven-core documentation.

## Documentation categories

Read `ref-deephaven-doc-categories` and identify which of the four categories (Tutorial — Crash
Course only, How-to guide, Concept guide, Reference guide) the doc is — that file has the
directory rule for each, the misclassification trap ("tutorial" is not a synonym for
"step-by-step"), and the pages that don't fit any of the four (see its "Pages outside the four
categories" section — the site's landing page and contributor-facing tooling docs, not just the
former). For an out-of-taxonomy page, skip the per-category tone/audience calibration below
entirely; the prose-quality and formatting rules still apply. For a page that does fit one of the
four, do the category identification before applying the tone rules below — they're calibrated
per category.

## Prose quality

- **Prefer present, active voice.** Avoid future-tense "will". Flag passive constructions and suggest an active rewrite unless the actor is genuinely unknown or irrelevant (e.g., "the file is created" only when who/what creates it doesn't matter to the reader).
- **Define jargon and internal terms on first use.** Terms like "ticking," "blink table," "live table," or internal service/component names should be defined in plain language or linked to a reference page the first time they appear in a doc — don't assume the reader already knows them.
- **Prefer standard terminology over coined phrases.** Defining a term isn't enough if a recognized one already exists — choose the term a reader could search for or already knows from computer science or the wider industry. Say "pure function," not "a function that only does math." Where no recognized term fits, prefer a phrase that describes itself over a terse label the reader has to decode: "concurrent row calculations," not "across rows." Don't invent a new name for something the codebase or the rest of the docs already names — search both before settling on a term. Test a phrase by asking whether it would mean the same thing to a reader who hasn't read the rest of the page. Once you've picked a term, use it consistently: don't alternate between near-synonyms that aren't actually synonyms (e.g. "stateless" and "thread-safe" describe different properties — using them interchangeably tells the reader they're the same thing).
- **Calibrate to the audience.** All *published* `docs/{python,groovy}` content is external-facing (deephaven.io) — none of it is an internal-only tier — but how much you can lean on internal vocabulary once it's defined still varies by category (see `ref-deephaven-doc-categories`): the Crash Course assumes zero prior context, Concept/Reference pages can assume more. Avoid unexplained internal-only vocabulary (internal service names, internal abbreviations, implementation details that don't matter to the reader) regardless of category. This external-audience assumption does **not** extend to contributor-facing tooling docs that happen to live under `docs/` but aren't published (e.g. `docs/README.md`, `docs/snapshotter/README.md`) — those are written for repo contributors and may freely use internal tooling vocabulary, script names, and implementation detail.
- **Avoid egregious jargon and hedging.** Prefer concrete, direct sentences over vague qualifiers ("may potentially," "in some cases could") unless the uncertainty is real and worth flagging. The same goes for mid-sentence parentheticals that carry a caveat, a property name, or an exception: if the sentence reads fine without the parenthetical, the detail doesn't belong wedged into the explanation. Where it goes depends on what it is. A caveat or exception the reader needs at that point can become its own sentence. A configuration property name or default can't: in a Concept guide or Tutorial, moving it to a separate sentence in the same paragraph is still configuration injection. Put it in the page's Configuration section or behind a link to the configuration reference, and when you propose a rewrite, don't reintroduce the property anywhere in the narrative (see `deephaven-core-accuracy-check`'s **Placement of configuration detail** and `deephaven-doc-structure-review`'s **Level of abstraction** check). Reference guides and configuration pages are the exception — there, the property is the content.
- **Tone.** Tutorials and how-tos can be conversational, first-person narrative while remaining professional. Reference material is dry and formal — third-person narrative without contractions — except individual `reference/community-questions/*` Q&A pages, which are a conversational Q&A format and take the conversational how-to tone instead (identify these by directory and content shape — a single question answered conversationally — not by assuming the question is in any specific front-matter field or that the body opens with it; see `ref-deephaven-doc-categories` for why). `cq-index.md` itself is the exception to that exception — a category-card index page, not a Q&A — and keeps the dry/formal Reference tone (see `ref-deephaven-doc-categories` for the full carve-out).
- **Sentence case in headings** — not Title Case. Don't include links in headers.
- **Straight quotes only.** Use `"` and `'`, never smart/curly quotes (`“` `”` `‘` `’`).
- **Em dashes** for parenthetical statements, not hyphens or en dashes. Surround with a single space on either side: `word — word`, not `word—word`. This rule governs how a dash is written, not whether to use one; see **Sentences that need rescuing punctuation** below before putting more than one dash construction in a sentence (a pair of dashes enclosing one aside counts as one construction), or a dash construction together with a colon that doesn't introduce a list or code block.
- **Link wording.** Always describe what you're linking to; never use "here" or "click here" as link text (e.g., "see the [Input table guide](link)," not "see [here](link)").
- **One idea per paragraph.** Long paragraphs mixing multiple claims are harder to verify and harder to read; split them.
- **Walls of text.** Two or more consecutive paragraphs of three or more sentences that explain a mechanism (what a setting does, how an ordering works, what happens by default) read as a wall even when every sentence is clear. Restructure them: a one-sentence definition, then short lists (what happens when it's on, what the default means, when to change it), then at most one sentence of guidance. Prose that a reader would skim in a well-edited technical book passes; prose they would reread doesn't.
- **Qualifiers with no concrete referent.** A trailing phrase that sounds informative but names nothing checkable ("with no additional configuration," "out of the box," "seamlessly," "automatically handles") makes a reader ask what it means. Ask that question yourself: if the answer is a specific fact (no property to set, no import needed), state that fact; otherwise cut the phrase.
- **Sentences that need rescuing punctuation.** A sentence or paragraph that is hard to parse on one read is a readability defect. Punctuation is only a clue to inspect: using a colon, an em dash, a parenthetical, or a semicolon is normal and is not itself a problem, but when a clause only holds together because of one of them, or several pile up in one sentence, the reader has to work out the structure before the meaning. Drafted prose, especially AI-drafted prose, tends to pile this structure up. A short definition that uses a colon and reads easily is not a finding. Read each paragraph once as a reader would; if you have to re-read a sentence to find where it ends, or a clause only works because a dash or semicolon holds it together, flag it. Typical shapes: a definition packed into one sentence with a colon, bold terms, and a semicolon; an opener that ends in a dash and then a colon (`When X is enabled — Y happens:`); a "name: A does this; B does that; C does the other" run; and a paragraph that opens "X needs no Y. These Z tune it; …".
  - **Fix:** split into short sentences, one claim each. When the sentence enumerates cases, steps, or parallel items, use a bullet list instead of a run-on with semicolons (one bullet per case).
  - **Not defects:** a colon that introduces a list or a code block; a run-in bold label such as `**Updates**: After initialization, …` when the text after it is plain sentences; a single em dash around a real aside the sentence reads fine without.
  - **Report it as one pattern** with the two or three clearest examples, then list the remaining locations. Give a proposed rewrite for every flagged paragraph (a flag with no rewrite leaves the author to guess), and don't reintroduce a semicolon, a dash aside, or a parenthetical aside in it; the rewrite is the test.
- **One idea per bullet.** A bullet states one point. A second sentence that states a different idea (not an elaboration, cause, or consequence of the first) belongs in its own bullet or outside the list. Test: can you give the bullet a short label that covers everything in it? A bullet that explains a rule and then adds "this also works for Y" fails; a bullet that states a cause and then its consequence passes. Apply the same test to "Key takeaways" bullets: split a multi-sentence bullet only when its sentences state different ideas, and leave one that explains or qualifies a single point.
- **Bullet points get periods** when they're complete sentences; incomplete phrases don't need them. Exception: don't add periods to bullets in the "Related documentation" section.

## Page structure

- Every published `docs/{python,groovy}` page (except landing pages, overviews, blog articles, a Crash Course tutorial chapter, the four root quickstarts, or an individual `reference/community-questions/*` Q&A page — see `ref-deephaven-doc-categories`) should include a "Related documentation" section at the end. The quickstart and community-questions exemptions reflect actual, established repo convention, verified by corpus count, not an assumption: none of the four root quickstarts (`getting-started/quickstart.md`, `pyclient-quickstart.md`, `jupyter-quickstart.md`) carry the section, and only 9 of the ~99 individual community-questions pages across both languages do — so flagging the other ~90 would mean flagging the repo's own established norm, not a real defect. This rule doesn't apply at all to the out-of-taxonomy contributor-facing tooling docs (`docs/README.md`, `docs/snapshotter/README.md`, `docs/tools/autoimport/README.md`, etc. — see `ref-deephaven-doc-categories`'s "Pages outside the four categories") — none of those carry the section, and that's not a defect to flag. Every exemption here means the section isn't *required* on those pages, not that it's *forbidden* — some exempted pages include one anyway (e.g. `conceptual/table-operations-overview.md`, or the 9 community-questions pages that have one), which is fine; don't flag its presence as a violation of the exemption.
- When a method or type is referenced in narrative text, link it to the appropriate reference page if one exists. Link the **first mention in each paragraph, list item, and table**, not only the first mention in the file or in the section: readers scan, land mid-section from a heading link or a search, and read a list or a callout on its own, so an identifier linked one paragraph or one bullet up is bare where they are. A bold lead term in a list item (**`sort_descending`** ...) is a mention and gets the link. In summary blocks (Key takeaways, a quick-reference table, a list of supported operations), link every API name, because readers use those blocks as a jump table. Within one paragraph or list item, link only the first occurrence, and don't link inside headings, fenced code blocks, or code comments. Don't ask a page to link an identifier to itself: a reference page that documents `view` leaves `view` plain, so skip any identifier whose link target is the page under review. For each bare identifier you flag, give the link target (the reference page path and anchor already used elsewhere on the page, or the page you found by searching), so the author can apply the fix without looking it up. In a Concept guide, check the opening paragraph and bullets of each major section first, and every sentence that enumerates operations or types, because that is where several identifiers tend to appear together and bare.

## Deephaven proper nouns

Capitalize:

- Deephaven Community
- Core+ (if referenced)
- Enterprise (if referenced)
- Persistent Query (if referenced)
- UpdateGraph
- TableUpdateListener
- RowSet
- ColumnSource
- ScriptSession
- Barrage

## Code formatting

**Python:** Follow [PEP 8 naming conventions](https://peps.python.org/pep-0008/#naming-conventions).

- `snake_case` for variables (including tables) and functions.
- `PascalCase` for classes and type variables.
- Avoid full imports: `from deephaven import time_table` not `from deephaven import *`

**Groovy:** Follow [Oracle naming conventions](https://www.oracle.com/java/technologies/javase/codeconventions-namingconventions.html).

- `camelCase` for variables (including tables) and methods.
- `PascalCase` for classes.

**General:**

- Column names start with capitals: `"NewColumn"`, `"StringColumn"`
- Write out "column": `"columnToMatch"`, `"sourceColumn"`
- Use "parameter"/"argument" for function arguments; "method"/"function" for functions.
- Varargs: `String...`
- True/false: `boolean`
- Whitespace for readability: `"A = 4"` not `"A=4"`
- Null: prose = "will not include null values"; parameter descriptions = `NULL`; code = language-appropriate null.

**Method names in prose:** No leading dot in prose, only in code. Empty parentheses add no value in
prose, so a bare method reference never carries them. Parentheses *with* an argument are allowed
when the argument itself conveys useful information to the reader (e.g. `isNaN(value)` shows what's
being tested) — that's a small usage example, not just a method name, so the empty-parens rule
doesn't apply to it.

- Correct prose: "Use `with_serial` when your formula has side effects"
- Correct prose: "Use `isNaN(value)` to explicitly test for `NaN`"
- Correct code: `col.with_serial()`
- Incorrect prose: "Use `.with_serial` when your formula has side effects" or "Use `with_serial()` when your formula has side effects"

## Mechanical verification

Run these as literal Grep searches when reviewing a doc — don't rely on catching them by eye.
These specific mistakes have recurred across many reviews of this doc set, so treat them as
required searches, not optional style intuition:

- Search for `` `\.[a-z] `` (backtick, dot, lowercase letter) in the file. For every hit, confirm
  it's a genuine file extension or config key (`.parquet`, `.env`, `.yml`) and not a
  method/property reference in prose — a bare method name with **no leading dot** is this repo's
  actual convention (confirmed by corpus frequency: hundreds of bare mentions of
  `where`/`update`/`with_serial`/etc. vs. only isolated dot-prefixed outliers, each traceable to a
  specific bug). Flag every dot-prefixed method reference in prose (e.g. `.with_serial`, `.where`)
  for correction — see **Method names in prose** above.
- Search for backticked type names too (`PascalCase` class and interface names) and apply the same per-paragraph check to them.
- Search for backticked method-shaped identifiers (`snake_case` or `camelCase`, especially ones
  matching `with_`, `is_`, `from_`, `agg_`, `update`, `select`, `where`, etc.) and, **only for
  those that have an appropriate reference page or pydoc/javadoc anchor to link to** (per the
  Page structure rule above — this check doesn't apply if no suitable target exists), check the
  first occurrence of each in every paragraph, list item, and table (and every occurrence in a summary
  block), not just whether a link exists anywhere. A doc whose first mention is bare and a later mention
  is linked still violates "first mention should link," even though a plain existence check would
  pass it, and an identifier linked in the page's introduction but bare in the opening of a later
  paragraph or list item violates the rule. List each identifier's first mention per paragraph or list item
  with its line number and whether it is linked; flag the bare ones. The fix is to link that
  mention, not every occurrence within the same paragraph.
- Search for qualifier phrases that may name nothing: `with no `, `without any`, `no additional`, `out of the box`, `seamless`, `automatically`, `simply`, `just works`. For each hit, ask what concrete fact it states (see **Qualifiers with no concrete referent**); flag it if the answer is nothing a reader could check, and give the fact to state instead or say to cut the phrase.
- For each `##` section and each `###` or `####` subsection, count the sentences in each paragraph. Flag any run of two or more consecutive paragraphs with three or more sentences that explain how something works (see **Walls of text**), and propose the definition-plus-lists structure in the fix.
- Search for `[Cc]olumns? [A-Z]` and for capitalized single-word names next to "column" or
  "columns" in prose (for example `column A`, `Column B gets`). Then build a list of the column
  names the page defines (the left-hand sides of formulas such as `"A = i * 2"`, and explicit
  labels in code) and search the prose for each of those names used alone or in a coordinated
  phrase, such as `A and B run in parallel` or `D starts after A finishes`. For every hit outside
  a fenced code block and outside a heading, check whether the name is backticked; flag the bare
  ones.
- Search for a backticked identifier immediately followed by empty `()` outside of a fenced code
  block (e.g. `` `with_serial()` `` in prose) — flag it; empty parentheses add no value in prose.
  However, parentheses *with* arguments (e.g. `` `isNaN(value)` ``) are acceptable when the
  argument conveys useful information to the reader. See **Method names in prose** above.
- Search for the literal *markdown link label* `[here]`, `[click here]`, or `[this page]`
  (case-insensitive — the brackets matter: this targets link syntax, not ordinary prose like
  "This page explains...") — the **Link wording** rule above bans non-descriptive link text; flag
  every instance for a replacement that names its destination.
- **If you're unsure whether a pattern is actually "the project standard"** (including when a
  prior comment or your own assumption asserts one), don't trust the assertion alone — verify by
  counting real occurrences of both forms across `docs/python` and `docs/groovy` (e.g. `grep -rc`
  for each candidate form). A stated convention — including one written into this skill — can
  itself be wrong; corpus frequency is the actual authority.

**Python vs Groovy:**

| Python                     | Groovy                   |
| -------------------------- | ------------------------ |
| `with_serial`               | `withSerial`             |
| `with_declared_barriers`    | `withDeclaredBarriers`   |
| `with_respected_barriers`   | `withRespectedBarriers`  |
| `Filter.from_`             | `Filter.from`            |

In a small number of cases, method names may differ from the snake_case/camelCase translation, so it is worth double-checking that method names are valid when translating between Python and Groovy.

## Backticks

Enclose: method names (`naturalJoin`), classes (`SystemTableLogger`), variables (`t`), file paths (`/tmp/etcd.snap`), and **column names in prose** ("column `A` finishes before column `B`", "the `Price` column"). Corpus count on 2026-10-07 across `docs/python`: about 509 backticked column names in prose (`` column `Name` ``, `` `Name` column ``) against about 59 bare ones, and the bare ones are mostly headings and UI labels, so backticking is the standard. Don't change column names inside fenced code blocks or code comments, and don't backtick the generic word "column".

## Code example tags

- `syntax` — Show syntax without executing
- `should-fail` — Reserved for a block that shouldn't run because it's broken; currently behaves identically to `skip-test` (not executed), so don't describe it as verifying a failure. Use sparingly.
- `order=table1,table2` — Specify the order of output objects (tables, plots, and other widgets). List only objects that this block creates. When blocks share a `test-set` and a setup block creates objects that later blocks reuse, don't list those objects in the later blocks' `order=` tags: the validator fails with "specified in an order string, but not generated by the snapshot."
- `order=null` — No output to display
- `order=:log` — Show log/print output
- `skip-test` — Skip snapshot testing
- `test-set=name` — Group code blocks as sequential test
- `ticking-table` — Mark as containing ticking tables; also use `order=null` unless the example intentionally tests named, log, or failing output
