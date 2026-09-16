---
name: deephaven-writing-style
description: Deephaven's documentation style guide for deephaven-core — proper noun capitalization, Python/Groovy code formatting conventions, backtick usage, code example tags, and prose quality standards (active voice, clarity, jargon/audience calibration). Applies to deephaven-core (Community) documentation specifically. Use this whenever drafting, writing, editing, or reviewing any deephaven-core documentation, tutorial, how-to guide, README, or API reference — not just when explicitly asked about "style." Also consult this alongside deephaven-core-accuracy-check and deephaven-doc-structure-review when reviewing existing docs (deephaven-core-accuracy-spot-check for a single small edit instead), or use deephaven-docs-review-full to run accuracy, structure, and style together in the right order; see ref-deephaven-doc-categories for the Tutorial/How-to/Concept/Reference categories this guide's tone rules are calibrated to.
---

# Deephaven documentation style guide (Community/Core)

These standards apply to deephaven-core documentation.

## Documentation categories

Read `ref-deephaven-doc-categories` and identify which of the four categories (Tutorial — Crash
Course only, How-to guide, Concept guide, Reference guide) the doc is — that file has the
directory rule for each, the misclassification trap ("tutorial" is not a synonym for
"step-by-step"), and the pages that don't fit any of the four (currently just the site's landing
page). Do this before applying the tone rules below — they're calibrated per category.

## Prose quality

- **Prefer present, active voice.** Avoid future-tense "will". Flag passive constructions and suggest an active rewrite unless the actor is genuinely unknown or irrelevant (e.g., "the file is created" only when who/what creates it doesn't matter to the reader).
- **Define jargon and internal terms on first use.** Terms like "ticking," "blink table," "live table," or internal service/component names should be defined in plain language or linked to a reference page the first time they appear in a doc — don't assume the reader already knows them.
- **Calibrate to the audience.** All *published* `docs/{python,groovy}` content is external-facing (deephaven.io) — none of it is an internal-only tier — but how much you can lean on internal vocabulary once it's defined still varies by category (see `ref-deephaven-doc-categories`): the Crash Course assumes zero prior context, Concept/Reference pages can assume more. Avoid unexplained internal-only vocabulary (internal service names, internal abbreviations, implementation details that don't matter to the reader) regardless of category. This external-audience assumption does **not** extend to contributor-facing tooling docs that happen to live under `docs/` but aren't published (e.g. `docs/README.md`, `docs/snapshotter/README.md`) — those are written for repo contributors and may freely use internal tooling vocabulary, script names, and implementation detail.
- **Avoid egregious jargon and hedging.** Prefer concrete, direct sentences over vague qualifiers ("may potentially," "in some cases could") unless the uncertainty is real and worth flagging.
- **Tone.** Tutorials and how-tos can be conversational, first-person narrative while remaining professional. Reference material is dry and formal — third-person narrative without contractions.
- **Sentence case in headings** — not Title Case. Don't include links in headers.
- **Straight quotes only.** Use `"` and `'`, never smart/curly quotes (`“` `”` `‘` `’`).
- **Em dashes** for parenthetical statements, not hyphens or en dashes. Surround with a single space on either side: `word — word`, not `word—word`.
- **Link wording.** Always describe what you're linking to; never use "here" or "click here" as link text (e.g., "see the [Input table guide](link)," not "see [here](link)").
- **One idea per paragraph.** Long paragraphs mixing multiple claims are harder to verify and harder to read; split them.
- **Bullet points get periods** when they're complete sentences; incomplete phrases don't need them. Exception: don't add periods to bullets in the "Related documentation" section.

## Page structure

- Every page (except landing pages, overviews, blog articles, or a Crash Course tutorial chapter — see `ref-deephaven-doc-categories` — none of which carry one today) should include a "Related documentation" section at the end.
- When a method is referenced in narrative text, link it to the appropriate reference page if one exists.

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

**Method names in prose:** No leading dot and no parentheses in prose, only in code.

- Correct prose: "Use `with_serial` when your formula has side effects"
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
- Search for backticked method-shaped identifiers (`snake_case` or `camelCase`, especially ones
  matching `with_`, `is_`, `from_`, `agg_`, `update`, `select`, `where`, etc.) and check the
  **first** occurrence of each in the file, not just whether a link exists anywhere — a doc whose
  first mention is bare and a later mention is linked still violates "first mention should link,"
  even though a plain existence check would pass it. Flag any identifier whose first occurrence is
  bare; the fix is to move the link to that first mention, not to add one anywhere in the file.
- Search for a backticked identifier immediately followed by `()` outside of a fenced code block
  (e.g. `` `with_serial()` `` in prose) — flag it; parentheses belong in code, not prose (see
  **Method names in prose** above).
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

Enclose: method names (`naturalJoin`), classes (`SystemTableLogger`), variables (`t`), file paths (`/tmp/etcd.snap`).

## Code example tags

- `syntax` — Show syntax without executing
- `should-fail` — Reserved for a block that shouldn't run because it's broken; currently behaves identically to `skip-test` (not executed), so don't describe it as verifying a failure. Use sparingly.
- `order=table1,table2` — Specify output table order
- `order=null` — No output to display
- `order=:log` — Show log/print output
- `skip-test` — Skip snapshot testing
- `test-set=name` — Group code blocks as sequential test
- `ticking-table` — Mark as containing ticking tables; also use `order=null` unless the example intentionally tests named, log, or failing output
