---
name: deephaven-writing-style
description: Deephaven's documentation style guide for deephaven-core — proper noun capitalization, Python/Groovy code formatting conventions, backtick usage, code example tags, and prose quality standards (active voice, clarity, jargon/audience calibration). Applies to deephaven-core (Community) documentation specifically. Use this whenever drafting, writing, editing, or reviewing any deephaven-core documentation, tutorial, how-to guide, README, or API reference — not just when explicitly asked about "style." Also consult this alongside deephaven-core-accuracy-check and deephaven-doc-structure-review when reviewing existing docs; see ref-deephaven-doc-categories for the Tutorial/How-to/Concept/Reference categories this guide's tone rules are calibrated to.
---

# Deephaven documentation style guide (Community/Core)

These standards apply to deephaven-core documentation.

## Documentation categories

See `ref-deephaven-doc-categories` for the four categories (Tutorial — Crash Course only,
How-to guide, Concept guide, Reference guide), which directory each lives in, and the
misclassification trap ("tutorial" is not a synonym for "step-by-step"). Identify the doc's
category before applying the tone rules below — they're calibrated per category.

## Prose quality

- **Prefer present, active voice.** Avoid future-tense "will". Flag passive constructions and suggest an active rewrite unless the actor is genuinely unknown or irrelevant (e.g., "the file is created" only when who/what creates it doesn't matter to the reader).
- **Define jargon and internal terms on first use.** Terms like "ticking," "blink table," "live table," or internal service/component names should be defined in plain language or linked to a reference page the first time they appear in a doc — don't assume the reader already knows them.
- **Calibrate to the audience.** All `docs/` content is external-facing (deephaven.io) — none of it is an internal-only tier — but how much you can lean on internal vocabulary once it's defined still varies by category (see `ref-deephaven-doc-categories`): the Crash Course assumes zero prior context, Concept/Reference pages can assume more. Avoid unexplained internal-only vocabulary (internal service names, internal abbreviations, implementation details that don't matter to the reader) regardless of category.
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
