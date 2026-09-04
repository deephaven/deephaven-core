---
name: deephaven-writing-style
description: Deephaven's documentation style guide for deephaven-core — proper noun capitalization, Python/Groovy code formatting conventions, backtick usage, code example tags, and prose quality standards (active voice, clarity, jargon/audience calibration). Applies to deephaven-core (Community) documentation specifically. Use this whenever drafting, writing, editing, or reviewing any deephaven-core documentation, tutorial, how-to guide, README, or API reference — not just when explicitly asked about "style." Also consult this alongside deephaven-core-accuracy-check when reviewing existing docs.
---

# Deephaven documentation style guide (Community/Core)

These standards apply to deephaven-core documentation.

## Documentation categories

**How-to guide:** A follow-along resource that guides a reader step-by-step. These usually show new users how to solve a specific problem. Similar to a recipe, when following along with a how-to guide, all users will learn the same information, but they can use their own ingredients, so outcomes may differ. Advice or general recommendations on how to use a feature/method may be offered.

**Concept guide:** A resource to study. It is intended to broaden understanding of higher concepts and to explain how or why with contextual explanations. Overview pages may be considered concept guides.

**Reference guide:** A study resource. It is purely informative; its purpose is to describe without extraneous information — very pointed, without advice or general tips. JavaDocs, PyDocs, and other API documentation are reference guides.

## Prose quality

- **Prefer present, active voice.** Avoid future-tense "will". Flag passive constructions and suggest an active rewrite unless the actor is genuinely unknown or irrelevant (e.g., "the file is created" only when who/what creates it doesn't matter to the reader).
- **Define jargon and internal terms on first use.** Terms like "ticking," "blink table," "live table," or internal service/component names should be defined in plain language or linked to a reference page the first time they appear in a doc — don't assume the reader already knows them.
- **Calibrate to the audience.** External-facing docs (deephaven.io, public tutorials) should avoid unexplained internal-only vocabulary (internal service names, internal abbreviations, implementation details that don't matter to the reader). Internal/contributor-facing docs can assume more shared context, but still define anything genuinely obscure.
- **Avoid egregious jargon and hedging.** Prefer concrete, direct sentences over vague qualifiers ("may potentially," "in some cases could") unless the uncertainty is real and worth flagging.
- **Tone.** Tutorials and how-tos can be conversational, first-person narrative while remaining professional. Reference material is dry and formal — third-person narrative without contractions.
- **Sentence case in headings** — not Title Case. Don't include links in headers.
- **Straight quotes only.** Use `"` and `'`, never smart/curly quotes (`“` `”` `‘` `’`).
- **Em dashes** for parenthetical statements, not hyphens or en dashes. Surround with a single space on either side: `word — word`, not `word—word`.
- **Link wording.** Always describe what you're linking to; never use "here" or "click here" as link text (e.g., "see the [Input table guide](link)," not "see [here](link)").
- **One idea per paragraph.** Long paragraphs mixing multiple claims are harder to verify and harder to read; split them.
- **Bullet points get periods** when they're complete sentences; incomplete phrases don't need them. Exception: don't add periods to bullets in the "Related documentation" section.

## Page structure

- Every page (except landing pages, overviews, or blog articles) should include a "Related documentation" section at the end.
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
