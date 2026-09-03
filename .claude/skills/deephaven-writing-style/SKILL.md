---
name: deephaven-writing-style
description: Deephaven's documentation style guide for deephaven-core — proper noun capitalization, Python/Groovy code formatting conventions, backtick usage, code example tags, and prose quality standards (active voice, clarity, jargon/audience calibration). Applies to deephaven-core (Community) documentation specifically. Use this whenever drafting, writing, editing, or reviewing any deephaven-core documentation, tutorial, how-to guide, README, or API reference — not just when explicitly asked about "style." Also consult this alongside deephaven-core-accuracy-check when reviewing existing docs.
---

# Deephaven Documentation Style Guide (Community/Core)

These standards apply to deephaven-core documentation.

## Prose Quality

- **Prefer active voice.** Flag passive constructions and suggest an active rewrite unless the actor is genuinely unknown or irrelevant (e.g., "the file is created" only when who/what creates it doesn't matter to the reader).
- **Define jargon and internal terms on first use.** Terms like "ticking," "blink table," "live table," or internal service/component names should be defined in plain language or linked to a reference page the first time they appear in a doc — don't assume the reader already knows them.
- **Calibrate to the audience.** External-facing docs (deephaven.io, public tutorials) should avoid unexplained internal-only vocabulary (internal service names, internal abbreviations, implementation details that don't matter to the reader). Internal/contributor-facing docs can assume more shared context, but still define anything genuinely obscure.
- **Avoid egregious jargon and hedging.** Prefer concrete, direct sentences over vague qualifiers ("may potentially," "in some cases could") unless the uncertainty is real and worth flagging.
- **Sentence case in headings** — not Title Case.
- **One idea per paragraph.** Long paragraphs mixing multiple claims are harder to verify and harder to read; split them.
- **Bullet points get periods** when they're complete sentences; incomplete phrases don't need them.

## Deephaven Proper Nouns

Capitalize:

- Deephaven Community
- Core+ (if referenced)
- UpdateGraph
- TableUpdateListener
- RowSet
- ColumnSource
- ScriptSession
- Barrage

## Code Formatting

**Python (PEP 8):**

- `snake_case` for variables (including tables) and functions.
- `PascalCase` for classes and type variables.
- Avoid full imports: `from deephaven import time_table` not `from deephaven import *`

**Groovy (Oracle conventions):**

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

**Method names in prose:** No parentheses in prose, only in code.

- Correct prose: "Use `.with_serial` when your formula has side effects"
- Correct code: `col.with_serial()`
- Incorrect prose: "Use `.with_serial()` when your formula has side effects"

**Python vs Groovy:**

| Python                     | Groovy                   |
| -------------------------- | ------------------------ |
| `.with_serial`             | `.withSerial`            |
| `.with_declared_barriers`  | `.withDeclaredBarriers`  |
| `.with_respected_barriers` | `.withRespectedBarriers` |
| `Filter.from_`             | `Filter.from`            |

## Backticks

Enclose: method names (`naturalJoin`), classes (`SystemTableLogger`), variables (`t`), file paths (`/tmp/etcd.snap`).

## Code Example Tags

- `syntax` — Show syntax without executing
- `should-fail` — Code that should execute but fail with an error
- `order=table1,table2` — Specify output table order
- `order=null` — No output to display
- `order=:log` — Show log/print output
- `skip-test` — Skip snapshot testing
- `test-set=name` — Group code blocks as sequential test
- `ticking-table` — Mark as containing ticking tables
