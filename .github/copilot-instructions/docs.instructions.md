---
applyTo:
  - "docs/**/*.md"
---

# Documentation Style Guidelines

When reviewing Markdown files in the `docs/` directory, check for the following:

## Headings

- Use sentence case (capitalize only the first word and proper nouns).
- Do NOT include links in headers.

## Method names in prose

- Do NOT include parentheses after method names in prose text.
- ✅ Correct: "Use `where` to filter rows"
- ❌ Incorrect: "Use `where()` to filter rows"
- Exception: Parentheses are fine in code blocks and when showing signatures

## Backticks

- Enclose in backticks: method names (`naturalJoin`), classes (`SystemTableLogger`), variables (`t`), and file paths (`/tmp/etcd.snap`).

## Bullet point punctuation

- Complete sentences (subject + verb) should end with a period.
- Fragments or phrases should NOT have periods.
- ✅ "Use `double` for most cases." (complete sentence)
- ✅ "Prefer primitive types over objects." (complete thought)
- ✅ "High-precision math" (fragment shouldn't have period)

## General style

- Prefer active voice.
- Avoid "will": "The system will process data in the order it is received" becomes "The system processes data in the order it is received".
- Link to related documentation when referencing other features.
