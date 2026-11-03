#!/usr/bin/env python3
"""Generate the Named colors reference pages from Color.java.

Reads every public Color constant in
engine/table/src/main/java/io/deephaven/gui/color/Color.java and writes
docs/python/reference/table-operations/format/named-colors.md and
docs/groovy/reference/table-operations/format/named-colors.md.

Run from the repository root:

    python3 docs/tools/named-colors/generate_named_colors.py

Then run ./docs/format and ./docs/updateSnapshots.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
COLOR_JAVA = ROOT / "engine/table/src/main/java/io/deephaven/gui/color/Color.java"
# Color(String) looks names up in this enum, which spells a few legacy names differently.
COLORS_JAVA = ROOT / "engine/table/src/main/java/io/deephaven/gui/color/Colors.java"
OUT = "docs/{lang}/reference/table-operations/format/named-colors.md"

# NO_FORMATTING is a Color constant, but it means "no color", so it isn't listed.
SKIP = {"NO_FORMATTING"}

CONSTANT = re.compile(r"public static (?:final )?Color (\w+)\s*=\s*([^;]+);")
RGB = re.compile(r"colorRGB\((\d+),\s*(\d+),\s*(\d+)\)")
HEX = re.compile(r'"#([0-9A-Fa-f]{6})"')
ENUM_NAME = re.compile(r"^\s{4}([A-Z][A-Z0-9_]*)\(", re.M)
ENUM_RGB = re.compile(r"^\s{4}([A-Z][A-Z0-9_]*)\(\s*(\d+),\s*(\d+),\s*(\d+)\s*\)", re.M)


def read_colors():
    colors = {}
    for name, expr in CONSTANT.findall(COLOR_JAVA.read_text()):
        if name in SKIP:
            continue
        rgb = RGB.search(expr)
        hexval = HEX.search(expr)
        if rgb:
            r, g, b = (int(v) for v in rgb.groups())
            colors[name] = f"#{r:02X}{g:02X}{b:02X}"
        elif hexval:
            colors[name] = "#" + hexval.group(1).upper()
        else:
            sys.exit(f"Can't read the value of {name}: {expr}")
    return dict(sorted(colors.items()))


def string_names(colors):
    """Map each constant to the name Color(String) accepts, when that name differs."""
    text = COLORS_JAVA.read_text()
    enum_names = set(ENUM_NAME.findall(text))
    enum_rgb = {n: f"#{int(r):02X}{int(g):02X}{int(b):02X}" for n, r, g, b in ENUM_RGB.findall(text)}
    renamed = {}
    for name, hexval in colors.items():
        if name in enum_names:
            continue
        matches = [e for e, v in enum_rgb.items() if v == hexval and e not in colors]
        if len(matches) != 1:
            sys.exit(f"{name} has no single matching Colors enum name: {matches}")
        renamed[name] = matches[0]
    return renamed


INTRO = {
    "python": """\
Deephaven defines {count} named colors. Each one is a constant of the [`Color`](/core/javadoc/io/deephaven/gui/color/Color.html) class, so you can use its name directly in a formatting formula, such as `"Score = bg(BRIGHT_GREEN)"` in [`format_columns`](./format-columns.md). Wherever Deephaven takes a color as a string, such as a plot's `color` argument or [`Color.of_name`](/core/pydoc/code/deephaven.plot.color.html#deephaven.plot.color.Color.of_name), you can pass the name instead, in any case.{exceptions}""",
    "groovy": """\
Deephaven defines {count} named colors. Each one is a constant of the [`Color`](/core/javadoc/io/deephaven/gui/color/Color.html) class, so you can use its name directly in a formatting formula, such as `"Score = bg(BRIGHT_GREEN)"` in [`formatColumns`](./format-columns.md). Wherever Deephaven takes a color as a string, such as a plot's color methods or [`Color.color`](</core/javadoc/io/deephaven/gui/color/Color.html#color(java.lang.String)>), you can pass the name instead, in any case.{exceptions}""",
}

EXAMPLE = {
    "python": """\
```python order=scores_colored,scores
from deephaven import new_table
from deephaven.column import int_col, string_col

scores = new_table(
    [
        string_col("Name", ["Ana", "Ben", "Cai"]),
        int_col("Score", [92, 71, 85]),
    ]
)

scores_colored = scores.format_columns(
    ["Score = Score >= 90 ? BRIGHT_GREEN : Score >= 80 ? BRIGHT_YELLOW : BRIGHT_RED"]
)
```""",
    "groovy": """\
```groovy order=scoresColored,scores
scores = newTable(
    stringCol("Name", "Ana", "Ben", "Cai"),
    intCol("Score", 92, 71, 85)
)

scoresColored = scores.formatColumns(
    "Score = Score >= 90 ? BRIGHT_GREEN : Score >= 80 ? BRIGHT_YELLOW : BRIGHT_RED"
)
```""",
}

RELATED = {
    "python": """\
- [Format columns](../../../how-to-guides/format-columns.md)
- [`format_columns`](./format-columns.md)
- [`format_column_where`](./format-column-where.md)
- [`format_row_where`](./format-row-where.md)
- [GUI functions](../../query-language/query-library/auto-imported/gui.md)
- [Javadoc](/core/javadoc/io/deephaven/gui/color/Color.html)
- [Pydoc](/core/pydoc/code/deephaven.plot.color.html)""",
    "groovy": """\
- [Format columns](../../../how-to-guides/format-columns.md)
- [`formatColumns`](./format-columns.md)
- [`formatColumnWhere`](./format-column-where.md)
- [`formatRowWhere`](./format-row-where.md)
- [GUI functions](../../query-language/query-library/auto-imported/gui.md)
- [Javadoc](/core/javadoc/io/deephaven/gui/color/Color.html)""",
}


def exceptions_sentence(renamed):
    if not renamed:
        return ""
    pairs = [f"`{s}` for `{n}`" for n, s in renamed.items()]
    listed = ", ".join(pairs[:-1]) + (", and " if len(pairs) > 2 else " and ") + pairs[-1] if len(pairs) > 1 else pairs[0]
    return f" The exceptions are {len(renamed)} legacy colors whose string names differ: pass {listed}. The table shows the string name next to each one."


def page(lang, colors, renamed):
    def name_cell(name):
        if name in renamed:
            return f"<code>{name}</code> (string: <code>{renamed[name]}</code>)"
        return f"<code>{name}</code>"

    rows = "\n".join(
        f"""    <tr>
      <td><span aria-hidden="true" style={{{{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '{hexval}'}}}} /></td>
      <td>{name_cell(name)}</td>
      <td><code>{hexval}</code></td>
    </tr>"""
        for name, hexval in colors.items()
    )
    return f"""---
title: Named colors
---

{INTRO[lang].format(count=len(colors), exceptions=exceptions_sentence(renamed))}

This list is generated from the `Color` class. [`NO_FORMATTING`](../../../how-to-guides/format-columns.md) is also a `Color` constant, but it means "apply no color", so it isn't listed here.

## Example

The following example colors each score by its value:

{EXAMPLE[lang]}

## Colors

<table>
  <thead>
    <tr>
      <th>Swatch</th>
      <th>Name</th>
      <th>Hex value</th>
    </tr>
  </thead>
  <tbody>
{rows}
  </tbody>
</table>

## Related documentation

{RELATED[lang]}
"""


def main():
    colors = read_colors()
    renamed = string_names(colors)
    for lang in ("python", "groovy"):
        out = ROOT / OUT.format(lang=lang)
        out.write_text(page(lang, colors, renamed))
        print(f"Wrote {len(colors)} colors to {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
