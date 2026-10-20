---
title: Named colors
---

Deephaven defines 279 named colors. Each one is a constant of the [`Color`](/core/javadoc/io/deephaven/gui/color/Color.html) class, so you can use its name directly in a formatting formula, such as `"Score = bg(BRIGHT_GREEN)"` in [`format_columns`](./format-columns.md). Wherever Deephaven takes a color as a string, such as a plot's `color` argument or [`Color.of_name`](/core/pydoc/code/deephaven.plot.color.html#deephaven.plot.color.Color.of_name), you can pass the name instead, in any case.

This list is generated from the `Color` class. [`NO_FORMATTING`](../../../how-to-guides/format-columns.md) is also a `Color` constant, but it means "apply no color", so it isn't listed here.

## Example

The following example colors each score by its value:

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
```

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
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#F0F8FF'}} /></td>
      <td><code>ALICEBLUE</code></td>
      <td><code>#F0F8FF</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FAEBD7'}} /></td>
      <td><code>ANTIQUEWHITE</code></td>
      <td><code>#FAEBD7</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#00FFFF'}} /></td>
      <td><code>AQUA</code></td>
      <td><code>#00FFFF</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#7FFFD4'}} /></td>
      <td><code>AQUAMARINE</code></td>
      <td><code>#7FFFD4</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#F0FFFF'}} /></td>
      <td><code>AZURE</code></td>
      <td><code>#F0FFFF</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#F5F5DC'}} /></td>
      <td><code>BEIGE</code></td>
      <td><code>#F5F5DC</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFE4C4'}} /></td>
      <td><code>BISQUE</code></td>
      <td><code>#FFE4C4</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#000000'}} /></td>
      <td><code>BLACK</code></td>
      <td><code>#000000</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFEBCD'}} /></td>
      <td><code>BLANCHEDALMOND</code></td>
      <td><code>#FFEBCD</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#0000FF'}} /></td>
      <td><code>BLUE</code></td>
      <td><code>#0000FF</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#8A2BE2'}} /></td>
      <td><code>BLUEVIOLET</code></td>
      <td><code>#8A2BE2</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#00939F'}} /></td>
      <td><code>BRIGHT_BLUE</code></td>
      <td><code>#00939F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#2B9759'}} /></td>
      <td><code>BRIGHT_BLUEGREEN</code></td>
      <td><code>#2B9759</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#58AB2D'}} /></td>
      <td><code>BRIGHT_GREEN</code></td>
      <td><code>#58AB2D</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#A9C723'}} /></td>
      <td><code>BRIGHT_GREENYELLOW</code></td>
      <td><code>#A9C723</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#B289A6'}} /></td>
      <td><code>BRIGHT_PURPLE</code></td>
      <td><code>#B289A6</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#3B829D'}} /></td>
      <td><code>BRIGHT_PURPLEBLUE</code></td>
      <td><code>#3B829D</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#E76C56'}} /></td>
      <td><code>BRIGHT_RED</code></td>
      <td><code>#E76C56</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#D1646D'}} /></td>
      <td><code>BRIGHT_REDPURPLE</code></td>
      <td><code>#D1646D</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFE40F'}} /></td>
      <td><code>BRIGHT_YELLOW</code></td>
      <td><code>#FFE40F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#F1B066'}} /></td>
      <td><code>BRIGHT_YELLOWRED</code></td>
      <td><code>#F1B066</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#A52A2A'}} /></td>
      <td><code>BROWN</code></td>
      <td><code>#A52A2A</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#DEB887'}} /></td>
      <td><code>BURLYWOOD</code></td>
      <td><code>#DEB887</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#5F9EA0'}} /></td>
      <td><code>CADETBLUE</code></td>
      <td><code>#5F9EA0</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#7FFF00'}} /></td>
      <td><code>CHARTREUSE</code></td>
      <td><code>#7FFF00</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#D2691E'}} /></td>
      <td><code>CHOCOLATE</code></td>
      <td><code>#D2691E</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FF7F50'}} /></td>
      <td><code>CORAL</code></td>
      <td><code>#FF7F50</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#6495ED'}} /></td>
      <td><code>CORNFLOWERBLUE</code></td>
      <td><code>#6495ED</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFF8DC'}} /></td>
      <td><code>CORNSILK</code></td>
      <td><code>#FFF8DC</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#DC143C'}} /></td>
      <td><code>CRIMSON</code></td>
      <td><code>#DC143C</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#00FFFF'}} /></td>
      <td><code>CYAN</code></td>
      <td><code>#00FFFF</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#00008B'}} /></td>
      <td><code>DARKBLUE</code></td>
      <td><code>#00008B</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#008B8B'}} /></td>
      <td><code>DARKCYAN</code></td>
      <td><code>#008B8B</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#B8860B'}} /></td>
      <td><code>DARKGOLDENROD</code></td>
      <td><code>#B8860B</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#A9A9A9'}} /></td>
      <td><code>DARKGRAY</code></td>
      <td><code>#A9A9A9</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#1D3C2F'}} /></td>
      <td><code>DARKGRAYISH_BLUE</code></td>
      <td><code>#1D3C2F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#1F382D'}} /></td>
      <td><code>DARKGRAYISH_BLUEGREEN</code></td>
      <td><code>#1F382D</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#223E33'}} /></td>
      <td><code>DARKGRAYISH_GREEN</code></td>
      <td><code>#223E33</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#2C3C31'}} /></td>
      <td><code>DARKGRAYISH_GREENYELLOW</code></td>
      <td><code>#2C3C31</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#223644'}} /></td>
      <td><code>DARKGRAYISH_PURPLE</code></td>
      <td><code>#223644</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#193E3F'}} /></td>
      <td><code>DARKGRAYISH_PURPLEBLUE</code></td>
      <td><code>#193E3F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#4F2E2B'}} /></td>
      <td><code>DARKGRAYISH_RED</code></td>
      <td><code>#4F2E2B</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#353430'}} /></td>
      <td><code>DARKGRAYISH_REDPURPLE</code></td>
      <td><code>#353430</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#4B3F2D'}} /></td>
      <td><code>DARKGRAYISH_YELLOW</code></td>
      <td><code>#4B3F2D</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#55372B'}} /></td>
      <td><code>DARKGRAYISH_YELLOWRED</code></td>
      <td><code>#55372B</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#006400'}} /></td>
      <td><code>DARKGREEN</code></td>
      <td><code>#006400</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#A9A9A9'}} /></td>
      <td><code>DARKGREY</code></td>
      <td><code>#A9A9A9</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#BDB76B'}} /></td>
      <td><code>DARKKHAKI</code></td>
      <td><code>#BDB76B</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#8B008B'}} /></td>
      <td><code>DARKMAGENTA</code></td>
      <td><code>#8B008B</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#556B2F'}} /></td>
      <td><code>DARKOLIVEGREEN</code></td>
      <td><code>#556B2F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FF8C00'}} /></td>
      <td><code>DARKORANGE</code></td>
      <td><code>#FF8C00</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#9932CC'}} /></td>
      <td><code>DARKORCHID</code></td>
      <td><code>#9932CC</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#8B0000'}} /></td>
      <td><code>DARKRED</code></td>
      <td><code>#8B0000</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#E9967A'}} /></td>
      <td><code>DARKSALMON</code></td>
      <td><code>#E9967A</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#8FBC8F'}} /></td>
      <td><code>DARKSEAGREEN</code></td>
      <td><code>#8FBC8F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#483D8B'}} /></td>
      <td><code>DARKSLATEBLUE</code></td>
      <td><code>#483D8B</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#2F4F4F'}} /></td>
      <td><code>DARKSLATEGRAY</code></td>
      <td><code>#2F4F4F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#2F4F4F'}} /></td>
      <td><code>DARKSLATEGREY</code></td>
      <td><code>#2F4F4F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#00CED1'}} /></td>
      <td><code>DARKTURQUOISE</code></td>
      <td><code>#00CED1</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#9400D3'}} /></td>
      <td><code>DARKVIOLET</code></td>
      <td><code>#9400D3</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#125341'}} /></td>
      <td><code>DARK_BLUE</code></td>
      <td><code>#125341</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#1B5631'}} /></td>
      <td><code>DARK_BLUEGREEN</code></td>
      <td><code>#1B5631</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#1E6232'}} /></td>
      <td><code>DARK_GREEN</code></td>
      <td><code>#1E6232</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#365830'}} /></td>
      <td><code>DARK_GREENYELLOW</code></td>
      <td><code>#365830</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#283967'}} /></td>
      <td><code>DARK_PURPLE</code></td>
      <td><code>#283967</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#104C54'}} /></td>
      <td><code>DARK_PURPLEBLUE</code></td>
      <td><code>#104C54</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#742F32'}} /></td>
      <td><code>DARK_RED</code></td>
      <td><code>#742F32</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#583C32'}} /></td>
      <td><code>DARK_REDPURPLE</code></td>
      <td><code>#583C32</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#675B2C'}} /></td>
      <td><code>DARK_YELLOW</code></td>
      <td><code>#675B2C</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#733F2C'}} /></td>
      <td><code>DARK_YELLOWRED</code></td>
      <td><code>#733F2C</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#00FF00'}} /></td>
      <td><code>DB_GREEN</code></td>
      <td><code>#00FF00</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFC800'}} /></td>
      <td><code>DB_ORANGE</code></td>
      <td><code>#FFC800</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFAFAF'}} /></td>
      <td><code>DB_PINK</code></td>
      <td><code>#FFAFAF</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FF1493'}} /></td>
      <td><code>DEEPPINK</code></td>
      <td><code>#FF1493</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#00BFFF'}} /></td>
      <td><code>DEEPSKYBLUE</code></td>
      <td><code>#00BFFF</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#14583C'}} /></td>
      <td><code>DEEP_BLUE</code></td>
      <td><code>#14583C</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#176A2B'}} /></td>
      <td><code>DEEP_BLUEGREEN</code></td>
      <td><code>#176A2B</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#147230'}} /></td>
      <td><code>DEEP_GREEN</code></td>
      <td><code>#147230</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#5B842F'}} /></td>
      <td><code>DEEP_GREENYELLOW</code></td>
      <td><code>#5B842F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#3A3777'}} /></td>
      <td><code>DEEP_PURPLE</code></td>
      <td><code>#3A3777</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#08576B'}} /></td>
      <td><code>DEEP_PURPLEBLUE</code></td>
      <td><code>#08576B</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#AC2430'}} /></td>
      <td><code>DEEP_RED</code></td>
      <td><code>#AC2430</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#6F3D38'}} /></td>
      <td><code>DEEP_REDPURPLE</code></td>
      <td><code>#6F3D38</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#9C8925'}} /></td>
      <td><code>DEEP_YELLOW</code></td>
      <td><code>#9C8925</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#A95731'}} /></td>
      <td><code>DEEP_YELLOWRED</code></td>
      <td><code>#A95731</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#696969'}} /></td>
      <td><code>DIMGRAY</code></td>
      <td><code>#696969</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#696969'}} /></td>
      <td><code>DIMGREY</code></td>
      <td><code>#696969</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#1E90FF'}} /></td>
      <td><code>DODGERBLUE</code></td>
      <td><code>#1E90FF</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#18593F'}} /></td>
      <td><code>DULL_BLUE</code></td>
      <td><code>#18593F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#277A3E'}} /></td>
      <td><code>DULL_BLUEGREEN</code></td>
      <td><code>#277A3E</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#587E3D'}} /></td>
      <td><code>DULL_GREEN</code></td>
      <td><code>#587E3D</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#6D7449'}} /></td>
      <td><code>DULL_GREENYELLOW</code></td>
      <td><code>#6D7449</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#2C4D8F'}} /></td>
      <td><code>DULL_PURPLE</code></td>
      <td><code>#2C4D8F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#356D62'}} /></td>
      <td><code>DULL_PURPLEBLUE</code></td>
      <td><code>#356D62</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#A2583D'}} /></td>
      <td><code>DULL_RED</code></td>
      <td><code>#A2583D</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#73474F'}} /></td>
      <td><code>DULL_REDPURPLE</code></td>
      <td><code>#73474F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#8B7541'}} /></td>
      <td><code>DULL_YELLOW</code></td>
      <td><code>#8B7541</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#A76443'}} /></td>
      <td><code>DULL_YELLOWRED</code></td>
      <td><code>#A76443</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#B22222'}} /></td>
      <td><code>FIREBRICK</code></td>
      <td><code>#B22222</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFFAF0'}} /></td>
      <td><code>FLORALWHITE</code></td>
      <td><code>#FFFAF0</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#228B22'}} /></td>
      <td><code>FORESTGREEN</code></td>
      <td><code>#228B22</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FF00FF'}} /></td>
      <td><code>FUCHSIA</code></td>
      <td><code>#FF00FF</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#DCDCDC'}} /></td>
      <td><code>GAINSBORO</code></td>
      <td><code>#DCDCDC</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#F8F8FF'}} /></td>
      <td><code>GHOSTWHITE</code></td>
      <td><code>#F8F8FF</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFD700'}} /></td>
      <td><code>GOLD</code></td>
      <td><code>#FFD700</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#DAA520'}} /></td>
      <td><code>GOLDENROD</code></td>
      <td><code>#DAA520</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#808080'}} /></td>
      <td><code>GRAY</code></td>
      <td><code>#808080</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#1C1C1C'}} /></td>
      <td><code>GRAY1</code></td>
      <td><code>#1C1C1C</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#383838'}} /></td>
      <td><code>GRAY2</code></td>
      <td><code>#383838</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#545454'}} /></td>
      <td><code>GRAY3</code></td>
      <td><code>#545454</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#707070'}} /></td>
      <td><code>GRAY4</code></td>
      <td><code>#707070</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#8C8C8C'}} /></td>
      <td><code>GRAY5</code></td>
      <td><code>#8C8C8C</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#A8A8A8'}} /></td>
      <td><code>GRAY6</code></td>
      <td><code>#A8A8A8</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#C4C4C4'}} /></td>
      <td><code>GRAY7</code></td>
      <td><code>#C4C4C4</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#E0E0E0'}} /></td>
      <td><code>GRAY8</code></td>
      <td><code>#E0E0E0</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#829A91'}} /></td>
      <td><code>GRAYISH_BLUE</code></td>
      <td><code>#829A91</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#7AA57B'}} /></td>
      <td><code>GRAYISH_BLUEGREEN</code></td>
      <td><code>#7AA57B</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#8FA279'}} /></td>
      <td><code>GRAYISH_GREEN</code></td>
      <td><code>#8FA279</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#908760'}} /></td>
      <td><code>GRAYISH_GREENYELLOW</code></td>
      <td><code>#908760</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#97968B'}} /></td>
      <td><code>GRAYISH_PURPLE</code></td>
      <td><code>#97968B</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#859A99'}} /></td>
      <td><code>GRAYISH_PURPLEBLUE</code></td>
      <td><code>#859A99</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#AB8373'}} /></td>
      <td><code>GRAYISH_RED</code></td>
      <td><code>#AB8373</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#A09383'}} /></td>
      <td><code>GRAYISH_REDPURPLE</code></td>
      <td><code>#A09383</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#948569'}} /></td>
      <td><code>GRAYISH_YELLOW</code></td>
      <td><code>#948569</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#9E806E'}} /></td>
      <td><code>GRAYISH_YELLOWRED</code></td>
      <td><code>#9E806E</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#008000'}} /></td>
      <td><code>GREEN</code></td>
      <td><code>#008000</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#ADFF2F'}} /></td>
      <td><code>GREENYELLOW</code></td>
      <td><code>#ADFF2F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#808080'}} /></td>
      <td><code>GREY</code></td>
      <td><code>#808080</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#F0FFF0'}} /></td>
      <td><code>HONEYDEW</code></td>
      <td><code>#F0FFF0</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FF69B4'}} /></td>
      <td><code>HOTPINK</code></td>
      <td><code>#FF69B4</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#CD5C5C'}} /></td>
      <td><code>INDIANRED</code></td>
      <td><code>#CD5C5C</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#4B0082'}} /></td>
      <td><code>INDIGO</code></td>
      <td><code>#4B0082</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFFFF0'}} /></td>
      <td><code>IVORY</code></td>
      <td><code>#FFFFF0</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#F0E68C'}} /></td>
      <td><code>KHAKI</code></td>
      <td><code>#F0E68C</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#E6E6FA'}} /></td>
      <td><code>LAVENDER</code></td>
      <td><code>#E6E6FA</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFF0F5'}} /></td>
      <td><code>LAVENDERBLUSH</code></td>
      <td><code>#FFF0F5</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#7CFC00'}} /></td>
      <td><code>LAWNGREEN</code></td>
      <td><code>#7CFC00</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFFACD'}} /></td>
      <td><code>LEMONCHIFFON</code></td>
      <td><code>#FFFACD</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#ADD8E6'}} /></td>
      <td><code>LIGHTBLUE</code></td>
      <td><code>#ADD8E6</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#F08080'}} /></td>
      <td><code>LIGHTCORAL</code></td>
      <td><code>#F08080</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#E0FFFF'}} /></td>
      <td><code>LIGHTCYAN</code></td>
      <td><code>#E0FFFF</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FAFAD2'}} /></td>
      <td><code>LIGHTGOLDENRODYELLOW</code></td>
      <td><code>#FAFAD2</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#D3D3D3'}} /></td>
      <td><code>LIGHTGRAY</code></td>
      <td><code>#D3D3D3</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#7FAFA6'}} /></td>
      <td><code>LIGHTGRAYISH_BLUE</code></td>
      <td><code>#7FAFA6</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#A6C9A3'}} /></td>
      <td><code>LIGHTGRAYISH_BLUEGREEN</code></td>
      <td><code>#A6C9A3</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#B3CA9D'}} /></td>
      <td><code>LIGHTGRAYISH_GREEN</code></td>
      <td><code>#B3CA9D</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#D1D5A5'}} /></td>
      <td><code>LIGHTGRAYISH_GREENYELLOW</code></td>
      <td><code>#D1D5A5</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#B8BEBD'}} /></td>
      <td><code>LIGHTGRAYISH_PURPLE</code></td>
      <td><code>#B8BEBD</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#A5B8C7'}} /></td>
      <td><code>LIGHTGRAYISH_PURPLEBLUE</code></td>
      <td><code>#A5B8C7</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#D5B6A6'}} /></td>
      <td><code>LIGHTGRAYISH_RED</code></td>
      <td><code>#D5B6A6</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#CEB9B3'}} /></td>
      <td><code>LIGHTGRAYISH_REDPURPLE</code></td>
      <td><code>#CEB9B3</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#E9E38F'}} /></td>
      <td><code>LIGHTGRAYISH_YELLOW</code></td>
      <td><code>#E9E38F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#DAC494'}} /></td>
      <td><code>LIGHTGRAYISH_YELLOWRED</code></td>
      <td><code>#DAC494</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#90EE90'}} /></td>
      <td><code>LIGHTGREEN</code></td>
      <td><code>#90EE90</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#D3D3D3'}} /></td>
      <td><code>LIGHTGREY</code></td>
      <td><code>#D3D3D3</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFB6C1'}} /></td>
      <td><code>LIGHTPINK</code></td>
      <td><code>#FFB6C1</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFA07A'}} /></td>
      <td><code>LIGHTSALMON</code></td>
      <td><code>#FFA07A</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#20B2AA'}} /></td>
      <td><code>LIGHTSEAGREEN</code></td>
      <td><code>#20B2AA</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#87CEFA'}} /></td>
      <td><code>LIGHTSKYBLUE</code></td>
      <td><code>#87CEFA</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#778899'}} /></td>
      <td><code>LIGHTSLATEGRAY</code></td>
      <td><code>#778899</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#778899'}} /></td>
      <td><code>LIGHTSLATEGREY</code></td>
      <td><code>#778899</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#B0C4DE'}} /></td>
      <td><code>LIGHTSTEELBLUE</code></td>
      <td><code>#B0C4DE</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFFFE0'}} /></td>
      <td><code>LIGHTYELLOW</code></td>
      <td><code>#FFFFE0</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#75ADA9'}} /></td>
      <td><code>LIGHT_BLUE</code></td>
      <td><code>#75ADA9</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#8CC36E'}} /></td>
      <td><code>LIGHT_BLUEGREEN</code></td>
      <td><code>#8CC36E</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#8DBC5A'}} /></td>
      <td><code>LIGHT_GREEN</code></td>
      <td><code>#8DBC5A</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#C3CA65'}} /></td>
      <td><code>LIGHT_GREENYELLOW</code></td>
      <td><code>#C3CA65</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#AAA5C7'}} /></td>
      <td><code>LIGHT_PURPLE</code></td>
      <td><code>#AAA5C7</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#8AA6BB'}} /></td>
      <td><code>LIGHT_PURPLEBLUE</code></td>
      <td><code>#8AA6BB</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#D38E6E'}} /></td>
      <td><code>LIGHT_RED</code></td>
      <td><code>#D38E6E</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#CD9A95'}} /></td>
      <td><code>LIGHT_REDPURPLE</code></td>
      <td><code>#CD9A95</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFCB58'}} /></td>
      <td><code>LIGHT_YELLOW</code></td>
      <td><code>#FFCB58</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#D79160'}} /></td>
      <td><code>LIGHT_YELLOWRED</code></td>
      <td><code>#D79160</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#00FF00'}} /></td>
      <td><code>LIME</code></td>
      <td><code>#00FF00</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#32CD32'}} /></td>
      <td><code>LIMEGREEN</code></td>
      <td><code>#32CD32</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FAF0E6'}} /></td>
      <td><code>LINEN</code></td>
      <td><code>#FAF0E6</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FF00FF'}} /></td>
      <td><code>MAGENTA</code></td>
      <td><code>#FF00FF</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#800000'}} /></td>
      <td><code>MAROON</code></td>
      <td><code>#800000</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#66CDAA'}} /></td>
      <td><code>MEDIUMAQUAMARINE</code></td>
      <td><code>#66CDAA</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#0000CD'}} /></td>
      <td><code>MEDIUMBLUE</code></td>
      <td><code>#0000CD</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#BA55D3'}} /></td>
      <td><code>MEDIUMORCHID</code></td>
      <td><code>#BA55D3</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#9370DB'}} /></td>
      <td><code>MEDIUMPURPLE</code></td>
      <td><code>#9370DB</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#3CB371'}} /></td>
      <td><code>MEDIUMSEAGREEN</code></td>
      <td><code>#3CB371</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#7B68EE'}} /></td>
      <td><code>MEDIUMSLATEBLUE</code></td>
      <td><code>#7B68EE</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#00FA9A'}} /></td>
      <td><code>MEDIUMSPRINGGREEN</code></td>
      <td><code>#00FA9A</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#48D1CC'}} /></td>
      <td><code>MEDIUMTURQUOISE</code></td>
      <td><code>#48D1CC</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#C71585'}} /></td>
      <td><code>MEDIUMVIOLETRED</code></td>
      <td><code>#C71585</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#191970'}} /></td>
      <td><code>MIDNIGHTBLUE</code></td>
      <td><code>#191970</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#F5FFFA'}} /></td>
      <td><code>MINTCREAM</code></td>
      <td><code>#F5FFFA</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFE4E1'}} /></td>
      <td><code>MISTYROSE</code></td>
      <td><code>#FFE4E1</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFE4B5'}} /></td>
      <td><code>MOCCASIN</code></td>
      <td><code>#FFE4B5</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFDEAD'}} /></td>
      <td><code>NAVAJOWHITE</code></td>
      <td><code>#FFDEAD</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#000080'}} /></td>
      <td><code>NAVY</code></td>
      <td><code>#000080</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FDF5E6'}} /></td>
      <td><code>OLDLACE</code></td>
      <td><code>#FDF5E6</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#808000'}} /></td>
      <td><code>OLIVE</code></td>
      <td><code>#808000</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#6B8E23'}} /></td>
      <td><code>OLIVEDRAB</code></td>
      <td><code>#6B8E23</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFA500'}} /></td>
      <td><code>ORANGE</code></td>
      <td><code>#FFA500</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FF4500'}} /></td>
      <td><code>ORANGERED</code></td>
      <td><code>#FF4500</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#DA70D6'}} /></td>
      <td><code>ORCHID</code></td>
      <td><code>#DA70D6</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#EEE8AA'}} /></td>
      <td><code>PALEGOLDENROD</code></td>
      <td><code>#EEE8AA</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#98FB98'}} /></td>
      <td><code>PALEGREEN</code></td>
      <td><code>#98FB98</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#AFEEEE'}} /></td>
      <td><code>PALETURQUOISE</code></td>
      <td><code>#AFEEEE</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#DB7093'}} /></td>
      <td><code>PALEVIOLETRED</code></td>
      <td><code>#DB7093</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#7EBCD1'}} /></td>
      <td><code>PALE_BLUE</code></td>
      <td><code>#7EBCD1</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#92C683'}} /></td>
      <td><code>PALE_BLUEGREEN</code></td>
      <td><code>#92C683</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#9BC471'}} /></td>
      <td><code>PALE_GREEN</code></td>
      <td><code>#9BC471</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#DBDC5D'}} /></td>
      <td><code>PALE_GREENYELLOW</code></td>
      <td><code>#DBDC5D</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#C5BCD5'}} /></td>
      <td><code>PALE_PURPLE</code></td>
      <td><code>#C5BCD5</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#93B8D5'}} /></td>
      <td><code>PALE_PURPLEBLUE</code></td>
      <td><code>#93B8D5</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#E9A390'}} /></td>
      <td><code>PALE_RED</code></td>
      <td><code>#E9A390</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#DAB0B0'}} /></td>
      <td><code>PALE_REDPURPLE</code></td>
      <td><code>#DAB0B0</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFEC4F'}} /></td>
      <td><code>PALE_YELLOW</code></td>
      <td><code>#FFEC4F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#F2B267'}} /></td>
      <td><code>PALE_YELLOWRED</code></td>
      <td><code>#F2B267</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFEFD5'}} /></td>
      <td><code>PAPAYAWHIP</code></td>
      <td><code>#FFEFD5</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFDAB9'}} /></td>
      <td><code>PEACHPUFF</code></td>
      <td><code>#FFDAB9</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#CD853F'}} /></td>
      <td><code>PERU</code></td>
      <td><code>#CD853F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFC0CB'}} /></td>
      <td><code>PINK</code></td>
      <td><code>#FFC0CB</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#DDA0DD'}} /></td>
      <td><code>PLUM</code></td>
      <td><code>#DDA0DD</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#B0E0E6'}} /></td>
      <td><code>POWDERBLUE</code></td>
      <td><code>#B0E0E6</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#800080'}} /></td>
      <td><code>PURPLE</code></td>
      <td><code>#800080</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#663399'}} /></td>
      <td><code>REBECCAPURPLE</code></td>
      <td><code>#663399</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FF0000'}} /></td>
      <td><code>RED</code></td>
      <td><code>#FF0000</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#BC8F8F'}} /></td>
      <td><code>ROSYBROWN</code></td>
      <td><code>#BC8F8F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#4169E1'}} /></td>
      <td><code>ROYALBLUE</code></td>
      <td><code>#4169E1</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#8B4513'}} /></td>
      <td><code>SADDLEBROWN</code></td>
      <td><code>#8B4513</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FA8072'}} /></td>
      <td><code>SALMON</code></td>
      <td><code>#FA8072</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#F4A460'}} /></td>
      <td><code>SANDYBROWN</code></td>
      <td><code>#F4A460</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#2E8B57'}} /></td>
      <td><code>SEAGREEN</code></td>
      <td><code>#2E8B57</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFF5EE'}} /></td>
      <td><code>SEASHELL</code></td>
      <td><code>#FFF5EE</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#A0522D'}} /></td>
      <td><code>SIENNA</code></td>
      <td><code>#A0522D</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#C0C0C0'}} /></td>
      <td><code>SILVER</code></td>
      <td><code>#C0C0C0</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#87CEEB'}} /></td>
      <td><code>SKYBLUE</code></td>
      <td><code>#87CEEB</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#6A5ACD'}} /></td>
      <td><code>SLATEBLUE</code></td>
      <td><code>#6A5ACD</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#708090'}} /></td>
      <td><code>SLATEGRAY</code></td>
      <td><code>#708090</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#708090'}} /></td>
      <td><code>SLATEGREY</code></td>
      <td><code>#708090</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFFAFA'}} /></td>
      <td><code>SNOW</code></td>
      <td><code>#FFFAFA</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#00FF7F'}} /></td>
      <td><code>SPRINGGREEN</code></td>
      <td><code>#00FF7F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#4682B4'}} /></td>
      <td><code>STEELBLUE</code></td>
      <td><code>#4682B4</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#03827A'}} /></td>
      <td><code>STRONG_BLUE</code></td>
      <td><code>#03827A</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#068654'}} /></td>
      <td><code>STRONG_BLUEGREEN</code></td>
      <td><code>#068654</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#129A2F'}} /></td>
      <td><code>STRONG_GREEN</code></td>
      <td><code>#129A2F</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#A2B324'}} /></td>
      <td><code>STRONG_GREENYELLOW</code></td>
      <td><code>#A2B324</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#5C68A3'}} /></td>
      <td><code>STRONG_PURPLE</code></td>
      <td><code>#5C68A3</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#067194'}} /></td>
      <td><code>STRONG_PURPLEBLUE</code></td>
      <td><code>#067194</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#CF2E31'}} /></td>
      <td><code>STRONG_RED</code></td>
      <td><code>#CF2E31</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#AF5C57'}} /></td>
      <td><code>STRONG_REDPURPLE</code></td>
      <td><code>#AF5C57</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#E3BD1C'}} /></td>
      <td><code>STRONG_YELLOW</code></td>
      <td><code>#E3BD1C</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#E2842D'}} /></td>
      <td><code>STRONG_YELLOWRED</code></td>
      <td><code>#E2842D</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#D2B48C'}} /></td>
      <td><code>TAN</code></td>
      <td><code>#D2B48C</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#008080'}} /></td>
      <td><code>TEAL</code></td>
      <td><code>#008080</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#D8BFD8'}} /></td>
      <td><code>THISTLE</code></td>
      <td><code>#D8BFD8</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FF6347'}} /></td>
      <td><code>TOMATO</code></td>
      <td><code>#FF6347</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#40E0D0'}} /></td>
      <td><code>TURQUOISE</code></td>
      <td><code>#40E0D0</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#C2DEF2'}} /></td>
      <td><code>VERYPALE_BLUE</code></td>
      <td><code>#C2DEF2</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#D1EAD3'}} /></td>
      <td><code>VERYPALE_BLUEGREEN</code></td>
      <td><code>#D1EAD3</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#DDE8CF'}} /></td>
      <td><code>VERYPALE_GREEN</code></td>
      <td><code>#DDE8CF</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#E4EBBF'}} /></td>
      <td><code>VERYPALE_GREENYELLOW</code></td>
      <td><code>#E4EBBF</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#E0DAE6'}} /></td>
      <td><code>VERYPALE_PURPLE</code></td>
      <td><code>#E0DAE6</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#CBD7E8'}} /></td>
      <td><code>VERYPALE_PURPLEBLUE</code></td>
      <td><code>#CBD7E8</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#ECD9CA'}} /></td>
      <td><code>VERYPALE_RED</code></td>
      <td><code>#ECD9CA</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#EBDBE0'}} /></td>
      <td><code>VERYPALE_REDPURPLE</code></td>
      <td><code>#EBDBE0</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#F9EFBD'}} /></td>
      <td><code>VERYPALE_YELLOW</code></td>
      <td><code>#F9EFBD</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#F5DFB5'}} /></td>
      <td><code>VERYPALE_YELLOWRED</code></td>
      <td><code>#F5DFB5</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#EE82EE'}} /></td>
      <td><code>VIOLET</code></td>
      <td><code>#EE82EE</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#01868D'}} /></td>
      <td><code>VIVID_BLUE</code></td>
      <td><code>#01868D</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#049457'}} /></td>
      <td><code>VIVID_BLUEGREEN</code></td>
      <td><code>#049457</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#13A632'}} /></td>
      <td><code>VIVID_GREEN</code></td>
      <td><code>#13A632</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#AAC61B'}} /></td>
      <td><code>VIVID_GREENYELLOW</code></td>
      <td><code>#AAC61B</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#2E148D'}} /></td>
      <td><code>VIVID_PURPLE</code></td>
      <td><code>#2E148D</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#03569B'}} /></td>
      <td><code>VIVID_PURPLEBLUE</code></td>
      <td><code>#03569B</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#E72F27'}} /></td>
      <td><code>VIVID_RED</code></td>
      <td><code>#E72F27</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#CC3F5C'}} /></td>
      <td><code>VIVID_REDPURPLE</code></td>
      <td><code>#CC3F5C</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFC808'}} /></td>
      <td><code>VIVID_YELLOW</code></td>
      <td><code>#FFC808</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#EE7119'}} /></td>
      <td><code>VIVID_YELLOWRED</code></td>
      <td><code>#EE7119</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#F5DEB3'}} /></td>
      <td><code>WHEAT</code></td>
      <td><code>#F5DEB3</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFFFFF'}} /></td>
      <td><code>WHITE</code></td>
      <td><code>#FFFFFF</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#F5F5F5'}} /></td>
      <td><code>WHITESMOKE</code></td>
      <td><code>#F5F5F5</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#FFFF00'}} /></td>
      <td><code>YELLOW</code></td>
      <td><code>#FFFF00</code></td>
    </tr>
    <tr>
      <td><span aria-hidden="true" style={{display: 'inline-block', width: '3em', height: '1.2em', verticalAlign: 'middle', borderRadius: '3px', border: '1px solid rgba(128, 128, 128, 0.6)', backgroundColor: '#9ACD32'}} /></td>
      <td><code>YELLOWGREEN</code></td>
      <td><code>#9ACD32</code></td>
    </tr>
  </tbody>
</table>

## Related documentation

- [Format columns](../../../how-to-guides/format-columns.md)
- [`format_columns`](./format-columns.md)
- [`format_column_where`](./format-column-where.md)
- [`format_row_where`](./format-row-where.md)
- [GUI functions](../../query-language/query-library/auto-imported/gui.md)
- [Javadoc](/core/javadoc/io/deephaven/gui/color/Color.html)
- [Pydoc](/core/pydoc/code/deephaven.plot.color.html)
