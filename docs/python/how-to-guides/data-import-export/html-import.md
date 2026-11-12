---
title: Read HTML files into Deephaven tables
---

While Deephaven does not have its own methods for reading HTML tables, you can read them with [pandas](https://pandas.pydata.org/docs/index.html) or [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/). This guide demonstrates multiple ways to pull data from HTML tables, such as those on web pages, into Deephaven tables.

## Example table and setup

We'll use this HTML table in the examples below:

<table style={{width:"100%", borderCollapse:"collapse", fontFamily:"sans-serif", fontSize:"14px"}}>
<thead>
    <tr>
    <th style={{textAlign:"left", padding:"8px", borderBottom:"1px solid #ccc"}}>
        Color
    </th>
    <th
        colSpan="11"
        style={{textAlign:"left", padding:"8px", borderBottom:"1px solid #ccc"}}
    >
        Shades
    </th>
    </tr>
</thead>
<tbody>
    <tr>
    <td style={{padding:"8px", fontWeight:"bold"}}>primary</td>
    <td style={{background:"#e5faff", padding:"12px", color:"#000"}} title="#e5faff">#e5faff</td>
    <td style={{background:"#d7f7ff", padding:"12px", color:"#000"}} title="#d7f7ff">#d7f7ff</td>
    <td style={{background:"#c8f4ff", padding:"12px", color:"#000"}} title="#c8f4ff">#c8f4ff</td>
    <td style={{background:"#b9f1ff", padding:"12px", color:"#000"}} title="#b9f1ff">#b9f1ff</td>
    <td style={{background:"#a9edff", padding:"12px", color:"#000"}} title="#a9edff">#a9edff</td>
    <td style={{background:"#99eaff", padding:"12px", color:"#000"}} title="#99eaff">#99eaff</td>
    <td style={{background:"#7fc6d9", padding:"12px", color:"#000"}} title="#7fc6d9">#7fc6d9</td>
    <td style={{background:"#65a4b4", padding:"12px", color:"#000"}} title="#65a4b4">#65a4b4</td>
    <td style={{background:"#4d8391", padding:"12px", color:"#fff"}} title="#4d8391">#4d8391</td>
    <td style={{background:"#35636f", padding:"12px", color:"#fff"}} title="#35636f">#35636f</td>
    <td style={{background:"#1f454f", padding:"12px", color:"#fff"}} title="#1f454f">#1f454f</td>
    </tr>
    <tr>
    <td style={{padding:"8px", fontWeight:"bold"}}>secondary</td>
    <td style={{background:"#6a81f2", padding:"12px", color:"#000"}} title="#6a81f2">#6a81f2</td>
    <td style={{background:"#596fe9", padding:"12px", color:"#000"}} title="#596fe9">#596fe9</td>
    <td style={{background:"#495ee0", padding:"12px", color:"#000"}} title="#495ee0">#495ee0</td>
    <td style={{background:"#384cd6", padding:"12px", color:"#fff"}} title="#384cd6">#384cd6</td>
    <td style={{background:"#2439cb", padding:"12px", color:"#fff"}} title="#2439cb">#2439cb</td>
    <td style={{background:"#0625c0", padding:"12px", color:"#fff"}} title="#0625c0">#0625c0</td>
    <td style={{background:"#0121aa", padding:"12px", color:"#fff"}} title="#0121aa">#0121aa</td>
    <td style={{background:"#001e94", padding:"12px", color:"#fff"}} title="#001e94">#001e94</td>
    <td style={{background:"#001a7e", padding:"12px", color:"#fff"}} title="#001a7e">#001a7e</td>
    <td style={{background:"#011569", padding:"12px", color:"#fff"}} title="#011569">#011569</td>
    <td style={{background:"#031155", padding:"12px", color:"#fff"}} title="#031155">#031155</td>
    </tr>
    <tr>
    <td style={{padding:"8px", fontWeight:"bold"}}>negative</td>
    <td style={{background:"#f883a0", padding:"12px", color:"#000"}} title="#f883a0">#f883a0</td>
    <td style={{background:"#f87694", padding:"12px", color:"#000"}} title="#f87694">#f87694</td>
    <td style={{background:"#f76989", padding:"12px", color:"#000"}} title="#f76989">#f76989</td>
    <td style={{background:"#f65a7d", padding:"12px", color:"#000"}} title="#f65a7d">#f65a7d</td>
    <td style={{background:"#f54a72", padding:"12px", color:"#000"}} title="#f54a72">#f54a72</td>
    <td style={{background:"#f33666", padding:"12px", color:"#fff"}} title="#f33666">#f33666</td>
    <td style={{background:"#da2e59", padding:"12px", color:"#fff"}} title="#da2e59">#da2e59</td>
    <td style={{background:"#c1254c", padding:"12px", color:"#fff"}} title="#c1254c">#c1254c</td>
    <td style={{background:"#a91d40", padding:"12px", color:"#fff"}} title="#a91d40">#a91d40</td>
    <td style={{background:"#911534", padding:"12px", color:"#fff"}} title="#911534">#911534</td>
    <td style={{background:"#7a0d28", padding:"12px", color:"#fff"}} title="#7a0d28">#7a0d28</td>
    </tr>
    <tr>
    <td style={{padding:"8px", fontWeight:"bold"}}>positive</td>
    <td style={{background:"#9dfc7e", padding:"12px", color:"#000"}} title="#9dfc7e">#9dfc7e</td>
    <td style={{background:"#93fa72", padding:"12px", color:"#000"}} title="#93fa72">#93fa72</td>
    <td style={{background:"#89f966", padding:"12px", color:"#000"}} title="#89f966">#89f966</td>
    <td style={{background:"#7ef758", padding:"12px", color:"#000"}} title="#7ef758">#7ef758</td>
    <td style={{background:"#72f549", padding:"12px", color:"#000"}} title="#72f549">#72f549</td>
    <td style={{background:"#65f336", padding:"12px", color:"#000"}} title="#65f336">#65f336</td>
    <td style={{background:"#58d92e", padding:"12px", color:"#000"}} title="#58d92e">#58d92e</td>
    <td style={{background:"#4cc025", padding:"12px", color:"#000"}} title="#4cc025">#4cc025</td>
    <td style={{background:"#3fa71d", padding:"12px", color:"#000"}} title="#3fa71d">#3fa71d</td>
    <td style={{background:"#348f15", padding:"12px", color:"#fff"}} title="#348f15">#348f15</td>
    <td style={{background:"#28780d", padding:"12px", color:"#fff"}} title="#28780d">#28780d</td>
    </tr>
    <tr>
    <td style={{padding:"8px", fontWeight:"bold"}}>warn</td>
    <td style={{background:"#ffe590", padding:"12px", color:"#000"}} title="#ffe590">#ffe590</td>
    <td style={{background:"#ffe183", padding:"12px", color:"#000"}} title="#ffe183">#ffe183</td>
    <td style={{background:"#fedd75", padding:"12px", color:"#000"}} title="#fedd75">#fedd75</td>
    <td style={{background:"#fed966", padding:"12px", color:"#000"}} title="#fed966">#fed966</td>
    <td style={{background:"#fdd455", padding:"12px", color:"#000"}} title="#fdd455">#fdd455</td>
    <td style={{background:"#fdd041", padding:"12px", color:"#000"}} title="#fdd041">#fdd041</td>
    <td style={{background:"#e8be38", padding:"12px", color:"#000"}} title="#e8be38">#e8be38</td>
    <td style={{background:"#d4ac2f", padding:"12px", color:"#000"}} title="#d4ac2f">#d4ac2f</td>
    <td style={{background:"#c09b27", padding:"12px", color:"#000"}} title="#c09b27">#c09b27</td>
    <td style={{background:"#ac8a1e", padding:"12px", color:"#000"}} title="#ac8a1e">#ac8a1e</td>
    <td style={{background:"#997915", padding:"12px", color:"#fff"}} title="#997915">#997915</td>
    </tr>
    <tr>
    <td style={{padding:"8px", fontWeight:"bold"}}>info</td>
    <td style={{background:"#f08df9", padding:"12px", color:"#000"}} title="#f08df9">#f08df9</td>
    <td style={{background:"#ee7ff8", padding:"12px", color:"#000"}} title="#ee7ff8">#ee7ff8</td>
    <td style={{background:"#eb70f7", padding:"12px", color:"#000"}} title="#eb70f7">#eb70f7</td>
    <td style={{background:"#e960f6", padding:"12px", color:"#000"}} title="#e960f6">#e960f6</td>
    <td style={{background:"#e64df4", padding:"12px", color:"#000"}} title="#e64df4">#e64df4</td>
    <td style={{background:"#e336f3", padding:"12px", color:"#000"}} title="#e336f3">#e336f3</td>
    <td style={{background:"#cd34dd", padding:"12px", color:"#000"}} title="#cd34dd">#cd34dd</td>
    <td style={{background:"#b732c7", padding:"12px", color:"#fff"}} title="#b732c7">#b732c7</td>
    <td style={{background:"#a22fb2", padding:"12px", color:"#fff"}} title="#a22fb2">#a22fb2</td>
    <td style={{background:"#8d2c9d", padding:"12px", color:"#fff"}} title="#8d2c9d">#8d2c9d</td>
    <td style={{background:"#792989", padding:"12px", color:"#fff"}} title="#792989">#792989</td>
    </tr>
    <tr>
    <td style={{padding:"8px", fontWeight:"bold"}}>fg</td>
    <td style={{background:"#f3f7fa", padding:"12px", color:"#000"}} title="#f3f7fa">#f3f7fa</td>
    <td style={{background:"#eef2f5", padding:"12px", color:"#000"}} title="#eef2f5">#eef2f5</td>
    <td style={{background:"#e9edf0", padding:"12px", color:"#000"}} title="#e9edf0">#e9edf0</td>
    <td style={{background:"#e4e9ec", padding:"12px", color:"#000"}} title="#e4e9ec">#e4e9ec</td>
    <td style={{background:"#dfe4e7", padding:"12px", color:"#000"}} title="#dfe4e7">#dfe4e7</td>
    <td style={{background:"#dadfe2", padding:"12px", color:"#000"}} title="#dadfe2">#dadfe2</td>
    <td style={{background:"#c6cbcf", padding:"12px", color:"#000"}} title="#c6cbcf">#c6cbcf</td>
    <td style={{background:"#b2b8bc", padding:"12px", color:"#000"}} title="#b2b8bc">#b2b8bc</td>
    <td style={{background:"#9fa5aa", padding:"12px", color:"#000"}} title="#9fa5aa">#9fa5aa</td>
    <td style={{background:"#8c9298", padding:"12px", color:"#000"}} title="#8c9298">#8c9298</td>
    <td style={{background:"#798086", padding:"12px", color:"#fff"}} title="#798086">#798086</td>
    </tr>
    <tr>
    <td style={{padding:"8px", fontWeight:"bold"}}>bg</td>
    <td style={{background:"#434f56", padding:"12px", color:"#fff"}} title="#434f56">#434f56</td>
    <td style={{background:"#3c474d", padding:"12px", color:"#fff"}} title="#3c474d">#3c474d</td>
    <td style={{background:"#353e44", padding:"12px", color:"#fff"}} title="#353e44">#353e44</td>
    <td style={{background:"#2f363c", padding:"12px", color:"#fff"}} title="#2f363c">#2f363c</td>
    <td style={{background:"#282f33", padding:"12px", color:"#fff"}} title="#282f33">#282f33</td>
    <td style={{background:"#22272b", padding:"12px", color:"#fff"}} title="#22272b">#22272b</td>
    <td style={{background:"#1f2327", padding:"12px", color:"#fff"}} title="#1f2327">#1f2327</td>
    <td style={{background:"#1b2023", padding:"12px", color:"#fff"}} title="#1b2023">#1b2023</td>
    <td style={{background:"#181c1f", padding:"12px", color:"#fff"}} title="#181c1f">#181c1f</td>
    <td style={{background:"#15181b", padding:"12px", color:"#fff"}} title="#15181b">#15181b</td>
    <td style={{background:"#121517", padding:"12px", color:"#fff"}} title="#121517">#121517</td>
    </tr>
</tbody>
</table>

The examples read this table from a string of HTML, `html_str`, rather than from a web page, so reading the table doesn't need a network connection. The following code defines `html_str` and installs BeautifulSoup and [lxml](https://lxml.de/), the HTML parser the examples use:

```python test-set=html-import order=null
import os

os.system("pip install beautifulsoup4 lxml")

html_str = """
<table>
  <thead>
    <tr><th>Color</th><th colspan="11">Shades</th></tr>
  </thead>
  <tbody>
    <tr><td>primary</td>
      <td>#e5faff</td><td>#d7f7ff</td><td>#c8f4ff</td><td>#b9f1ff</td><td>#a9edff</td><td>#99eaff</td>
      <td>#7fc6d9</td><td>#65a4b4</td><td>#4d8391</td><td>#35636f</td><td>#1f454f</td></tr>
    <tr><td>secondary</td>
      <td>#6a81f2</td><td>#596fe9</td><td>#495ee0</td><td>#384cd6</td><td>#2439cb</td><td>#0625c0</td>
      <td>#0121aa</td><td>#001e94</td><td>#001a7e</td><td>#011569</td><td>#031155</td></tr>
    <tr><td>negative</td>
      <td>#f883a0</td><td>#f87694</td><td>#f76989</td><td>#f65a7d</td><td>#f54a72</td><td>#f33666</td>
      <td>#da2e59</td><td>#c1254c</td><td>#a91d40</td><td>#911534</td><td>#7a0d28</td></tr>
    <tr><td>positive</td>
      <td>#9dfc7e</td><td>#93fa72</td><td>#89f966</td><td>#7ef758</td><td>#72f549</td><td>#65f336</td>
      <td>#58d92e</td><td>#4cc025</td><td>#3fa71d</td><td>#348f15</td><td>#28780d</td></tr>
    <tr><td>warn</td>
      <td>#ffe590</td><td>#ffe183</td><td>#fedd75</td><td>#fed966</td><td>#fdd455</td><td>#fdd041</td>
      <td>#e8be38</td><td>#d4ac2f</td><td>#c09b27</td><td>#ac8a1e</td><td>#997915</td></tr>
    <tr><td>info</td>
      <td>#f08df9</td><td>#ee7ff8</td><td>#eb70f7</td><td>#e960f6</td><td>#e64df4</td><td>#e336f3</td>
      <td>#cd34dd</td><td>#b732c7</td><td>#a22fb2</td><td>#8d2c9d</td><td>#792989</td></tr>
    <tr><td>fg</td>
      <td>#f3f7fa</td><td>#eef2f5</td><td>#e9edf0</td><td>#e4e9ec</td><td>#dfe4e7</td><td>#dadfe2</td>
      <td>#c6cbcf</td><td>#b2b8bc</td><td>#9fa5aa</td><td>#8c9298</td><td>#798086</td></tr>
    <tr><td>bg</td>
      <td>#434f56</td><td>#3c474d</td><td>#353e44</td><td>#2f363c</td><td>#282f33</td><td>#22272b</td>
      <td>#1f2327</td><td>#1b2023</td><td>#181c1f</td><td>#15181b</td><td>#121517</td></tr>
  </tbody>
</table>
"""
```

## `pandas.read_html`

[`pandas.read_html`](https://pandas.pydata.org/docs/reference/api/pandas.read_html.html) is a convenient method for reading HTML tables into a list of pandas DataFrames. The method also reads tables from a URL or a local file. To read HTML that is already in a string, wrap the string in [`io.StringIO`](https://docs.python.org/3/library/io.html#io.StringIO). pandas 2.1 deprecated passing literal HTML as a plain string, and pandas 3 treats a plain string as a URL or file path.

Let's start with [`pandas.read_html`](https://pandas.pydata.org/docs/reference/api/pandas.read_html.html). The following example reads the color table in `html_str` and converts it to a Deephaven table with [`to_table`](../../reference/pandas/to-table.md). The [Example table and setup](#example-table-and-setup) section defines `html_str` and installs lxml, the parser that `read_html` uses by default. `read_html` returns one DataFrame for each `<table>` element in its input, so the example converts the first one:

```python test-set=html-import order=deephaven_theme_colors
from deephaven import pandas as dhpd
import pandas as pd
import io

dfs = pd.read_html(io.StringIO(html_str))

deephaven_theme_colors = dhpd.to_table(dfs[0])
```

This approach is simple, but you have little control over the column names or which part of each cell it reads:

- [`read_html`](https://pandas.pydata.org/docs/reference/api/pandas.read_html.html) names each column after its header cell. The `Shades` header spans 11 columns, so pandas repeats the name and makes each copy unique: `Shades`, `Shades.1`, and so on through `Shades.10`. Deephaven column names must be valid Java identifiers, so [`to_table`](../../reference/pandas/to-table.md) replaces characters that a column name can't contain, such as `.`, with `_`, which gives `Shades_1` through `Shades_10`.
- By default, [`read_html`](https://pandas.pydata.org/docs/reference/api/pandas.read_html.html) keeps only the text of each cell. Its `extract_links` argument can also keep link URLs, but `read_html` doesn't return any other cell attributes. An empty cell becomes `NaN`, so if a table stores its values in attributes such as `title` instead of in the cell text, those columns contain only `NaN` values.

## BeautifulSoup

[BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) is a Python library for pulling data out of HTML and XML files. It takes more code than [`read_html`](https://pandas.pydata.org/docs/reference/api/pandas.read_html.html), but you decide what to read from each cell and what to name each column.

In this example, we'll use [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) to read the color table in `html_str` and convert it to a Deephaven table with [`new_table`](../../reference/table-operations/create/newTable.md) and [`string_col`](../../reference/table-operations/create/stringCol.md). The [Example table and setup](#example-table-and-setup) section defines `html_str` and installs BeautifulSoup. The `Shades` header spans 11 columns, so the code generates a name for each of them. The result matches the [`read_html`](https://pandas.pydata.org/docs/reference/api/pandas.read_html.html) example's result, so you can compare the two approaches:

```python test-set=html-import order=deephaven_theme_colors
from deephaven.column import string_col
from deephaven import new_table
from bs4 import BeautifulSoup

soup = BeautifulSoup(html_str, "lxml")

# Find the first table; pass class_="..." to pick a specific one
table = soup.find("table")

# Extract headers: "Color" and "Shades"
headers = [th.text.strip() for th in table.find("thead").find_all("th")]

# Extract rows
rows = []
for tr in table.find("tbody").find_all("tr"):
    row = [td.text.strip() for td in tr.find_all("td")]
    if row:
        rows.append(row)

# Name every column: "Color", "Shades", then "Shades_1" through "Shades_10"
column_names = headers + [f"{headers[1]}_{i}" for i in range(1, len(rows[0]) - 1)]

deephaven_theme_colors = new_table(
    [string_col(name, [row[i] for row in rows]) for i, name in enumerate(column_names)]
)
```

If a table stores a cell's value in an attribute, such as `title`, instead of in the cell text, replace `td.text.strip()` with `td.get("title", td.text.strip())`. The cells in `html_str` have no `title` attribute, so this change doesn't alter the example's result.

To read a table from a local HTML file, open the file and pass it to [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) in place of `html_str`, for example `with open("/path/to/file.html") as f: soup = BeautifulSoup(f, "lxml")`. To read a table from a web page, download the page first and pass the result to `BeautifulSoup`. For example, install the [`requests`](https://requests.readthedocs.io/) library with `pip install requests` and use `requests.get(url).content`.

## Data types

HTML tables store all data as plain text and have no concept of data types, so check each column's type after you import a table. Each tool handles types differently:

- By default, [`pandas.read_html`](https://pandas.pydata.org/docs/reference/api/pandas.read_html.html) converts columns of numeric text to numeric columns but leaves other values, such as dates, as strings.
- [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) returns every value as a string.
- [`to_table`](../../reference/pandas/to-table.md) gives each Deephaven column the type that matches the DataFrame column's type, so a column of text becomes a `String` column. `to_table` doesn't parse text, so numbers stored as strings stay strings.

Whether you use [pandas](https://pandas.pydata.org/docs/index.html) or [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/), you can set each column's data type in either of two places:

- In the Deephaven table, with [`update`](../../reference/table-operations/select/update.md) or another of Deephaven's [selection](../use-select-view-update.md) methods.
- In the DataFrame, with the pandas [`astype`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.astype.html#pandas.DataFrame.astype) method.

The examples in this section use a small HTML table of stock trades with `Symbol`, `Price`, `Size`, and `Timestamp` columns. We'll read it with [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/), so every column starts as text. With [`pandas.read_html`](https://pandas.pydata.org/docs/reference/api/pandas.read_html.html), `Price` and `Size` would already be numeric, and only `Timestamp` would need converting. The code puts the rows in a pandas DataFrame, so both typing methods below can start from the same data.

The following code reads the table into `trades_df`, a DataFrame of strings. It then uses [`to_table`](../../reference/pandas/to-table.md) to convert `trades_df` to `trades`, a Deephaven table of `String` columns:

```python test-set=html-import order=trades
from deephaven import pandas as dhpd
from bs4 import BeautifulSoup
import pandas as pd

trades_html = """
<table>
  <thead>
    <tr><th>Symbol</th><th>Price</th><th>Size</th><th>Timestamp</th></tr>
  </thead>
  <tbody>
    <tr><td>AAPL</td><td>226.50</td><td>100</td><td>2024-09-03T13:30:00Z</td></tr>
    <tr><td>MSFT</td><td>409.44</td><td>250</td><td>2024-09-03T13:31:15Z</td></tr>
    <tr><td>GOOG</td><td>163.38</td><td>75</td><td>2024-09-03T13:32:40Z</td></tr>
  </tbody>
</table>
"""

soup = BeautifulSoup(trades_html, "lxml")
trade_columns = [th.text.strip() for th in soup.find("thead").find_all("th")]
trade_rows = [
    [td.text.strip() for td in tr.find_all("td")]
    for tr in soup.find("tbody").find_all("tr")
]

trades_df = pd.DataFrame(trade_rows, columns=trade_columns)

# Every column holds text, so to_table makes each one a String column
trades = dhpd.to_table(trades_df)
```

### Typing with `update`

In this example, we start from `trades`, the table of `String` columns created in [Data types](#data-types). It sets the correct type for each column with Deephaven's [`update`](../../reference/table-operations/select/update.md) method, using Java's `Double.parseDouble` and `Integer.parseInt` methods and the built-in [`parseInstant`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseInstant(java.lang.String)) function:

```python test-set=html-import order=trades_typed
trades_typed = trades.update(
    [
        "Price = Double.parseDouble(Price)",
        "Size = Integer.parseInt(Size)",
        "Timestamp = parseInstant(Timestamp)",
    ]
)
```

### Typing with `astype`

In this example, we start from `trades_df`, the DataFrame of strings created in [Data types](#data-types). It sets each column's type with [`astype`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.astype.html#pandas.DataFrame.astype) before it converts the DataFrame to a Deephaven table. [`to_table`](../../reference/pandas/to-table.md) turns the `float64` column into a `double` column, the `int32` column into an `int` column, and the `datetime64` column into an `Instant` column:

```python test-set=html-import order=trades_from_astype
trades_from_astype = dhpd.to_table(
    trades_df.astype(
        {
            "Price": "float64",
            "Size": "int32",
            "Timestamp": "datetime64[ns, UTC]",
        }
    )
)
```

## Related documentation

- [Convert tables to HTML strings](./html-export.md)
- [`to_table`](../../reference/pandas/to-table.md)
- [`new_table`](../../reference/table-operations/create/newTable.md)
- [`update`](../../reference/table-operations/select/update.md)
