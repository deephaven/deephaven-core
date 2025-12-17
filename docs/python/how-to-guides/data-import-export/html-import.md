---
title: Read HTML files into Deephaven tables
---

While Deephaven does not have its own methods for reading HTML tables, it's easy to do with [`pandas`](https://pandas.pydata.org/pandas-docs/stable/index.html) or [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/). This guide will demonstrate multiple ways to pull data from online HTML tables into Deephaven tables.

## HTML table

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

## `pandas.read_html`

[`pandas.read_html`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_html.html) is a convenient method for reading HTML tables into a list of pandas DataFrames. The method can be used to read tables from a URL or a local file.

We'll start with an example of how to use [`pandas.read_html`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_html.html) to read an HTML table from a URL and convert it to a Deephaven table. This approach is simple and easy, but has some limitations - for instance, it does not handle the non-numerical values in this table gracefully:

```python order=deephaven_theme_colors
from deephaven import pandas as dhpd
from deephaven import merge

import os

os.system("pip install lxml")

import pandas as pd
import lxml

deephaven_theme_colors = merge(
    [
        dhpd.to_table(df)
        for df in pd.read_html(
            "https://deephaven.io/core/docs/how-to-guides/data-import-export/html-import/"
        )
    ]
)
```

The sections below properly handle data types, leading to tables with actual data rather than just `NaN`s.

## BeautifulSoup

[BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) is a Python library for pulling data out of HTML and XML files.

In this example, we'll use [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) to read the same HTML table as in the example above and convert it to a Deephaven table:

```python order=deephaven_theme_colors
import os

os.system("pip install beautifulsoup4 requests lxml")
from deephaven.column import string_col, int_col
from deephaven import new_table
from bs4 import BeautifulSoup
import requests

url = "https://deephaven.io/core/docs/how-to-guides/data-import-export/html-import/"
headers = {"User-Agent": "Mozilla/5.0"}  # helps avoid being blocked

r = requests.get(url, headers=headers)

soup = BeautifulSoup(r.content, "lxml")

# Try to find the table
table = soup.find("table")  # you can add class_="..." if needed

# Extract headers
headers = [th.text.strip() for th in table.find("thead").find_all("th")]

# Extract rows
rows = []
for tr in table.find("tbody").find_all("tr"):
    row = [td.text.strip() for td in tr.find_all("td")]
    if row:
        rows.append(row)

deephaven_theme_colors = new_table(
    [
        string_col("Color", [row[0] for row in rows]),
        string_col("Shades", [row[1] for row in rows]),
        string_col("Shades_1", [row[2] for row in rows]),
        string_col("Shades_2", [row[3] for row in rows]),
        string_col("Shades_3", [row[4] for row in rows]),
        string_col("Shades_4", [row[5] for row in rows]),
        string_col("Shades_5", [row[6] for row in rows]),
        string_col("Shades_6", [row[7] for row in rows]),
        string_col("Shades_7", [row[8] for row in rows]),
        string_col("Shades_8", [row[9] for row in rows]),
        string_col("Shades_9", [row[10] for row in rows]),
        string_col("Shades_10", [row[11] for row in rows]),
    ]
)
```

## Data types

Since HTML tables store all data as plain text and have no concept of data types, some care must be taken when importing HTML tables into Deephaven to ensure that you end up with correct data types for each column. Deephaven's [`to_table`](../../reference/pandas/to-table.md) method will automatically infer types as long as `infer_objects=True`, but to guarantee that the types are correct, manual specification is recommended.

Whether you are using [`pandas`](https://pandas.pydata.org/pandas-docs/stable/index.html) or [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/), you can specify the data type of each column at either the DataFrame stage or by calling [`update`](../../reference/table-operations/select/update.md) to typecast columns after the Deephaven table has been created. This can be done by using the [`astype`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.astype.html#pandas.DataFrame.astype) method in [`pandas`](https://pandas.pydata.org/pandas-docs/stable/index.html) or by using one of Deephaven's [selection](../use-select-view-update.md) methods.

### Typing with selection methods

In this example, we use the same HTML table as in the examples above as a source. We then read the table into Deephaven with [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) and convert it to a Deephaven table with `to_table`. Finally, we restore the correct types fore each column with Deephaven's [`update`](../../reference/table-operations/select/update.md) method:

```python order=table_typed
from deephaven import pandas as dhpd
import pandas as pd
import os

os.system("pip install beautifulsoup4 requests")
os.system("pip install requests")
os.system("pip install lxml")
from bs4 import BeautifulSoup
import requests

url = "https://deephaven.io/core/docs/how-to-guides/data-import-export/html-import/"
headers = {"User-Agent": "Mozilla/5.0"}  # helps avoid being blocked

r = requests.get(url, headers=headers)

soup = BeautifulSoup(r.content, "lxml")

# Try to find the table
table = soup.find("table")  # you can add class_="..." if needed

# Get basic headers from thead (usually just 'Color' and 'Shades')
basic_headers = [th.text.strip() for th in table.find("thead").find_all("th")]

# Extract rows
rows = []
for tr in table.find("tbody").find_all("tr"):
    row = [td.text.strip() for td in tr.find_all("td")]
    if row:
        rows.append(row)

# Create expanded headers to match the actual number of columns
# First column keeps its name (usually 'Color')
headers = [basic_headers[0]]
# For remaining columns, use second header name with index suffix if needed
for i in range(1, len(rows[0])):
    if i == 1:
        headers.append(basic_headers[1])  # Usually 'Shades'
    else:
        headers.append(f"{basic_headers[1]}_{i - 1}")  # 'Shades_1', 'Shades_2', etc.

df = pd.DataFrame(rows, columns=headers)

# infer_objects is set to `True` be default
# in this case, all columns are inferred as Strings
deephaven_theme_colors = dhpd.to_table(df)

table_typed = deephaven_theme_colors.update(
    [
        "Color = String.valueOf(Color)",
        "Shades = String.valueOf(Shades)",
        "Shades_1 = String.valueOf(Shades_1)",
        "Shades_2 = String.valueOf(Shades_2)",
        "Shades_3 = String.valueOf(Shades_3)",
        "Shades_4 = String.valueOf(Shades_4)",
        "Shades_5 = String.valueOf(Shades_5)",
        "Shades_6 = String.valueOf(Shades_6)",
        "Shades_7 = String.valueOf(Shades_7)",
        "Shades_8 = String.valueOf(Shades_8)",
        "Shades_9 = String.valueOf(Shades_9)",
        "Shades_10 = String.valueOf(Shades_10)",
    ]
)
```

Note that the Pandas [`DataFrame.astype`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.astype.html#pandas.DataFrame.astype) method can also be used to restore typing. However, it does not handle Deephaven's datetime types effectively, so the [`update`](../../reference/table-operations/select/update.md) method is recommended in those cases.

### Typing with `astype`

This example demonstrates how to add typing to the `deephaven_theme_colors` table created above, using [`astype`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.astype.html#pandas.DataFrame.astype):

```python order=deephaven_theme_colors
from deephaven import pandas as dhpd
import pandas as pd
import os

os.system("pip install beautifulsoup4 requests")
os.system("pip install requests")
os.system("pip install lxml")
from bs4 import BeautifulSoup
import requests

url = "https://deephaven.io/core/docs/how-to-guides/data-import-export/html-import/"
headers = {"User-Agent": "Mozilla/5.0"}  # helps avoid being blocked

r = requests.get(url, headers=headers)

soup = BeautifulSoup(r.content, "lxml")

# Try to find the table
table = soup.find("table")

# Get basic headers
basic_headers = [th.text.strip() for th in table.find("thead").find_all("th")]

# Extract rows
rows = []
for tr in table.find("tbody").find_all("tr"):
    row = [td.text.strip() for td in tr.find_all("td")]
    if row:
        rows.append(row)

# Create expanded headers to match the actual number of columns
# First column keeps its name (usually 'Color')
headers = [basic_headers[0]]
# For remaining columns, use second header name with index suffix if needed
for i in range(1, len(rows[0])):
    if i == 1:
        headers.append(basic_headers[1])  # Usually 'Shades'
    else:
        headers.append(f"{basic_headers[1]}_{i - 1}")  # 'Shades_1', 'Shades_2', etc.

# create the dataframe and specify types for each column
df = pd.DataFrame(rows, columns=headers).astype(
    {
        "Color": "string",
        "Shades": "string",
        "Shades_1": "string",
        "Shades_2": "string",
        "Shades_3": "string",
        "Shades_4": "string",
        "Shades_5": "string",
        "Shades_6": "string",
        "Shades_7": "string",
        "Shades_8": "string",
        "Shades_9": "string",
        "Shades_10": "string",
    }
)

# convert to table - Deephaven correctly interprets "string" as a String
deephaven_theme_colors = dhpd.to_table(df)
```

## Related documentation

- [Export HTML files](./html-export.md)
