---
title: Read HTML files into Deephaven tables
---

Deephaven does not provide built-in methods for reading HTML tables in Groovy. Because Groovy runs on the JVM, you can use any Java HTML parsing library to pull data from HTML into Deephaven tables.

[jsoup](https://jsoup.org/) is a popular Java library for parsing HTML, similar to [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) in Python. It parses malformed HTML, and it can both download and parse a web page.

Deephaven doesn't include jsoup, so add it to the classpath before you use it. See [Install and use Java packages](../install-and-use-java-packages.md) for instructions on how to add external JARs to a Deephaven worker.

## Read an HTML table with jsoup

Let's start with an example that reads a small HTML table of stock trades from a string. [`Jsoup.parse`](https://jsoup.org/apidocs/org/jsoup/Jsoup.html#parse(java.lang.String)) parses the HTML, [`select`](https://jsoup.org/apidocs/org/jsoup/nodes/Element.html#select(java.lang.String)) finds the header cells and the body rows with CSS selectors, and [`text`](https://jsoup.org/apidocs/org/jsoup/nodes/Element.html#text()) returns each cell's text. The example then builds a Deephaven table with [`newTable`](../../reference/table-operations/create/newTable.md) and one [`stringCol`](../../reference/table-operations/create/stringCol.md) per header cell:

```groovy skip-test
import io.deephaven.engine.table.impl.util.ColumnHolder
import org.jsoup.Jsoup

tradesHtml = """
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

doc = Jsoup.parse(tradesHtml)

// Extract headers
tradeColumns = doc.select("thead th").collect { it.text() }

// Extract rows
tradeRows = doc.select("tbody tr").collect { tr -> tr.select("td").collect { it.text() } }

// Build one String column per header cell
trades = newTable(
    tradeColumns.withIndex().collect { name, i ->
        stringCol(name, tradeRows.collect { it[i] } as String[])
    } as ColumnHolder[]
)
```

To read a table from a local HTML file instead, replace `Jsoup.parse(tradesHtml)` with `Jsoup.parse(new File("/path/to/file.html"), "UTF-8")`. To read a table from a web page, use [`Jsoup.connect`](https://jsoup.org/apidocs/org/jsoup/Jsoup.html#connect(java.lang.String)): `Jsoup.connect(url).get()` downloads and parses the page.

## Data types

HTML tables store all data as plain text and have no concept of data types, so every column in the `trades` table above is a `String` column. Set the correct type for each column with the [`update`](../../reference/table-operations/select/update.md) method or another of Deephaven's [selection](../use-select-view-update.md) methods. This example uses Java's `Double.parseDouble` and `Integer.parseInt` methods and the built-in [`parseInstant`](../../reference/time/datetime/parseInstant.md) function:

```groovy skip-test
tradesTyped = trades.update(
    "Price = Double.parseDouble(Price)",
    "Size = Integer.parseInt(Size)",
    "Timestamp = parseInstant(Timestamp)"
)
```

## Related documentation

- [Convert tables to HTML strings](./html-export.md)
- [Install and use Java packages](../install-and-use-java-packages.md)
- [`newTable`](../../reference/table-operations/create/newTable.md)
- [`update`](../../reference/table-operations/select/update.md)
