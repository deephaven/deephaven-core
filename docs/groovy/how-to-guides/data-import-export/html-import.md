---
title: Read HTML files into Deephaven tables
---

Deephaven does not provide built-in methods for reading HTML tables in Groovy. Because Groovy runs on the JVM, you can use any Java HTML parsing library to pull data from HTML into Deephaven tables.

[jsoup](https://jsoup.org/) is a popular Java library for parsing HTML, similar to [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) in Python. It parses malformed HTML, and it can both download and parse a web page.

Deephaven doesn't include jsoup, so add it to the classpath before you use it. See [Install and use Java packages](../install-and-use-java-packages.md) for instructions on how to add external JARs to the Deephaven server's classpath.

This guide reads an HTML table with jsoup and then shows how to give the imported columns the correct [data types](#data-types).

## Read an HTML table with jsoup

Let's start with an example that reads a small HTML table of stock trades from a string. The code uses three jsoup methods:

- [`Jsoup.parse`](https://jsoup.org/apidocs/org/jsoup/Jsoup.html#parse(java.lang.String)) parses the HTML.
- [`Element.select`](https://jsoup.org/apidocs/org/jsoup/nodes/Element.html#select(java.lang.String)) finds the header cells and body rows with CSS selectors.
- [`text`](https://jsoup.org/apidocs/org/jsoup/nodes/Element.html#text()) returns each cell's text.

It then builds a Deephaven table with [`newTable`](../../reference/table-operations/create/newTable.md), with one [`stringCol`](../../reference/table-operations/create/stringCol.md) per header cell:

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

The result, `trades`, is a Deephaven table with one row for each of the three trades and four `String` columns: `Symbol`, `Price`, `Size`, and `Timestamp`.

To read a different source, replace `Jsoup.parse(tradesHtml)`:

- For a local file, use `Jsoup.parse(new File("/path/to/file.html"), "UTF-8")`.
- For a web page, use `Jsoup.connect(url).get()`. [`Jsoup.connect`](https://jsoup.org/apidocs/org/jsoup/Jsoup.html#connect(java.lang.String)) creates a connection for the URL, and [`get`](https://jsoup.org/apidocs/org/jsoup/Connection.html#get()) downloads and parses the page.

## Data types

HTML tables store all data as plain text and have no concept of data types, so every column in `trades` is a `String` column. You can confirm this with [`meta`](../../reference/table-operations/metadata/meta.md).

The jsoup example above builds `trades` from HTML. So that you can run this section without jsoup, the following code builds the same table directly with [`newTable`](../../reference/table-operations/create/newTable.md) and [`stringCol`](../../reference/table-operations/create/stringCol.md):

```groovy test-set=html-import order=trades
trades = newTable(
    stringCol("Symbol", "AAPL", "MSFT", "GOOG"),
    stringCol("Price", "226.50", "409.44", "163.38"),
    stringCol("Size", "100", "250", "75"),
    stringCol("Timestamp", "2024-09-03T13:30:00Z", "2024-09-03T13:31:15Z", "2024-09-03T13:32:40Z")
)
```

To give each column the right type, use [`update`](../../reference/table-operations/select/update.md) or another [selection](../use-select-view-update.md) method. This example uses the [Java](../java-classes.md#methods-and-constructors) `Double.parseDouble` and `Integer.parseInt` methods and the [built-in](../built-in-functions.md) [`parseInstant`](../../reference/time/datetime/parseInstant.md) function:

```groovy test-set=html-import order=tradesTyped
tradesTyped = trades.update(
    "Price = Double.parseDouble(Price)",
    "Size = Integer.parseInt(Size)",
    "Timestamp = parseInstant(Timestamp)"
)
```

The result is an ordinary Deephaven table, so you can filter it, aggregate it, or join it with your other tables, including tables that update in real time.

## Related documentation

- [Convert tables to HTML strings](./html-export.md)
- [Install and use Java packages](../install-and-use-java-packages.md)
- [Select and create columns](../use-select-view-update.md)
- [Java objects and classes in query strings](../java-classes.md)
- [Built-in query language functions](../built-in-functions.md)
- [`newTable`](../../reference/table-operations/create/newTable.md)
- [`stringCol`](../../reference/table-operations/create/stringCol.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`meta`](../../reference/table-operations/metadata/meta.md)
- [`parseInstant`](../../reference/time/datetime/parseInstant.md)
