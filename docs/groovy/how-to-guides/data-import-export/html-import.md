---
title: Read HTML files into Deephaven tables
---

Deephaven does not provide built-in methods for reading HTML tables in Groovy. Because Groovy runs on the JVM, you can use any Java HTML parsing library to pull data from HTML into Deephaven tables.

[Jsoup](https://jsoup.org/) is a popular Java library for parsing HTML, similar to [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) in Python. It handles malformed real-world HTML gracefully and bundles both HTTP fetching and HTML parsing in a single dependency. It is not included in Deephaven by default and must be added to the classpath before use. See [Install and use Java packages](../install-and-use-java-packages.md) for instructions on how to add external JARs to a Deephaven worker.

Once Jsoup is available, you can use it in Groovy scripts to fetch and parse HTML tables, then construct Deephaven tables from the extracted data using [`newTable`](/core/javadoc/io/deephaven/engine/util/TableTools.html#newTable(io.deephaven.engine.table.impl.util.ColumnHolder...)) and column factory methods such as `stringCol`.

## Related documentation

- [Export HTML files](./html-export.md)
- [Install and use Java packages](../install-and-use-java-packages.md)
