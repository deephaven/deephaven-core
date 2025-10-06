---
title: An Overview of Deephaven's Architecture
sidebar_label: Architecture Overview
---

Deephaven's power is largely due to the concept that _everything_ is a table. Think of Deephaven tables like [Pandas DataFrames](https://pandas.pydata.org/pandas-docs/stable/user_guide/dsintro.html#dataframe), except that they support real-time operations! The Deephaven table is the key abstraction that unites static and real-time data for a seamless, integrated experience. This section will discuss the conceptual building blocks of Deephaven tables before diving into some real code.

## The Deephaven Table API

If you're familiar with tabular data (think Excel, Pandas, and more), Deephaven will feel natural to you. In addition to representing data as tables, you must be able to manipulate, transform, extract, and analyze the data to produce valuable insight. Deephaven represents transformations to the data stored in a table as _operations_ on that table. These table operations can be chained together to generate a new perspective on the data:

```python order=new_table,source_table
from deephaven import new_table
from deephaven.column import string_col, int_col

source_table = new_table(
    [string_col("Group", ["A", "B", "A", "B"]), int_col("Data", [1, 2, 3, 4])]
)

new_table = (
    source_table.update_view("NewData = Data * 2")
    .sum_by("Group")
    .rename_columns(["DataSum=Data", "NewDataSum=NewData"])
)
```

This paradigm of representing data transformations with methods that act on tables is not new. However, Deephaven takes this a step further. In addition to its large collection of table operations, Deephaven provides a vast library of efficient functions and operations, accessible through _query strings_ that get invoked from _within_ table operations. These query strings, once understood, are a superpower that is totally unique to Deephaven. More on these later.

Collectively, the representation of data as tables, the table operations that act upon them, and the query strings used within table operations are known as the _Deephaven Query Language_, or DQL for short. The data transformations written with DQL (like the code above) are called _Deephaven queries_. The fundamental principle of the Deephaven experience is this:

<Pullquote>
Deephaven queries are unaware and indifferent to whether the underlying data source is static or streaming.
</Pullquote>

The magic here is hard to overstate. The same queries written to parse a simple CSV file can be used to analyze a similarly fashioned living dataset, evolving at millions of rows per second, without changing the query itself. Real-time analysis doesn't get simpler.

### Immutable schema, mutable data

When Deephaven tables are created, they follow a well-defined recipe that _cannot_ be modified after creation. This means that the number of columns in a table, the column names, and their data types - collectively known as the table's _schema_ - are immutable. Even so, the data stored in a table can change. Here's a simple example that uses [`time_table`](../../reference/table-operations/create/timeTable.md):

```python test-set=1 ticking-table order=null
from deephaven import time_table

# Create a table that ticks once per second - defaults to a single "Timestamp" column
ticking_table = time_table("PT1s")
```

![The above ticking table](../../assets/tutorials/crash-course/crash-course-1.gif)

New data is added to the table each second, but the schema stays the same.

Some queries appear to modify a table's schema. They may add or remove a column, modify a column's type, or rename a column. For example, the following query appears to rename the `Timestamp` column in the table to `RenamedTimestamp`:

```python test-set=1 ticking-table order=null
# Apply 'rename_columns' table operation to ticking_table
ticking_table = ticking_table.rename_columns("RenamedTimestamp=Timestamp")
```

![The above ticking table](../../assets/tutorials/crash-course/crash-course-2.gif)

Since the schema of `ticking_table` is immutable, its columns cannot be renamed. Instead, Deephaven creates a new table with a new schema. Deephaven is smart about creating the new table so that only a tiny amount of additional memory is used to make it. An important consequence of this design is that Deephaven does not support in-place operations:

```python skip-test
# In-place operation - DON'T DO THIS! ticking_table is not changed.
ticking_table.update_view("NewColumn = 1")
```

See how that operation has no effect. Instead, always assign the output of any table operation to a new table, even if it has the same name:

```python test-set=1 order=new_ticking_table,ticking_table
ticking_table = ticking_table.update_view("NewColumn = 1")
new_ticking_table = ticking_table.update_view("RowIndex = ii")
```

## The engine

The Deephaven engine is the powerhouse that implements everything you've seen so far. The engine is responsible for processing streaming data as efficiently as possible, and all Deephaven queries must flow through the engine.

Internally, the engine represents queries as [directed acyclic graphs (DAGs)](../../conceptual/dag.md). This representation is key to Deephaven's efficiency. When a parent (upstream) table changes, a change summary is sent to child (downstream) tables. This compute-on-deltas model ensures that only updated rows in child tables are re-evaluated for each update cycle and allows Deephaven to process millions of rows per second.

For the sake of efficiency and scalability, the engine is implemented mostly in [Java](https://en.wikipedia.org/wiki/Java_(programming_language)). Although you primarily interface with tables and write queries from Python, _all_ of the underlying data in a table is stored in Java data structures, and all low-level operations are implemented in Java. Most Deephaven users will interact with Java at least a little bit, and there are [important things to consider](../../conceptual/python-java-boundary.md) when passing data between Python and Java.

## User interface

The Deephaven user interface (UI) is a web-based application that runs in your browser. It is built with [React](https://react.dev/) and [TypeScript](https://www.typescriptlang.org/) and communicates with the Deephaven engine using the [JavaScript API](../../how-to-guides/use-jsapi.md). The UI provides a rich experience for exploring tables, writing queries, and visualizing data.

Using [deephaven.ui](../crash-course/deephaven-ui.md), you can create interactive web applications that leverage the power of Deephaven tables directly from your Python code. The deephaven.ui library provides a set of React components and hooks that make it easy to build custom dashboards, data explorers, and visualizations. You can use these components to display tables, create charts, and interact with data in real-time.

The UI is also highly extensible. You can add custom widgets, visualizations, and functionality using [JavaScript plugins](../../how-to-guides/install-use-plugins.md#extend-docker-images-for-js-plugins). These plugins can be [installed in your Deephaven server](../../how-to-guides/install-use-plugins.md) configuration and will be automatically loaded when the server starts. Create your own plugin starting with one of the [plugin templates](https://github.com/deephaven/deephaven-plugins/tree/main/templates) available on GitHub.

All components of the Deephaven UI are published as packages to npmjs under the [@deephaven](https://www.npmjs.com/org/deephaven) and [@deephaven-enterprise](https://www.npmjs.com/org/deephaven-enterprise) organizations. These packages can be used and installed in your own web applications. Example React applications that use the Deephaven UI components are available in the [deephaven-react-app](https://github.com/deephaven-examples/deephaven-react-app) and [deephaven-react-app-enterprise](https://github.com/deephaven-examples/deephaven-react-app-enterprise) repositories.

In addition to publishing the packages, all components of the UI are available open-source on GitHub in the [deephaven/web-client-ui](https://github.com/deephaven/web-client-ui) repository. You can explore the source code, contribute to the project, or even fork it to create your own custom version of the Deephaven UI.
