# Implementation

The Deephaven SQL integration is driven by [Apache Calcite](https://github.com/apache/calcite), a mature project that
supports a wide variety of industry and enterprise use-cases. Calcite is very configurable and modular, making it
applicable for many SQL use-cases. That said, the configurability and modularity, not to mention the SQL standard
itself, makes wielding Calcite a non-trivial task.

Deephaven is using Calcite only for its SQL relational parsing ability; those parsed results are then visited and
transformed into "approximately equivalent" Deephaven table operations. There's potential in the future to use more
Calcite features such as SQL query optimization, or as a means of executing the plan and transforming the results back
into Calcite result types (this may allow Calcite to serve as an ODBC / JDBC endpoint).

The integration is currently only exposed as a script-session function call - it is _not_ exposed via a "proper" SQL api
such as ODBC, JDBC, or Flight SQL.

## Column references

Calcite treats all references as indexes (with callers responsible for knowing the proper context in which those indexes
exist); see org.apache.calcite.rex.RexInputRef.

Since Deephaven refers to everything by name instead of index, there needs to be some translation layer that bridges
this gap. An "index-string" strategy is used to name columns internally based on their SQL / calcite field index. Of
course, the final node in the dag (the table the user actually cares about), needs to be named appropriately as the user
expects.

This strategy of maintaining internal names that are different than the final names may lead to "excessive" or confusing
operations that don't seem to serve much purpose. In fact though, this strategy greatly simplifies and aids in the
ease-of-implementation and maintenance of these adapting layers. There's potential to improve this in the future with
SQLTODO(qst-optimization).
