---
title: Use Application Mode libraries
---

This guide shows you how to use the [`ApplicationContext`](/core/javadoc/io/deephaven/appmode/ApplicationContext.html) and [`ApplicationState`](/core/javadoc/io/deephaven/appmode/ApplicationState.html) classes from the `io.deephaven.appmode` package in [Application Mode scripts](./application-mode-script.md). These classes give you more control over Deephaven's state during launch.

[`ApplicationContext`](/core/javadoc/io/deephaven/appmode/ApplicationContext.html) provides access to the [`ApplicationState`](/core/javadoc/io/deephaven/appmode/ApplicationState.html) used during the initial startup of a Deephaven server that uses Application Mode.

Use [`ApplicationContext.get()`](/core/javadoc/io/deephaven/appmode/ApplicationContext.html) to read the [`ApplicationState`](/core/javadoc/io/deephaven/appmode/ApplicationState.html), or [`ApplicationContext.initialize(...)`](/core/javadoc/io/deephaven/appmode/ApplicationContext.html) to pass it to a closure you supply. Use `initialize` for scripts split across multiple Groovy files; the `file_N` properties in the [Application Mode config file](../reference/app-mode/application-mode-config.md) list those files. Both work only during startup; see [When the application state is available](#when-the-application-state-is-available).

[`ApplicationState`](/core/javadoc/io/deephaven/appmode/ApplicationState.html) publishes tables and other objects as application fields, which the web IDE and API clients can open. A field is a named object, such as a table, that the server publishes to clients. For more on fields, see [Initialize server state with Application Mode](./application-mode.md).

## Example

The following example uses [`ApplicationContext`](/core/javadoc/io/deephaven/appmode/ApplicationContext.html) and [`ApplicationState`](/core/javadoc/io/deephaven/appmode/ApplicationState.html) to create tables within Deephaven through Application Mode.

```groovy skip-test
import io.deephaven.appmode.ApplicationContext
import io.deephaven.appmode.ApplicationState

def start = { ApplicationState app ->
    def size = 42
    app.setField("hello", emptyTable(size))
    app.setField("world", timeTable("PT1S"))
}

ApplicationContext.initialize(start)
```

## When the application state is available

[`ApplicationContext.get()`](/core/javadoc/io/deephaven/appmode/ApplicationContext.html) and [`ApplicationContext.initialize(...)`](/core/javadoc/io/deephaven/appmode/ApplicationContext.html) work only while the files of an Application Mode script run during startup. Calling either one at any other time throws an `IllegalStateException`.

Deephaven runs every file listed in the `file_N` properties under the same [`ApplicationContext`](/core/javadoc/io/deephaven/appmode/ApplicationContext.html), so each file gets the same [`ApplicationState`](/core/javadoc/io/deephaven/appmode/ApplicationState.html). If a script needs to add fields later, it must keep a reference to the [`ApplicationState`](/core/javadoc/io/deephaven/appmode/ApplicationState.html).

## Add or replace a field

Call [`setField(name, value)`](/core/javadoc/io/deephaven/appmode/ApplicationState.html) to add a field. If a field with that name already exists, the call replaces it.

```groovy skip-test
import io.deephaven.appmode.ApplicationContext
import io.deephaven.appmode.ApplicationState

ApplicationState app = ApplicationContext.get()
app.setField("hello", emptyTable(42))
app.setField("hello", timeTable("PT1S")) // replaces the earlier "hello" field
```

To attach a description to a field, pass it as a third argument. Clients can read the description. Pass a non-empty description; an empty string throws `IllegalArgumentException`.

```groovy skip-test
import io.deephaven.appmode.ApplicationContext
import io.deephaven.appmode.ApplicationState

ApplicationState app = ApplicationContext.get()
app.setField("world", timeTable("PT1S"), "A table that ticks once per second")
```

> [!NOTE]
> Fields set on [`ApplicationState`](/core/javadoc/io/deephaven/appmode/ApplicationState.html) appear in the web IDE's **Panels** menu and are available to API clients, but they are not added to the [query scope](./query-scope.md). To refer to one of these tables by name in console code, also add it to the [query scope](./query-scope.md) in the script, for example by assigning it to a global variable.

## Read a field

Call [`getField(name)`](/core/javadoc/io/deephaven/appmode/ApplicationState.html) to read a field. It returns a `Field` object, or `null` if no field has that name. Check the result for `null` before you call `value` on it. The `Field` object holds the field's name, value, and description (see [List fields](#list-fields)).

```groovy skip-test
import io.deephaven.appmode.ApplicationContext
import io.deephaven.appmode.ApplicationState

ApplicationState app = ApplicationContext.get()
def field = app.getField("hello")
if (field != null) {
    def hello = field.value()
}
```

## List fields

Call [`listFields()`](/core/javadoc/io/deephaven/appmode/ApplicationState.html) to get every field as a list of `Field` objects. Each `Field` has `name()`, `value()`, and `description()` methods. `description()` returns an `Optional<String>`, so the example uses `orElse` for fields without one.

```groovy skip-test
import io.deephaven.appmode.ApplicationContext
import io.deephaven.appmode.ApplicationState

ApplicationState app = ApplicationContext.get()
app.listFields().each { field ->
    println "${field.name()}: ${field.description().orElse("no description")}"
}
```

## Remove a field

Call [`removeField(name)`](/core/javadoc/io/deephaven/appmode/ApplicationState.html) to remove a field. If no field has that name, the call does nothing.

```groovy skip-test
import io.deephaven.appmode.ApplicationContext
import io.deephaven.appmode.ApplicationState

ApplicationState app = ApplicationContext.get()
app.removeField("hello")
```

## Related documentation

- [How to use Application Mode video](https://youtu.be/GNm1k0WiRMQ)
- [How to use Application Mode scripts](./application-mode-script.md)
- [Query scope](./query-scope.md)
- [Initialize server state with Application Mode](./application-mode.md)
- [Application Mode config file](../reference/app-mode/application-mode-config.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`ApplicationContext` Javadoc](/core/javadoc/io/deephaven/appmode/ApplicationContext.html)
- [`ApplicationState` Javadoc](/core/javadoc/io/deephaven/appmode/ApplicationState.html)
