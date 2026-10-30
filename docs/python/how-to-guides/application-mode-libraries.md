---
title: Use Application Mode libraries
---

This guide shows you how to use the [`ApplicationState`](/core/pydoc/code/deephaven.appmode.html) class and the [`get_app_state`](/core/pydoc/code/deephaven.appmode.html) function from the `deephaven.appmode` module in [Application Mode scripts](./application-mode-script.md). These give you more control over Deephaven's state during launch. The [`get_app_state`](/core/pydoc/code/deephaven.appmode.html) function returns the state only during startup; see [When the application state is available](#when-the-application-state-is-available).

[`ApplicationState`](/core/pydoc/code/deephaven.appmode.html) publishes tables and other objects as application fields, which the web IDE and API clients can open. A field is a named object, such as a table, that the server publishes to clients. For more on fields, see [Initialize server state with Application Mode](./application-mode.md).

## Example

The following example uses [`ApplicationState`](/core/pydoc/code/deephaven.appmode.html) to create tables within Deephaven through Application Mode.

```python skip-test
from deephaven.appmode import ApplicationState, get_app_state
from deephaven import time_table, empty_table


def start(app: ApplicationState):
    size = 42
    app["hello"] = empty_table(size)
    app["world"] = time_table("PT1S")


start(get_app_state())
```

## When the application state is available

[`get_app_state`](/core/pydoc/code/deephaven.appmode.html) returns the application state only while the files of an Application Mode script run during startup. Calling [`get_app_state`](/core/pydoc/code/deephaven.appmode.html) at any other time raises a `DHError`.

Deephaven runs every file listed in the `file_N` entries of the [Application Mode config file](../reference/app-mode/application-mode-config.md) in the same startup run, so each file gets the same [`ApplicationState`](/core/pydoc/code/deephaven.appmode.html). If a script needs to add fields later, it must keep a reference to the [`ApplicationState`](/core/pydoc/code/deephaven.appmode.html).

## Add or replace a field

Assign a value to a name on the [`ApplicationState`](/core/pydoc/code/deephaven.appmode.html) instance. If a field with that name already exists, the assignment replaces it.

```python skip-test
from deephaven import empty_table, time_table
from deephaven.appmode import get_app_state

app = get_app_state()
app["hello"] = empty_table(42)
app["hello"] = time_table("PT1S")  # replaces the earlier "hello" field
```

> [!NOTE]
> Fields set on [`ApplicationState`](/core/pydoc/code/deephaven.appmode.html) appear in the web IDE's **Panels** menu and are available to API clients, but they are not added to the [query scope](./query-scope.md). To refer to one of these tables by name in console code, also add it to the [query scope](./query-scope.md) in the script, for example by assigning it to a global variable.

## Read a field

Use the field name as a key to get its value. If no field has that name, the lookup raises a `KeyError`.

```python skip-test
from deephaven.appmode import get_app_state

app = get_app_state()
hello = app["hello"]
```

## List fields

The [`fields`](/core/pydoc/code/deephaven.appmode.html) property of [`ApplicationState`](/core/pydoc/code/deephaven.appmode.html) returns a dictionary that maps each field name to its value.

```python skip-test
from deephaven.appmode import get_app_state

app = get_app_state()
for name, value in app.fields.items():
    print(name, type(value))
```

## Known issues

- Removing a field with `del` removes it, but the statement always raises a `KeyError`, even though the field is gone. To remove a field without that error, call `removeField(name)` from a Groovy Application Mode script; see [remove a field in Groovy](/core/groovy/docs/how-to-guides/application-mode-libraries#remove-a-field).
- The Python API doesn't accept a description for a field. To set one, use Groovy; see [add or replace a field in Groovy](/core/groovy/docs/how-to-guides/application-mode-libraries#add-or-replace-a-field).

## Related documentation

- [How to use Application Mode video](https://youtu.be/GNm1k0WiRMQ)
- [How to use Application Mode scripts](./application-mode-script.md)
- [Query scope](./query-scope.md)
- [Initialize server state with Application Mode](./application-mode.md)
- [Application Mode config file](../reference/app-mode/application-mode-config.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`ApplicationState` Pydoc](/core/pydoc/code/deephaven.appmode.html)
- [`get_app_state` Pydoc](/core/pydoc/code/deephaven.appmode.html)
