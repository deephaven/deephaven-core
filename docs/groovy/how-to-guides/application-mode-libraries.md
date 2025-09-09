---
title: Use Application Mode libraries
sidebar_label: Application Mode libraries
---

This guide shows you how to use the two main libraries for [Application Mode scripts](./application-mode-script.md), `ApplicationContext` and `ApplicationState`. These libraries give you more control over Deephaven's state during launch.

`ApplicationContext` is a gateway to the application state used during the initial startup of a Deephaven server using Application Mode. This is useful when using complicated scripts that require multiple Python/Groovy files, defined by the `file_N` fields in the `*.app` file. Deephaven maintains the state of these classes between each file through the `ApplicationContext` class.

`ApplicationState` is used to interact with the Deephaven server within Application Mode. The `setField(str: name, T: value)` and `getField(str: name).value()` methods are used to interact with fields on the Deephaven server.

## Example

The following example uses `ApplicationContext` and `ApplicationState` to create tables within Deephaven through Application Mode. We use `ApplicationContext` to grab the current `ApplicationState`, and then use `setField` to set the tables within Deephaven.

> [!NOTE]
> The tables created below are not bound to a global variable. Because of this, they are only useable within an Application Mode script, and not within the Deephaven Console.
>
> If you want to use the tables within the Deephaven Console, you must bind them to a global variable.

```groovy skip-test
import io.deephaven.appmode.ApplicationContext
import io.deephaven.appmode.ApplicationState

def start = { ApplicationState app ->
    size = 42
    app.setField("hello", emptyTable(size))
    app.setField("world", timeTable("00:00:01"))
}

ApplicationContext.initialize(start)
```

## Related documentation

- [How to use Application Mode video](https://youtu.be/GNm1k0WiRMQ)
- [How to use Application Mode scripts](./application-mode-script.md)
- [Initialize server state with Application Mode](./app-mode.md)
- [Application Mode config file](../reference/app-mode/application-mode-config.md)
