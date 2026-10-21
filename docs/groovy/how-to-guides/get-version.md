---
title: Get version information
---

Deephaven's web UI and server have separate version numbers. When you [report an issue](https://github.com/deephaven/deephaven-core/issues), include both.

The server version also tells you whether your build includes a particular Core feature or fix. [Find the Core release that added it](../reference/community-questions/how-do-i-find-a-specific-build.md), then check that your server version is the same or later.

## In the web UI

Click the gear icon at the top right of the [web UI](./user-interface/navigating-the-ui.md) to open the **Settings** menu. Near the bottom of the menu, the **Version** entry shows the server version: the version of the Deephaven engine that the server runs.

![The version number highlighted in the Settings menu](../assets/how-to/version_number.png)

Click the info icon next to the version number to open **Version Info**. It lists:

- The engine and web UI versions.
- The Java, Groovy, and [Barrage](../conceptual/what-is-barrage.md) versions.
- The browser and operating system.

In **Version Info**, the **Copy Versions** button copies this information to your clipboard so you can paste it into an issue. It also copies the version of each [web UI plugin](./install-use-plugins.md) that reports one.

To see a plugin's version without copying anything, look at the **Plugins** list below the **Version** entry in the **Settings** menu.

## From code

To get the server version from code, print the implementation version of the package that contains the engine's [`Table`](/core/javadoc/io/deephaven/engine/table/Table.html) interface:

```groovy order=:log
println io.deephaven.engine.table.Table.class.getPackage().getImplementationVersion()
```

This prints the same value that the **Version** entry in the **Settings** menu shows.

## Related documentation

- [What is Barrage?](../conceptual/what-is-barrage.md)
- [Install and use plugins](./install-use-plugins.md)
- [Navigate the GUI](./user-interface/navigating-the-ui.md)
- [Retrieve logs](./logs.md)
- [Triage errors in queries](./triage-errors.md)
- [How can I find which Core version added a feature or fix?](../reference/community-questions/how-do-i-find-a-specific-build.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
