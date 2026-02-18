---
title: Get version information
---

To find the web client version you are running, open the **Settings** menu:

![The version number highlighted in the Settings menu](../assets/how-to/version_number.png)

To get the server version, run the following query:

```groovy
println io.deephaven.engine.table.Table.class.getPackage().getImplementationVersion()
```

## Related documentation

- [Retrieve logs](./logs.md)
- [Triage errors in queries](./triage-errors.md)
