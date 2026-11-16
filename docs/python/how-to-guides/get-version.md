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
- The Python, Java, Groovy, and [Barrage](../conceptual/what-is-barrage.md) versions.
- The browser and operating system.

In **Version Info**, the **Copy Versions** button copies this information to your clipboard so you can paste it into an issue. It also copies the version of each [web UI plugin](./install-use-plugins.md) that reports one.

To see a plugin's version without copying anything, look at the **Plugins** list below the **Version** entry in the **Settings** menu.

## From code

To get the server version from code, print `deephaven.__version__`, which reports the version of the [`deephaven-core` Python package](./deephaven-python-package.md) installed with the server:

```python order=:log
from deephaven import __version__

print(__version__)
```

For release builds, this matches the engine version in **Version Info**. When the server version has a qualifier, as development builds do, the Python package records it in [PEP 440](https://peps.python.org/pep-0440/#local-version-identifiers) form, so `43.0-SNAPSHOT` appears as `43.0+snapshot`.

## Related documentation

- [What is Barrage?](../conceptual/what-is-barrage.md)
- [Deephaven's Python package](./deephaven-python-package.md)
- [Install and use plugins](./install-use-plugins.md)
- [Navigate the GUI](./user-interface/navigating-the-ui.md)
- [Retrieve logs](./logs.md)
- [Triage errors in queries](./triage-errors.md)
- [How can I find which Core version added a particular feature or fix?](../reference/community-questions/how-do-i-find-a-specific-build.md)
- [Version matrix](../reference/version-matrix.md)
- [Pydoc](/core/pydoc/code/deephaven.html#module-deephaven)
