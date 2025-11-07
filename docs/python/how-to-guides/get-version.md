---
title: Get version information
---

To find the web client version you are running, open the **Settings** menu:

![The version number highlighted in the Settings menu](../assets/how-to/version_number.png)

To get the server version, print the `deephaven.__version__` variable:

```python
from deephaven import __version__

print(__version__)
```
