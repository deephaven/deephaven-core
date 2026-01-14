---
title: Use Application Mode libraries
sidebar_label: Application Mode libraries
---

This guide shows you how to use the main library for [Application Mode scripts](./application-mode-script.md), `ApplicationState`. This library gives you more control over Deephaven's state during launch.

`ApplicationState` is used to interact with the Deephaven server within Application Mode. Values set on an instance of this class interact with fields on the Deephaven server.

## Example

The following example uses `ApplicationState` to create tables within Deephaven through Application Mode.

```python skip-test
from deephaven.appmode import ApplicationState, get_app_state
from deephaven import time_table, empty_table

from typing import Callable


def start(app: ApplicationState):
    size = 42
    app["hello"] = empty_table(size)
    app["world"] = time_table("PT1S")


def initialize(func: Callable[[ApplicationState], None]):
    app = get_app_state()
    func(app)


initialize(start)
```

## Related documentation

- [How to use Application Mode video](https://youtu.be/GNm1k0WiRMQ)
- [How to use Application Mode scripts](./application-mode-script.md)
- [Initialize server state with Application Mode](./application-mode.md)
- [Application Mode config file](../reference/app-mode/application-mode-config.md)
