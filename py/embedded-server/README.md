# Deephaven Python Embedded Server

Embeds the Deephaven Core server into a python module, so that it can be started from python, with tables produced
directly as part of startup.

## Dev environment setup
Java 11 and Docker are required to build this project, as most of the repository is needed to properly build it.
Note that jpy and the deephaven server api wheel is also required. 

## Build
From the root directory of this repository
```shell
$ ./gradlew :py-embedded-server:assemble
```

## Install
```shell
$ pip install py/embedded-server/build/wheel/deephaven_server-0.12.0-py3-none-any.whl
```

## Quick start

```python
from deephaven_server import Server
server = Server()
server.start()

from deephaven import time_table
ticking_table = time_table('PT1s').update_view(formulas=["Col1 = i % 2"])
```
