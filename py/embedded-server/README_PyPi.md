# deephaven-server

A Python-native way of launching [deephaven-core](https://www.github.com/deephaven/deephaven-core).

Deephaven Community Core is a real-time, time-series, column-oriented analytics engine with relational database features. It is an open version of [Deephaven Enterprise](https://deephaven.io/enterprise/), which serves as the data backbone for prominent hedge funds, banks, and financial exchanges.

## Requirements

Java 11+ is required for this module, and the `JAVA_HOME` environment variable must be set appropriately.

This module also requires Python version 3.8 or newer.

## Setup

```shell
pip3 install --upgrade pip setuptools wheel
pip3 install deephaven-server
```

## Quick start

```python
from deephaven_server import Server
server = Server()
server.start()

from deephaven import time_table
ticking_table = time_table('PT1s').update_view(formulas=["Col1 = i % 2"])
```
