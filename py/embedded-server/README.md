# Deephaven Python Embedded Server

Embeds the Deephaven Core server into a python module, so that it can be started from python, with tables produced
directly as part of startup.

## Dev envirnoment setup
Java 11 and Docker are required to build this project, as most of the repository is needed to properly build it.
Note that jpy or deephaven-jpy (built for your OS and archetecture) and the deephaven server apiwheel is also
required. 

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
>>> from deephaven_server import *
>>> s = Server()
>>> s.start()

>>> from deephaven import *
>>> ticking_table = time_table('00:00:01').update_view(formulas=["Col1 = i % 2"])


```