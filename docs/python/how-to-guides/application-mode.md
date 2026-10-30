---
title: Application Mode
---

<div className="comment-title">

Hands-free. Production-ready.

</div>

When launching Deephaven, you may want to initialize the server state before any client connects to the server. Application Mode enables users to bind tables, plots, functions, and other objects to named fields that clients can open while the server runs. Deephaven calls these named fields exported fields.

A script application can also bind objects to [REPL variables](https://en.wikipedia.org/wiki/Read-eval-print_loop).

Exported fields appear in the IDE under the **Panels** menu. Use this to share helper functions or full dashboards that you build as scripts.

Each application is described by an `*.app` file. The [Syntax](#syntax) section lists the keys in these files, for reference.

This guide shows how to create a script application that exports tables to the IDE, and summarizes the other application types and the server settings that control them. To package scripts into a Docker image, see [How to use Application Mode scripts](./application-mode-script.md). For Application Mode libraries, see [How to use Application Mode libraries](./application-mode-libraries.md).

## Application types

There are many different applications that you can run, from a Python or Groovy script designed to run in Deephaven, to a dynamic application written in any [JVM-based language](https://en.wikipedia.org/wiki/List_of_JVM_languages).

- Script application (`type=script`): Run a Python or Groovy script when the server starts. See the [Script application example](#script-application-example).
- Static application (`type=static`): Configure an application that declares all of its fields when the server creates the application. Use it when all fields are known and accessible at all times. See [Static applications](#static-applications).
- Dynamic application (`type=dynamic`): Configure an application whose exported fields can change over its lifetime. Use it when fields might be created or removed over time, or as data changes. See [Dynamic applications](#dynamic-applications).

## Script application example

In this example, we seed the files needed to use Application Mode, start the Deephaven server, and then see the results in the server log and the IDE. This example assumes you run Deephaven with Docker Compose, using the `./data` volume from Deephaven's examples.

> [!NOTE]
> The Docker Compose files in Deephaven's examples mount `./data` in the local deployment directory to `/data` in the running Deephaven containers. See our guide on [Docker data volumes](../conceptual/docker-data-volumes.md) for more information.

Create an `app.d` folder inside your `./data` directory:

```bash
mkdir data/app.d
```

The server reads all files with an `.app` extension in the `app.d` folder and executes each one individually in alphabetical order.

As an example `.app` file, create the file `firstApp.app` with the following contents.

```properties
type=script
scriptType=python
enabled=true
id=hello.world
name=Hello World!
file_0=firstApp.py
```

This application file runs the script named in the `file_` line. For this example, the script is `firstApp.py`, in the same directory as the application file. The script is:

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

In our "hello world" example, we get the application state through the [`get_app_state`](/core/pydoc/code/deephaven.appmode.html#deephaven.appmode.get_app_state) function. [`ApplicationState`](/core/pydoc/code/deephaven.appmode.html) collects the state of each application. The `initialize` function gets the application state with [`get_app_state`](/core/pydoc/code/deephaven.appmode.html#deephaven.appmode.get_app_state) and passes it to `start`, so the server runs `start` when it loads this file.

Setting a field on the provided [`ApplicationState`](/core/pydoc/code/deephaven.appmode.html) exports it to connecting clients, as described in the introduction.

Each application has its own state that it can manipulate. This enables similar applications to co-exist without stepping on one another.

In this example, we have one method (`start`) that runs when the container starts. Inside this method, we set two fields:

1. An [empty table](../reference/table-operations/create/emptyTable.md).
2. A [time table](../reference/table-operations/create/timeTable.md) that updates every second.

> [!NOTE]
> Script applications may directly manipulate variables bound in the [REPL](https://en.wikipedia.org/wiki/Read-eval-print_loop) in addition to, or instead of, using the [`ApplicationState`](/core/pydoc/code/deephaven.appmode.html) portion of the Application Mode API.

Once you have these two files inside the `app.d` directory, configure the server to find them.

> [!NOTE]
> The Docker images already set `deephaven.console.type` (`python` for `server`, `groovy` for `server-slim`). Set the other runtime flags in `START_OPTS` in your `docker-compose.yml`. See [Runtime JVM flags](#runtime-jvm-flags).

Before you start the containers, tell the server where to find the `app.d` folder, as described in [Runtime JVM flags](#runtime-jvm-flags). Add `-Ddeephaven.application.dir=/data/app.d` to `START_OPTS` in your `docker-compose.yml`:

```yaml
environment:
  - START_OPTS=-Xmx4g -Ddeephaven.application.dir=/data/app.d
```

Inside your Deephaven directory, start the containers with `docker compose up`.

```bash
docker compose up
```

In the server log at startup, you see two exported fields, one for each field the script sets.

![Log readout with the exports highlighted](../assets/how-to/appMode.png)

Launch the Deephaven IDE in your web browser. Open the **Panels** menu to see the defined tables `hello` and `world`.

![The Deephaven IDE, with the Panels menu highlighted](../assets/how-to/appMode1.png)

## Multiple script application example

The [Script application example](#script-application-example) defines the tables `hello` and `world` in `firstApp.py`. A second script can use those tables, and it can make them available in the IDE by assigning them to the [query scope](./query-scope.md).

Here we add to the `.app` file to run another script:

```properties
type=script
scriptType=python
enabled=true
id=hello.world
name=Hello World!
file_0=firstApp.py
file_1=secondApp.py
```

The fields that `firstApp.py` sets through [`ApplicationState`](/core/pydoc/code/deephaven.appmode.html) are available to `secondApp.py` through `app["world"]`, as the example below shows. Here we keep `file_0` the same, and add a `file_1` defined as:

```python skip-test
from deephaven.appmode import ApplicationState, get_app_state

from typing import Callable


def start(app: ApplicationState):
    global my_new_table
    my_new_table = app["world"]
    my_new_table = my_new_table.update(formulas=["A = i"])


def initialize(func: Callable[[ApplicationState], None]):
    app = get_app_state()
    func(app)


initialize(start)
```

Declaring `my_new_table` with `global` makes it a module-level variable, which the script session adds to the [query scope](./query-scope.md). The other tables are only available in the **Panels** menu. If you want to perform queries on fields defined in Application Mode, you must explicitly assign them to the [query scope](./query-scope.md) inside the applications.

Files run in numerical order (after removing the prefix `file_`), not in the order listed in the application. Because of this, an early file can initialize values that later files use.

## Runtime JVM flags

The following flags are available for configuration:

- `-Ddeephaven.application.dir=/path/to/application/root` — The Deephaven server loads all `*.app` files in alphabetical order from the given directory during initialization, before it begins to listen for requests from connecting clients.
  - If this property is not set, the server uses the `app.d` directory inside its configuration directory, if one exists.
- `-Ddeephaven.console.type=$SCRIPT_LANGUAGE_TYPE` — Specifies which language type the Deephaven server uses for Application Mode and console [REPL](https://en.wikipedia.org/wiki/Read-eval-print_loop) sessions. The default is `python`. The server supports one script type per instance, so every script application must match this value.
- `-Ddeephaven.console.disable=$BOOLEAN` — Prevents API clients, including Deephaven's web client, from starting REPL sessions. The default is `false`.

> [!WARNING]
> A REPL session gives its user the same privileges on the host machine as the user running the server process.

## Syntax

Deephaven expects the following keys to be in an `*.app` file:

- `type` — Required. One of `script`, `static`, or `dynamic`.
- `scriptType` — Required for `script` applications: `python` or `groovy`.
- `enabled` — Set to `true` or `false` to run or skip the scripts in the application file. The default is `true`.
- `id` — Required for `script` applications. An identifier shown only to API clients, which refer to an application by `id`.
- `name` — Required for `script` applications. The label that users see for this application. Changing `name` does not affect saved layouts, because clients refer to the application by `id`.
- `file_N` — Required for `script` applications. Set at least one. Path to each script to run.
- `class` — Required for `static` and `dynamic` applications: the fully qualified name of the factory class.

> [!WARNING]
> The Deephaven server supports only one script type per instance. If you attempt to load the wrong type of script application, then the server fails fast to aid in debugging the misconfiguration. See [deephaven-core#1172](https://github.com/deephaven/deephaven-core/issues/1172) for more details.

The [Application Mode configuration reference](../reference/app-mode/application-mode-config.md) documents each script key in more detail, including the default for `enabled`.

## Static and dynamic applications

Besides script applications, Deephaven supports static and dynamic applications. You write these in a [JVM-based language](https://en.wikipedia.org/wiki/List_of_JVM_languages) such as Java, Scala, or Kotlin, and then export them to clients, such as the IDE's **Panels** menu. Be sure to [package these into a jar file](https://docs.oracle.com/javase/tutorial/deployment/jar/) and provide them as third-party dependencies to Deephaven's server. See [How to install and use Java packages](./install-and-use-java-packages.md) for more information.

### Static applications

Static applications define all their fields when the server creates the application.

Implement [`io.deephaven.appmode.Application.Factory`](/core/javadoc/io/deephaven/appmode/Application.html). Its `create` method returns an [`Application`](/core/javadoc/io/deephaven/appmode/Application.html) with its id, name, and fields.

The `.app` configuration file is as follows:

```properties
type=static
class=fully.qualified.class.Name
enabled=true
```

### Dynamic applications

Dynamic applications can create and remove fields while the server runs.

Implement [`io.deephaven.appmode.ApplicationState.Factory`](/core/javadoc/io/deephaven/appmode/ApplicationState.html). Its `create(listener)` method returns an [`ApplicationState`](/core/javadoc/io/deephaven/appmode/ApplicationState.html) you can add fields to and remove fields from over time.

The `.app` configuration file is as follows:

```properties
type=dynamic
class=fully.qualified.class.Name
enabled=true
```

## Related documentation

- [Docker data volumes](../conceptual/docker-data-volumes.md)
- [How to install and use Java packages](./install-and-use-java-packages.md)
- [How to use Application Mode libraries](./application-mode-libraries.md)
- [How to use Application Mode scripts](./application-mode-script.md)
- [Query scope](./query-scope.md)
- [Application Mode configuration reference](../reference/app-mode/application-mode-config.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`deephaven.appmode` Pydoc](/core/pydoc/code/deephaven.appmode.html)
- [`get_app_state` Pydoc](/core/pydoc/code/deephaven.appmode.html#deephaven.appmode.get_app_state)
- [`Application` Javadoc](/core/javadoc/io/deephaven/appmode/Application.html)
- [`ApplicationState.Factory` Javadoc](/core/javadoc/io/deephaven/appmode/ApplicationState.html)
- [How to use Application Mode video](https://youtu.be/GNm1k0WiRMQ)
