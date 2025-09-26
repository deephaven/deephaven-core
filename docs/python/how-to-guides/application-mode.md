---
title: Application Mode
---

<div className="comment-title">

Hands-free. Production-ready.

</div>

When launching Deephaven, you may want to initialize the server state before any client can connect to the server. Application Mode enables users to bind tables, plots, functions, etc. to [REPL variables](https://en.wikipedia.org/wiki/Read-eval-print_loop) and read-only accessors that are available during the runtime of the server process.

The variables, tables, functions, etc. that are exposed in the application are then available on the dashboard. For example, you can create scripts to instantiate helper functions to use inside the IDE. Or, you can even develop scripts for full dashboards. Application Mode allows this work to be more readily shared.

This guide will show you how to use Application Mode and what feature set it offers.

## Application types

There are many different applications that you can run, from a Python or Groovy script designed to run in Deephaven, to a dynamic application written in any [JVM-based language](https://en.wikipedia.org/wiki/List_of_JVM_languages).

- Script Application: Configure an application in the context of the script type of the Deephaven server.
- Static Application: Configure an application that declares all exported variables during server initialization.
- Dynamic Application: Configure an application that may change what variables are exported based on state changes throughout the life of the application.

## Runtime JVM Flags

The following flags are available for configuration:

- `-Ddeephaven.application.dir=/path/to/application/root` - The Deephaven server loads all files that match `*.app` in alphabetical order from the given directory during initialization before it begins to listen for requests to serve connecting clients (including Deephaven’s web user interface). This parameter is not set by default.
- `-Ddeephaven.console.type=$SCRIPT_LANGUAGE_TYPE` - Specifies which language type the Deephaven server will use for Application Mode and console REPL sessions if applicable. The default is `python`.
- `-Ddeephaven.console.disable=$BOOLEAN` - Some applications may wish to prevent API clients, including Deephaven’s web client, from starting REPL sessions. Beware that REPL sessions give users the same privileges on the host machine as the user executing the process. The default is `false`.

> [!NOTE]
> When using Deephaven’s Docker setup, API clients will not be able to access anything that is not directly exposed by the `docker-compose` configuration. The Docker environment configurations `.env` (picked up by default) and `default-groovy.env` set these flags on your behalf.

> [!WARNING]
> At this time, the Deephaven server can only support a single script type per instance. If you attempt to load the wrong type of script application, then the server will fail-fast to aid in debugging the misconfiguration. See [deephaven-core#1172](https://github.com/deephaven/deephaven-core/issues/1172) for more details.

## Syntax

Deephaven expects the following fields to be in an `*.app` file:

- `type` - Recommended to help organize the scripts you'll run; e.g, `static`, `script`, `qst`, `dynamic`.
- `scriptType` - Useful to describe the proper environment for scripts; e.g., `groovy` or `python`.
- `enabled` - Set to `true` or `false` to run or skip the scripts in the application file. The default is `true`.
- `id` - An identifier only exposed to API clients; enabling `name` changes without requiring layouts to be regenerated.
- `name` - A description that is displayed to users when mentioning this application.
- `file_` - Pathway to each script to run. These are executed in numerical order (after removing the prefix`file_`), not in the order listed in the application. This feature can be used to initialize commonly used values across disparate Applications.

## Example \*.app file

This is an example `.app` file with the fields listed.

```properties
type=script
scriptType=python
enabled=true
id=hello.world
name=Hello World!
file_0=helloWorld.py
```

## Script Application example

In this example, we seed the files needed to use Application Mode, start the Deephaven server, and then see the results in the console.

> [!NOTE]
> By default, all Deephaven deployments mount `./data` in the local deployment directory to `/data` in the running Deephaven containers. See our guide on [Docker data volumes](../conceptual/docker-data-volumes.md) for more information.

Create an `app.d` folder inside your `./data` directory:

```bash
mkdir data/app.d
```

The `app.d` folder is parsed for all files with an `.app` extension and executes each individually in alphabetical order.

As an example `.app` file, create the file `firstApp.app` with the following contents.

```properties
type=script
scriptType=python
enabled=true
id=hello.world
name=Hello World!
file_0=firstApp.py
```

This application file looks for a script listed in the `file_` lines. Here, it's titled `firstApp.py` or `firstApp.groovy` inside the same directory as the application. For this example, our script is simply:

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

In our "hello world" example, we initialize the Deephaven library `ApplicationState` through the `get_app_state` method. `ApplicationState` will collect the state of each application.

Applications expose fields to connecting API Clients, including the web client, by interacting with the provided `ApplicationState`.

Each application has its own state that it can manipulate. This enables similar applications to co-exist without stepping on one another.

In this example, we have one method (`start`) that runs when the container starts. Inside this method, we set two fields:

1. An empty table.
2. A timetable that will update each second.

> [!NOTE]
> Script Applications may directly manipulate variables bound in the [REPL](https://en.wikipedia.org/wiki/Read-eval-print_loop) in addition to, or instead of, using the `ApplicationState` portion of the Application Mode API.

Once you have these two files inside the `app.d` directory, it is time to start the Deephaven containers.

Inside your Deephaven directory, start the containers with the normal Docker command.

```bash
docker compose up
```

You will observe in the start up that two exports are found inside the prompt that refer to our two fields inside the `firstApp.py` or `firstApp.groovy` script.

![Log readout with the exports highlighted](../assets/how-to/appMode.png)

Launch the Deephaven IDE in your web browser. Open the **Panels** menu to see the defined tables `hello` and `world`.

![The Deephaven IDE, with the Panels menu highlighted](../assets/how-to/appMode1.png)

Now that you know how to configure Application Mode, you are empowered to:

- Create all data sources your application needs to function without human interaction.
- Configure and expose commonly viewed plots.
- Prepare the REPL environment with variables, functions, and classes, enabling you to get down to business more quickly.

<!--TODO: Consider [adding prepared layouts](link/to/layouts/documentation) to utilize Application Mode at its full potential.-->

## Multiple Script Application example

In the first script we define tables. We can then access those tables in the Deephaven IDE by making them global, or we can use those tables in future scripts.

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

Files are run in numerical order (after removing the prefix`file_`). Anything created in the `file_0` can be accessed in `file_1`. Here we keep `file_0` the same, and add a `file_1` defined as:

```python skip-test
from deephaven.appmode import ApplicationState


def start(app: ApplicationState):
    global my_new_table
    my_new_table = app["world"]
    my_new_table = my_new_table.update(formulas=["A=i"])


initialize(start)
```

These scripts allow only the `my_new_table` to be available to the [query scope](./query-scope.md), while the other tables are only available in the **Panels** menu. If you want to perform queries on fields defined in application mode, they need to be explicitly assigned to the [query scope](./query-scope.md) inside the applications.

## Other application modes

The other application modes are just as easily configured. You can write applications in other [JVM-based languages](https://en.wikipedia.org/wiki/List_of_JVM_languages) such as Java/Scala/Kotlin and then access them from the REPL via scripting mechanics. This can be done with either static or dynamic applications.

### Static Applications

Static Applications define all the fields at the creation of the instance. These applications are useful for applications in which all fields are known and accessible at all times.

The subclass to implement is `io.deephaven.appmode.StaticClassApplication`. Be sure to [package this into a jar file](https://docs.oracle.com/javase/tutorial/deployment/jar/) and provide it as a third-party dependency to Deephaven’s server. See [How to install and use Java packages](./install-and-use-java-packages.md) for more information.

The `.app` configuration file is as follows:

```bash
type=static
class=fully.qualified.class.Name
enabled=true
```

### Dynamic Applications

Dynamic Applications allow for fields to be created and removed during the instance. These applications are useful when fields might be created or removed with time or as data changes.

The subclass to implement is `io.deephaven.appmode.DynamicApplication` for making your app a dynamic application. Be sure to [package this into a jar file](https://docs.oracle.com/javase/tutorial/deployment/jar/) and provide it as a third-party dependency to Deephaven’s server. See [How to install and use Java packages](./install-and-use-java-packages.md) for more information.

The `.app` configuration file is as follows:

```bash
type=dynamic
class=fully.qualified.class.Name
enabled=true
```

## Related documentation

- [Core API design](../conceptual/deephaven-core-api.md)
- [Docker data volumes](../conceptual/docker-data-volumes.md)
- [How to install and use Java packages](./install-and-use-java-packages.md)
- [How to use Application Mode libraries](./application-mode-libraries.md)
- [How to use Application Mode scripts](./application-mode-script.md)
- [How to use Application Mode video](https://youtu.be/GNm1k0WiRMQ)
