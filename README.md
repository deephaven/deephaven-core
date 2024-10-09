# Deephaven Community Core

![Deephaven Data Labs Logo](docs/images/Deephaven_GH_Logo.svg)

Deephaven Community Core is a real-time, time-series, column-oriented analytics engine
with relational database features.
Queries can seamlessly operate upon both historical and real-time data.
Deephaven includes an intuitive user experience and visualization tools.
It can ingest data from a variety of sources, apply computation and analysis algorithms
to that data, and build rich queries, dashboards, and representations with the results.

Deephaven Community Core is the open version of [Deephaven Enterprise](https://deephaven.io),
which functions as the data backbone for prominent hedge funds, banks, and financial exchanges.

- ![Build CI](https://github.com/deephaven/deephaven-core/actions/workflows/build-ci.yml/badge.svg?branch=main)
- ![Quick CI](https://github.com/deephaven/deephaven-core/actions/workflows/quick-ci.yml/badge.svg?branch=main)
- ![Docs CI](https://github.com/deephaven/deephaven-core/actions/workflows/docs-ci.yml/badge.svg?branch=main)
- ![Check CI](https://github.com/deephaven/deephaven-core/actions/workflows/check-ci.yml/badge.svg?branch=main)
- ![Nightly Check CI](https://github.com/deephaven/deephaven-core/actions/workflows/nightly-check-ci.yml/badge.svg?branch=main)

This README is intended to provide a high-level overview of the installation and use of Deephaven Community Core. For more detailed guides on the topics presented below, see our [Community documentation](https://deephaven.io/core/docs).

## Supported Languages

| Language      | Server Application | Client Application |
| ------------- | ------------------ | ------------------ |
| Python        | Yes                | Yes                |
| Java / Groovy | Yes                | Yes                |
| C++           | No                 | Yes                |
| JavaScript    | No                 | Yes                |
| Go            | No                 | Yes                |
| R             | No                 | Yes                |

Deephaven's client APIs use [gRPC](https://grpc.io/), [protobuf](https://github.com/deephaven/deephaven-core/tree/main/proto/proto-backplane-grpc/src/main/proto/deephaven/proto), [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html), and [Barrage](https://github.com/deephaven/barrage) to handle ticking data. Users who wish to build their own client APIs can use these tools to do so.

The following list contains documentation links for installation instructions and more:

- Python
  - [Run from Docker](https://deephaven.io/core/docs/getting-started/quickstart/)
  - [pip-installed](https://deephaven.io/core/docs/getting-started/pip-install/)
- Groovy
  - [Run from Docker](https://deephaven.io/core/docs/getting-started/quickstart/)
- [Python client](https://pypi.org/project/pydeephaven/)
- [Java client](https://deephaven.io/core/docs/how-to-guides/java-client/)
- [JS client](https://deephaven.io/core/client-api/javascript/modules/dh.html)
- [Go client](https://pkg.go.dev/github.com/deephaven/deephaven-core/go)
- [R client](https://github.com/deephaven/deephaven-core/blob/main/R/rdeephaven/README.md)

## Install and run Deephaven

The Deephaven server can be installed and instantiated [from Docker](#from-docker), [from Python](#from-python), or [from source code](#built-from-source).

### From Docker

This is the easiest way to get started with Deephaven. For complete instructions, see our [quickstart for Docker](https://deephaven.io/core/docs/getting-started/quickstart/). The table below shows installation dependencies.

| Dependency     | Version  | OS      | Required/Recommended |
| -------------- | -------- | --------| -------------------- |
| Docker         | ^20.10.8 | All     | Required             |
| Docker compose | ^2       | All     | Recommended          |
| Windows        | 10+      | Windows | Required             |
| WSL            | ^2       | Windows | Required             |

The quickest way to install and run Deephaven from Docker is with a single Docker command:

**Python without Docker Compose**

```sh
# Python
docker run --rm --name deephaven -p 10000:10000 ghcr.io/deephaven/server:latest
```

**Groovy without Docker Compose**

```sh
# Groovy
docker run --rm name deephaven -p 10000:10000 ghcr.io/deephaven/server-slim:latest
```

Users who wish to customize their deployment should use Docker Compose. Deephaven offers a multitude of pre-made [docker-compose.yml files](https://deephaven.io/core/docs/getting-started/docker-install/#choose-a-deployment) to choose from. To get started, all that's required is to download a file, pull the images, and start the server.

**Python with Docker Compose**

The base Python `docker-compose.yml` file can be found [here](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/base/docker-compose.yml).

```sh
mkdir deephaven-deployment
cd deephaven-deployment

curl -O https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/base/docker-compose.yml

docker compose pull
docker compose up
```

**Groovy with Docker Compose**

The base Groovy `docker-compose.yml` file can be found [here](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/groovy/docker-compose.yml).

```sh
mkdir deephaven-deployment
cd deephaven-deployment

curl -O https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/groovy/docker-compose.yml

docker compose pull
docker compose up
```

### pip-installed Deephaven

Users who wish to use Python but not Docker should use [pip-installed Deephaven](https://deephaven.io/core/docs/tutorials/quickstart-pip/). For users with Windows operating systems, WSL is **not** required to use Deephaven this way.

```sh
pip install --upgrade pip setuptools wheel
pip install deephaven-server deephaven-ipywidgets
```

Then, from Python:

```python
from deephaven_server import Server
s = Server(port=10000, jvm_args=["-Xmx4g"]).start()
```

The input arguments to `Server` specify to bind to the Deephaven server on port `10000` and to allocate 4GB of memory to the server JVM.

### Built from source

Users who wish to modify source code and contribute to the project should build Deephaven from source. For complete instructions, see [How to build Deephaven from source](https://deephaven.io/core/docs/getting-started/launch-build/).

Building and running Deephaven requires a few software packages.

| Package        | Version                       | OS           | Required/Recommended |
| -------------- | ----------------------------- | ------------ | -------------------- |
| git            | ^2.25.0                       | All          | Required             |
| java           | >=11, <=22                    | All          | Required             |
| docker         | ^20.10.8                      | All          | Required             |
| docker compose | ^2                            | All          | Recommended          |
| Windows        | 10 (OS build 20262 or higher) | Only Windows | Required             |
| WSL            | 2                             | Only Windows | Required             |

You can check if these packages are installed and functioning by running:

```bash
git version
java -version
docker version
docker compose version
docker run hello-world
```

> **_NOTE:_** Internally, the Java build process will use [Gradle Auto Provisioning](https://docs.gradle.org/current/userguide/toolchains.html#sec:provisioning)
to download and use the appropriate Java version for building and testing.

> **_NOTE:_** On Windows, all commands must be run inside a WSL 2 terminal.

#### Python

A Python virtual environment is highly recommended for building Deephaven from source. Additionally, the wheel is installed with [pip](https://pypi.org/project/pip/) and built with [Gradle](https://gradle.org/).

```sh
git clone https://github.com/deephaven/deephaven-core.git
cd deephaven-core
python3 -m venv /tmp/my-dh-venv
source /tmp/my-dh-venv/bin/activate
./gradlew py-server:assemble
pip install "py/server/build/wheel/deephaven_core-<version>-py3-non-any.whl[autocomplete]
./gradlew server-jetty-app:run
```

#### Groovy

The Groovy server is built with [Gradle](https://gradle.org/). `-Pgroovy` builds the Groovy server instead of Python.

```sh
git clone https://github.com/deephaven/deephaven-core.git
cd deephaven-core
./gradlew server-jetty-app:run -Pgroovy
```

#### Debugging

You can debug the server by adding the `-Pdebug` flag, and then attaching a debugger to port 5005. This can be used in conjunction with other flags. For example, if you wanted to debug a server and startup with Groovy:
```sh
./gradlew server-jetty-app:run -Pgroovy -Pdebug
```

## Get the authentication key

Deephaven, by default, uses [pre-shared key authentication](https://deephaven.io/core/docs/how-to-guides/authentication/auth-psk/) to authenticate against unauthorized access. 

### Deephaven run from Docker

The pre-shared key is printed to the Docker logs when the server is started. Set your own key with the configuration parameter `-Dauthentication.psk=<YourKey>`. For users running Deephaven via Docker, this is set in the `environment` section of a `docker-compose.yml` file, or as a space-separated configuration parameter at the end of the [`docker run` command](#from-docker).

To find the pre-shared key in the Docker logs:

```sh
docker compose logs -f | grep "access through pre-shared key"
```

### Deephaven run from Python

When a Deephaven server is started from Python, executing Deephaven queries from Python does _not_ require the key. However, if you wish to connect to the IDE via your web browser, you will need the pre-shared key. You will not be able to get the pre-shared key unless you set it yourself. To set the pre-shared key, add `"-Dauthentication.psk=<YourKey>"` as an additional JVM parameter to the server. The following example sets the key to `MyPreSharedKey`:

```python
from deephaven_server import Server
s = Server(port=10000, jvm_args=["-Xmx4g", "-Dauthentication.psk=MyPreSharedKey"]).start()
```

### Client APIs

Clients that attempt to connect to a server using pre-shared key authentication will need to supply the key to complete the connection. The key is the same for a client connection as it is for connecting directly to the server. For instance, in the [above example](#deephaven-run-from-python), the key for a client connection would also be `MyPreSharedKey`.

## Connect to the server

The Deephaven UI is accessible from a web browser. For a server running locally on port 10000, it can be connected to via `https://localhost:10000/ide`. For a server running remotely on port 10000, it can be connected to via `https://<hostname>:10000/ide`. If using authentication, enter credentials to gain access to the IDE. For information on supported browsers, see [here](https://github.com/deephaven/web-client-ui#browser-support).

## First query

From the Deephaven IDE, you can perform your first query.

The scripts below create two small tables: one for employees and one for departments. They are joined on the `DeptID` column to show the name of the department where each employee works.

### Python

```python
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.constants import NULL_INT

left = new_table([
        string_col("LastName", ["Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers"]),
        int_col("DeptID", [31, 33, 33, 34, 34, NULL_INT]),
        string_col("Telephone", ["(347) 555-0123", "(917) 555-0198", "(212) 555-0167", "(952) 555-0110", None, None])
    ])

right = new_table([
        int_col("DeptID", [31, 33, 34, 35]),
        string_col("DeptName", ["Sales", "Engineering", "Clerical", "Marketing"]),
        string_col("Telephone", ["(646) 555-0134", "(646) 555-0178", "(646) 555-0159", "(212) 555-0111"])
    ])

t = left.join(right, "DeptID", "DeptName, DeptTelephone=Telephone")
```

![alt_text](docs/images/ide_first_query.png "Deephaven IDE First Query")

### Groovy

```groovy
left = newTable(
        string_col("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers"),
        int_col("DeptID", 31, 33, 33, 34, 34, NULL_INT),
        string_col("Telephone", "(347) 555-0123", "(917) 555-0198", "(212) 555-0167", "(952) 555-0110", null, null)
    )

right = newTable(
        intCol("DeptID", 31, 33, 34, 35),
        stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
        stringCol("Telephone", "(646) 555-0134", "(646) 555-0178", "(646) 555-0159", "(212) 555-0111")
    )

t = left.join(right, "DeptID", "DeptName, DeptTelephone=Telephone")
```

![alt_text](docs/images/ide_first_query.png "Deephaven IDE First Query")

## Resources

* [Help!](https://github.com/deephaven/deephaven-core/discussions/969)
* [Deephaven Community Slack](https://deephaven.io/slack)
* [Discussions](https://github.com/deephaven/deephaven-core/discussions)
* [Deephaven Community Core docs](https://deephaven.io/core/docs/)
* [Java API docs](https://deephaven.io/core/javadoc/)
* [Python API docs](https://deephaven.io/core/pydoc/)

## Contributing

See [CONTRIBUTING](./CONTRIBUTING.md) for full instructions on how to contribute to this project.

### Code Of Conduct

This project has adopted the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/0/code_of_conduct/).
For more information see the [Code of Conduct](CODE_OF_CONDUCT.md) or contact [opencode@deephaven.io](mailto:opencode@deephaven.io)
with any additional questions or comments.

### License

Copyright © 2016-2023 Deephaven Data Labs and Patent Pending. All rights reserved.

Provided under the [Deephaven Community License](LICENSE.md).
