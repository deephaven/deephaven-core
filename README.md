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

## Supported Languages

| Language      | Server Application | Client Application |
| ------------- | ------------------ | ------------------ |
| Python        | Yes                | Yes                |
| Java / Groovy | Yes                | Yes                |
| C++           | No                 | Yes                |
| JavaScript    | No                 | Yes                |
| Go            | No                 | Yes                |
| R             | No                 | Yes                |

## Installation

Deephaven Community Core can be used from both server- and client-side APIs. Server-side APIs allow users to connect directly to the Deephaven server and execute commands, write queries, and much more. Client-side APIs allow users to connect to a Deephaven server through a client application.

See the following documentation links for installation instructions and more:

- Python
  - [Run from Docker](https://deephaven.io/core/docs/tutorials/quickstart/)
  - [pip-installed](https://deephaven.io/core/docs/tutorials/quickstart/)
- [Groovy]
  - [Run from Docker](https://deephaven.io/core/groovy/docs/tutorials/quickstart/)
- [Python client](https://pypi.org/project/pydeephaven/)
- [Java client](https://deephaven.io/core/docs/how-to-guides/java-client/)
- [JS client](https://deephaven.io/core/docs/reference/js-api/documentation/)
- [Go client](https://pkg.go.dev/github.com/deephaven/deephaven-core/go)
- [R client](https://github.com/deephaven/deephaven-core/blob/main/R/rdeephaven/README.md)

Deephaven's client APIs use [gRPC](https://grpc.io/), [protobuf](https://github.com/deephaven/deephaven-core/tree/main/proto/proto-backplane-grpc/src/main/proto/deephaven/proto), [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html), and [Barrage](https://github.com/deephaven/barrage) to handle ticking data. Users who wish to build their own client APIs can use these tools to do so.

## Build from source

Users who wish to modify or contribute to this project should [build from source](https://deephaven.io/core/docs/how-to-guides/launch-build/) to run either the Python or Groovy server-side API.

Start by cloning this repository, and `cd` into the cloned repo.

```bash
git clone https://github.com/deephaven/deephaven-core.git
cd deephaven-core
```

### Required dependencies

Building and running Deephaven requires a few software packages.

| Package        | Version                       | OS           |
| -------------- | ----------------------------- | ------------ |
| git            | ^2.25.0                       | All          |
| java           | >=11, <20                     | All          |
| docker         | ^20.10.8                      | All          |
| docker-compose | ^1.29.0                       | All          |
| Windows        | 10 (OS build 20262 or higher) | Only Windows |
| WSL            | 2                             | Only Windows |

You can check if these packages are installed and functioning by running:

```bash
git version
java -version
docker version
docker-compose version
docker run hello-world
```

:::note

Internally, the Java build process will use [Gradle Auto Provisioning](https://docs.gradle.org/current/userguide/toolchains.html#sec:provisioning)
to download and use the appropriate Java version for building and testing.

On Windows, all commands must be run inside a WSL 2 terminal.

:::

### Python

Set up a virtual environment.

```bash
python -m venv /tmp/my-dh-venv
source /tmp/my-dh-venv/bin/activate
```

Then, build and install the wheel.

```bash
./gradlew py-server:assemble
pip install "py/server/build/wheel/deephaven_core-<version>-py3-non-any.whl[autocomplete]
```

Where:

- `<version>` is replaced by a deephaven-core [release](https://github.com/deephaven/deephaven-core/releases).
- Users who wish to use Deephaven without autocomplete should remove `[autocomplete]` from the `pip install` command.

Lastly, build.

```bash
./gradlew server-jetty-app:run
```

### Groovy/Java

This single command will build and run the Deephaven Groovy server-side API from source.

```bash
./gradlew server-jetty-app:run
```

### Authentication

Deephaven, by default, uses pre-shared key authentication to authenticate against unauthorized access. Users will be prompted for a key when connecting to an instance of the server. Unless otherwise specified, a new randomly generated key will be used each time the server is started. The key is printed to the Docker logs. To search the logs for the key, run:

```bash
docker compose logs -f | grep "access through pre-shared key"
```

To set the key, add `-Ppsk` to the build command:

```bash
./gradlew server-jetty-app:run -Ppsk=My-Password
```

### Connect to the server

Deephaven is run from a web browser, and can be connected to via `http://localhost:10000/ide`.

## First query

From the Deephaven IDE, you can perform your first query.

This Python script creates two small tables: one for employees and one for departments.
It joins the two tables on the DeptID column to show the name of the department
where each employee works.

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