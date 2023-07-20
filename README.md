# Deephaven Community Core

![Deephaven Data Labs Logo](docs/images/Deephaven_GH_Logo.svg)

Deephaven Community Core is a real-time, time-series, column-oriented analytics engine
with relational database features.
Queries can seamlessly operate upon both historical and real-time data.
Deephaven includes an intuitive user experience and visualization tools.
It can ingest data from a variety of sources, apply computation and analysis algorithms
to that data, and build rich queries, dashboards, and representations with the results.

Deephaven Community Core is an open version of [Deephaven Enterprise](https://deephaven.io),
which functions as the data backbone for prominent hedge funds, banks, and financial exchanges.

[![Join the chat at https://gitter.im/deephaven/deephaven](https://badges.gitter.im/deephaven/deephaven.svg)](https://gitter.im/deephaven/deephaven?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
![Build CI](https://github.com/deephaven/deephaven-core/actions/workflows/build-ci.yml/badge.svg?branch=main)
![Quick CI](https://github.com/deephaven/deephaven-core/actions/workflows/quick-ci.yml/badge.svg?branch=main)
![Docs CI](https://github.com/deephaven/deephaven-core/actions/workflows/docs-ci.yml/badge.svg?branch=main)
![Check CI](https://github.com/deephaven/deephaven-core/actions/workflows/check-ci.yml/badge.svg?branch=main)
![Nightly Check CI](https://github.com/deephaven/deephaven-core/actions/workflows/nightly-check-ci.yml/badge.svg?branch=main)
![Nightly Benchmarks](https://github.com/deephaven/deephaven-core/actions/workflows/nightly-benchmarks.yml/badge.svg?branch=main)

## Supported Languages

| Language      | Server Application | Client Application |
| ------------- | ------------------ | ------------------ |
| Python        | Yes                | Yes                |
| Java / Groovy | Yes                | Yes                |
| C++           | No                 | Yes                |
| JavaScript    | No                 | Yes                |
| Go            | No                 | Yes                |
| R             | No                 | Yes                |
| gRPC          | -                  | Yes                |

## Run Deephaven

There are both server- and client-side applications available, as seen in the table above.

### Server-side

Deephaven's server-side APIs allow you to connect directly to the Deephaven server and execute commands, write queries, and much more.

Most users will want to run Deephaven from pre-built images. It's the easiest way to configure, deploy, and use Deephaven. For detailed instructions, see [Launch Deephaven from pre-built images](https://deephaven.io/core/docs/tutorials/quickstart).

Some Python users will want to use Deephaven entirely from Python. For instructions on starting and using Deephaven from Python, see [Quick start for pip-installed Deephaven](https://deephaven.io/core/docs/tutorials/quickstart-pip/).

Developers interested in tinkering with and modifying source code should build from source. For detailed instructions on how to do this, see [Build and launch Deephaven](https://deephaven.io/core/docs/how-to-guides/launch-build).

If you are not sure which of the two is right for you, use the pre-built images.

### Client-side

Deephaven's client APIs allow you to connect to a Deephaven server and execute code. Documentation links are below:

- [Python](https://deephaven.io/core/client-api/python/)
- [Java/Groovy](https://deephaven.io/core/javadoc/)
- [C++](https://deephaven.io/core/client-api/cpp/)
- [JavaScript](https://deephaven.io/core/docs/reference/js-api/documentation/)
- [Go](https://pkg.go.dev/github.com/deephaven/deephaven-core/go)
- Documentation for R can be found with the `?` operator in R.

### Docker Dependencies

Running Deephaven from pre-built Docker images requires a few software packages.

| Package        | Version                       | OS           |
| -------------- | ----------------------------- | ------------ |
| docker         | ^20.10.8                      | All          |
| docker-compose | ^1.29.0                       | All          |
| Windows        | 10 (OS build 20262 or higher) | Only Windows |
| WSL            | 2                             | Only Windows |

You can check if these packages are installed and functioning by running:
```
docker version
docker-compose version
docker run hello-world
```

> :warning: **On Windows, all commands must be run inside a WSL 2 terminal.**

If any dependencies are missing or unsupported versions are installed, see [Launch Deephaven from pre-built images](https://deephaven.io/core/docs/tutorials/quickstart#prerequisites) for installation instructions.


### Create deployment

A directory must be created to store files and mount points for your deployment.  Here, we are using the `deephaven-deployment` directory.  

You will need to `cd` into the deployment directory to launch or interact with the deployment.

```bash
mkdir deephaven-deployment
cd deephaven-deployment
```

> :warning: **Commands in the following sections for interacting with a deployment must be run from the deployment directory.**

### Launch options

Deephaven offers a variety of pre-made Docker Compose YAML files for different deployments. The following commands will launch Deephaven. Replace `<URL>` with that of the deployment from the table below that best suits your needs.

```bash
curl <URL> -O
docker compose pull
docker compose up -d
```

| Language | Example data | Additional packages | URL |
| Python   | No           | None                | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/base/docker-compose.yml |
| Python   | No           | SciKit-Learn        | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/SciKit-Learn/docker-compose.yml |
| Python   | No           | PyTorch             | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/PyTorch/docker-compose.yml |
| Python   | No           | TensorFlow          | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/TensorFlow/docker-compose.yml |
| Python   | No           | NLTK                | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/NLTK/docker-compose.yml |
| Python   | Yes          | None                | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/base/docker-compose.yml |
| Python   | Yes          | SciKit-Learn        | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/SciKit-Learn/docker-compose.yml |
| Python   | Yes          | PyTorch             | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/PyTorch/docker-compose.yml |
| Python   | Yes          | TensorFlow          | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/TensorFlow/docker-compose.yml |
| Python   | Yes          | NLTK                | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/NLTK/docker-compose.yml |
| Groovy   | No           | None                | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/groovy/docker-compose.yml |
| Groovy   | Yes          | None                | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/groovy-examples/docker-compose.yml |


### Monitor logs

The `-d` option to `docker-compose` causes the containers to run in the background, in detached mode.  This option allows you to use your shell after Docker launches the containers.

Since the container is running detached, you will not see any logs. However, you can follow the logs by running:

```bash
docker-compose logs -f
```

Use CTRL+C to stop monitoring the logs and return to a prompt.

### Authentication

Deephaven, by default, comes stock with pre-shared key to authenticate any users trying to access it. If no key is set, a randomly generated key is printed to the Docker logs. The key can be found by monitoring the logs. It will look like this:

```
================================================================================
Superuser access through pre-shared key is enabled - use 1c3opvgzl5scm to connect
Connect automatically to Web UI with http://localhost:10000/?psk=1c3opvgzl5scm
================================================================================
```

You can also `grep` for the key in the logs:

```bash
docker compose logs -f | grep "access through pre-shared key"
```

For more information on setting your own pre-shared key, see [How to configure and use pre-shared key authentication](https://deephaven.io/core/docs/how-to-guides/authentication/auth-psk/).

For information on enabling anonymous authentication, see [How to enable anonymous authentication](https://deephaven.io/core/docs/how-to-guides/authentication/auth-anon/).

### Shutdown

The deployment can be brought down by running:

```bash
docker-compose down
```

### Manage example data

[Deephaven's examples repository](https://github.com/deephaven/examples) contains data sets that are useful when learning 
to use Deephaven. These data sets are used extensively in Deephaven's documentation and are needed to run some examples. [Deephaven's examples repository](https://github.com/deephaven/examples) contains documentation on the available data sets and how to manage them. 

If you have chosen a deployment with example data, the example data sets will be downloaded. Production deployments containing your own data will not need the example data sets.


To upgrade a deployment to the latest example data, run:

```bash
docker-compose run examples download
```

To see what other example data management commands are available, run:

```bash
docker-compose run examples
```

If your deployment does not have example data, these commands will fail with `ERROR: No such service`.

### Build from source

If you are interested in tinkering and modifying source code for your own Deephaven deployment, or contributing to the project, see this [README](https://github.com/deephaven/deephaven-core/blob/main/server/jetty-app/README.md) for build instructions. There are several [required dependencies](https://deephaven.io/core/docs/how-to-guides/launch-build/#required-dependencies) to build and launch from source code.

Additionally, see our [code of conduct](https://github.com/deephaven/deephaven-core/blob/main/CODE_OF_CONDUCT.md) and [contributing guide](https://github.com/deephaven/deephaven-core/blob/main/CONTRIBUTING.md).

## Run Deephaven IDE

Once Deephaven is running, you can launch a Deephaven IDE in your web browser.  Deephaven IDE allows you to interactively analyze data.

- If Deephaven is running locally, navigate to [http://localhost:10000/ide/](http://localhost:10000/ide/).
- If Deephaven is running remotely, navigate to `http://<hostname>:10000/ide/`, where `<hostname>` is the address of the machine Deephaven is running on.

![alt_text](docs/images/ide_startup.png "Deephaven IDE")

## First query

From the Deephaven IDE, you can perform your first query.

This script creates two small tables: one for employees and one for departments.
It joins the two tables on the DeptID column to show the name of the department
where each employee works.

```python

from deephaven.TableTools import newTable, stringCol, intCol
from deephaven.conversion_utils import NULL_INT

left = newTable(
        stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers"),
        intCol("DeptID", 31, 33, 33, 34, 34, NULL_INT),
        stringCol("Telephone", "(347) 555-0123", "(917) 555-0198", "(212) 555-0167", "(952) 555-0110", None, None)
    )

right = newTable(
        intCol("DeptID", 31, 33, 34, 35),
        stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
        stringCol("Telephone", "(646) 555-0134", "(646) 555-0178", "(646) 555-0159", "(212) 555-0111")
    )

t = left.join(right, "DeptID", "DeptName,DeptTelephone=Telephone")
```

![alt_text](docs/images/ide_first_query.png "Deephaven IDE First Query")

## Resources

* [Help!](https://github.com/deephaven/deephaven-core/discussions/969)
* [Deephaven Community Slack](https://deephaven.io/slack)
* [Discussions](https://github.com/deephaven/deephaven-core/discussions)
* [Deephaven Community Core docs](https://deephaven.io/core/docs/)
* [Java API docs](https://deephaven.io/core/javadoc/)
* [Python API docs](https://deephaven.io/core/pydoc/)

## Code Of Conduct

This project has adopted the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/0/code_of_conduct/).
For more information see the [Code of Conduct](CODE_OF_CONDUCT.md) or contact [opencode@deephaven.io](mailto:opencode@deephaven.io)
with any additional questions or comments.


## License

Copyright (c) Deephaven Data Labs. All rights reserved.

Provided under the [Deephaven Community License](LICENSE.md).
