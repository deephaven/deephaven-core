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

![Build CI](https://github.com/deephaven/deephaven-core/actions/workflows/build-ci.yml/badge.svg?branch=main)
![Check CI](https://github.com/deephaven/deephaven-core/actions/workflows/check-ci.yml/badge.svg?branch=main)
![Docs CI](https://github.com/deephaven/deephaven-core/actions/workflows/docs-ci.yml/badge.svg?branch=main)
![Long Check CI](https://github.com/deephaven/deephaven-core/actions/workflows/long-check-ci.yml/badge.svg?branch=main)
![Nightly Check CI](https://github.com/deephaven/deephaven-core/actions/workflows/nightly-check-ci.yml/badge.svg?branch=main)
![Nightly Benchmarks](https://github.com/deephaven/deephaven-core/actions/workflows/nightly-benchmarks.yml/badge.svg?branch=main)

## Supported Languages

| Language      | Server Application | Client Application (OpenAPI) |
| ------------- | ------------------ | ---------------------------- |
| Python        | Yes                | Yes                           |
| Java / Groovy | Yes                | Yes                           |
| JavaScript    | No                 | Yes                          |
| gRPC          | -                  | Yes                          |

## Run Deephaven

This section is a quick start guide for running Deephaven from pre-built images.
For detailed instructions on running Deephaven, see [Launch Deephaven from pre-built images](https://deephaven.io/core/docs/tutorials/launch-pre-built).

For detailed instructions on building and running Deephaven from source code, 
see [Build and launch Deephaven](https://deephaven.io/core/docs/tutorials/launch-build)

### Required Dependencies

Running Deephaven requires a few software packages.

| Package | Version | OS  |
| ------- | ------- | --- |
| docker  | ≥19.3 | All |
| docker-compose | ≥1.29 | All |
| WSL | 2 | Only Windows |

You can check if these packages are installed and functioning by running:
```
docker version
docker-compose version
docker run hello-world
```

On Windows, these commands must be run using WSL 2.

If any dependencies are missing or unsupported versions are installed, 
see [Launch Deephaven from pre-built images](https://deephaven.io/core/docs/tutorials/launch-pre-built).

### Quick Start: Python

Run the following commands to launch Deephaven for Python. 

```bash
mkdir deephaven-deployment
cd deephaven-deployment
curl https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/docker-compose.yml -O
docker-compose pull
docker-compose up -d
```

### Quick Start: Python With Example Data

Run the following commands to launch Deephaven for Python, with example data.

```bash
mkdir deephaven-deployment
cd deephaven-deployment
curl https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/docker-compose.yml -O
docker-compose pull
docker-compose up -d
```

### Quick Start: Groovy / Java

Run the following commands to launch Deephaven for Groovy / Java. 

```bash
mkdir deephaven-deployment
cd deephaven-deployment
curl https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/groovy/docker-compose.yml -O
docker-compose pull
docker-compose up -d
```

### Quick Start: Groovy / Java With Example Data

Run the following commands to launch Deephaven for Groovy / Java, with example data.

```bash
mkdir deephaven-deployment
cd deephaven-deployment
curl https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/groovy-examples/docker-compose.yml -O
docker-compose pull
docker-compose up -d
```

### Monitor Logs

The `-d` option to `docker-compose` causes the containers to run in the background, in detached mode. 
This option allows you to use your shell after Docker launches the containers.

Since the container is running detached, you will not see any logs. However, you can follow the logs by running:

```bash
docker-compose logs -f
```

This command must be run from the deployment directory.

Use CTRL+C to stop monitoring the logs and return to a prompt.

### Shutdown Deephaven

The deployment can be brought down by running:

```bash
docker-compose down
```

This command must be run from the deployment directory.

### Manage example data

The [Deephaven examples repository](https://github.com/deephaven/examples) contains data sets that are useful when learning 
to use Deephaven. These data sets are used extensively in Deephaven's documentation and are needed to run some examples.

If you have chosen a deployment with example data, the example data sets will be downloaded. Production deployments containing 
your own data will not need the example data sets.

[Deephaven's examples repository](https://github.com/deephaven/examples) contains documentation on the available data sets. 
Additionally, there is documentation on managing the data sets. This includes instructions on how to upgrade to the latest version.


## Run Deephaven IDE

Once Deephaven is running, you can launch a Deephaven IDE in your web browser.  Deephaven IDE allows you
to interactively analyze data and develop new analytics.

- If Deephaven is running locally,
navigate to [http://localhost:10000/ide/](http://localhost:10000/ide/).
- If Deephaven is running remotely, navigate
to `http://<hostname>:10000/ide/`, where `<hostname>` is the address of the machine Deephaven is running on.

![alt_text](docs/images/ide_startup.png "Deephaven IDE")

# First Query

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
* [Discussions](https://docs.github.com/en/discussions)
* [deephaven.io](https://deephaven.io)
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
