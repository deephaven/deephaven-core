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
| Python        | Yes                | No                           |
| Java / Groovy | Yes                | No                           |
| JavaScript    | No                 | Yes                          |
| gRPC          | -                  | Yes                          |

## Running Deephaven

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

<details>
  <summary>Installing WSL...</summary>

  On Windows, Windows Subsystem for Linux (WSL) version 2 must be installed.  WSL is not needed
  on other operating systems.
  
  Instructions for installing WSL 2 can be found at [https://docs.microsoft.com/en-us/windows/wsl/install-win10](https://docs.microsoft.com/en-us/windows/wsl/install-win10).
</details>

<details>
  <summary>Installing Docker...</summary>

  Instructions for installing and configuring Docker can be found at
  [https://docs.docker.com/get-docker/](https://docs.docker.com/get-docker/).  Windows users should follow the WSL2 instructions.
  Docker Engine Version 18.03 (or newer) is needed to build.
  Docker Engine Version 19.03 (or newer) is needed to build and run.
 

  Instructions for installing and configuring `docker-compose` can be found at
  [https://docs.docker.com/compose/install/](https://docs.docker.com/compose/install/).
  Version 1.29 (or newer) is recommended; Version 1.27 (or newer) is required.
</details>

<details>
  <summary>Docker RAM settings...</summary>

  Tests run as part of the build process require at least 4GB of Docker RAM.  To check your Docker configuration, run:
  ```
  docker info | grep Memory
  ```

  By default, Docker on Mac is configured with 2 GB of RAM.  If you need to increase the memory on your Mac, click
  on the Docker icon on the top bar and navigate to `Preferences->Resources->Memory`.

  ![alt_text](docs/images/DockerConfigMac.png "Docker Configuration on a Mac")
</details>

<details>
  <summary>If <code>docker run hello-world</code> does not work...</summary>

  If `docker run hello-world` does not work, try the following:
  1. [Is Docker running?](https://docs.docker.com/config/daemon/#check-whether-docker-is-running)
      ```
      docker info
     ```
  2. (Linux) [Are you in the `docker` user group?](https://docs.docker.com/engine/install/linux-postinstall/)
      ```
      sudo groupadd docker
      sudo usermod -aG docker $USER
      ```
</details>

### Choose a deployment

When determining which deployment is right for your application, there are two key questions:

1. What programming language will your queries be written in?
2. Do you need example data from the [Deephaven's examples repository](https://github.com/deephaven/examples)?

Based on your answers, you can use the following table to find the URL to the desired Docker Compose configuration. For example, if you will be working through examples in the Deephaven documentation, and you develop in Python, you will choose [https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/docker-compose.yml](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/docker-compose.yml), since it supports Python queries and has the example data used in the Deephaven documentation.

| Language | Examples | URL                                                                                                           |
| -------- | -------- | ------------------------------------------------------------------------------------------------------------- |
| Groovy   | No       | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/groovy/docker-compose.yml          |
| Groovy   | Yes      | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/groovy-examples/docker-compose.yml |
| Python   | No       | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/docker-compose.yml          |
| Python   | Yes      | https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/docker-compose.yml |

### Set up your Deephaven deployment

First, create a directory for the system to live in. Use any directory name you like; we chose `deephaven-deployment`:

```bash
mkdir deephaven-deployment
```

Then, make that the current working directory:

```bash
cd deephaven-deployment
```

Now, use `curl` to get the Docker Compose file for your desired configuration. Substitute the URL of your choice from the table above. We use the Python build with the examples manager included:

```bash
curl https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/docker-compose.yml -O
```

Now that the `docker-compose.yml` file is locally available, download the Docker images:

```bash
docker-compose pull
```

Since this step only gets the container images and does not run anything, the Deephaven services will not start, and you will not see any logging output.

When new features are added to Deephaven, you will need to redownload the `docker-compose.yml` file and redownload the Docker images to get the latest version of Deephaven.

## Manage the Deephaven deployment

Now that your chosen configuration is set up, enter its directory and bring up the deployment:

```bash
docker-compose up -d
```

The `-d` option causes the containers to run in the background, in detached mode. This option allows you to use your shell after Docker launches the containers.

Since the container is running detached, you will not see any logs. However, you can follow the logs by running:

```bash
docker-compose logs -f
```

Use CTRL+C to stop monitoring the logs and return to a prompt.

The deployment can be brought down by running:

```bash
docker-compose down
```

The Deephaven containers use a few [Docker volumes](https://docs.docker.com/storage/volumes/) to store persistent data. If you don't want to keep that persistent storage around, you might want to remove all the volumes that were associated with the deployment. This can be done by running:

Running the following command will permanently delete important state for your Deephaven deployment. Only perform this step if you are certain that the deployment state is no longer needed.

```bash
docker-compose down -v
```

### Manage example data

The [Deephaven examples repository](https://github.com/deephaven/examples) contains data sets that are useful when learning to use Deephaven. These data sets are used extensively in Deephaven's documentation and are needed to run some examples.

If you have chosen a deployment with example data, the example data sets will be downloaded. Production deployments containing your own data will not need the example data sets.

[Deephaven's examples repository](https://github.com/deephaven/examples) contains documentation on the available data sets. Additionally, there is documentation on managing the data sets. This includes instructions on how to upgrade to the latest version.


### Run Deephaven IDE

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
