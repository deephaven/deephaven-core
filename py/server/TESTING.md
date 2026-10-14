# Testing the Deephaven Python packages

The server tests in `py/server/tests` and the client tests in `py/client/tests` each run in two ways:

- **In Docker**, with `./gradlew :Integrations:test-py-deephaven` and `./gradlew :py-client:testPyClient`. These are the suites that `check` runs. They use the Python version in the server Docker image and in the `python` registry image.
- **Without Docker**, with `./gradlew :py-server:testEmbedded` and `./gradlew :py-client:testEmbedded`. These run the same tests on any Python version. The server tests start the JVM from the embedded server, the `deephaven-server` wheel. The client tests run against the `server-jetty` distribution, the same server the Docker image runs. The nightly `Nightly Python Versions CI` workflow runs both on every supported version.

This page covers the second way. It does not measure coverage. For coverage reports, run the Docker tasks with `-Pcoverage.enabled=true`.

## Prerequisites

- [uv](https://docs.astral.sh/uv/). It creates the venv and downloads the Python version if it is not installed.
- A JDK, 17 or later. Set `JAVA_HOME` if `java` on your `PATH` is a different version.
- Docker, to build the `deephaven-server` wheel for the server tests. The tests themselves do not use it.

## Run the tests

```bash
./gradlew :py-server:testEmbedded -PpythonVersion=3.14t
./gradlew :py-client:testEmbedded -PpythonVersion=3.14t
```

- `-PpythonVersion` takes any version uv accepts, such as `3.9`, `3.14` or `3.14t`. A version ending in `t` is the free-threaded build. The default is `3.12`.
- `-PpyTests` runs only some tests. It takes a comma-separated list of test names, such as `-PpyTests=tests.test_csv,tests.test_time`.

Each task builds what it needs, then calls the `run-tests-embedded.sh` script next to its `build.gradle`. The server task builds the `deephaven-server` wheel and the Java test classes. The client task builds the `server-jetty` distribution with `:server-jetty-app:installDist`. Once those are built, you can call a script directly, which skips Gradle:

```bash
py/server/run-tests-embedded.sh 3.14t tests.test_csv
py/client/run-tests-embedded.sh 3.14t tests.test_session
```

The JUnit XML reports go to `build/test-results/test-embedded/<version>/` under `py/server` or `py/client`. The venv and a copy of the tests go to `build/test-embedded/<version>/`. Each script deletes both at the start of a run.

The two suites both use port 10000, so run them one at a time.

## What the server script does

1. It creates a venv with `uv venv --python <version>`.
2. It installs `py/server` and `py/client` from source and the `deephaven-server` wheel from `py/embedded-server/build/wheel`. It then installs the soft dependencies `numba`, `adbc-driver-manager`, `adbc-driver-postgresql` and `connectorx` one at a time, and skips any that have no wheel for that version.
3. It sets `DEEPHAVEN_TEST_EMBEDDED=true`, so `test_helper` starts the JVM from the `deephaven-server` wheel instead of a Docker classpath. `DEEPHAVEN_CLASSPATH` adds the test config, the test calendars and three Java test classes.
4. It sets `TZ=Etc/UTC`, because `tests/test_time.py` expects that time zone name.
5. For a free-threaded version, it sets `PYTHON_GIL=0`. Without it, importing `grpcio` through `pydeephaven` turns the GIL back on for the rest of the run.
6. It runs the tests from a fresh copy of `tests/`, because some tests write files into the working directory. When you rerun locally in the same checkout, leftovers from the earlier run make them fail. CI does not need this, since each matrix entry runs on a fresh runner.

## What the client script does

1. It creates a venv and installs `py/server` and `py/client`. It then installs the soft dependencies `flightsql-dbapi`, `adbc-driver-flightsql` and `numba`. The server-side script in `py/client/tests/test_console.py` imports `numba`.
2. It starts the `server-jetty` distribution on port 10000. That is the same start script and jars the server Docker image runs: Java is the main program and finds Python through `VIRTUAL_ENV`, which points at the venv. So the server runs on the same Python version. It uses the same settings as the Docker-based `testPyClient` task: anonymous and PSK logins, with the PSK `deephaven`. Its log goes to `server.log` next to the test reports.
3. It runs the client tests against that server, then stops the server.

## Kafka and Postgres tests

In the server suite, `tests/test_kafka_consumer.py`, `tests/test_kafka_producer.py` and `tests/test_dbc.py` need Redpanda and Postgres. Without them, those tests fail. To run them, start the containers and map their host names to your machine:

```bash
docker compose -f Integrations/docker-compose.yml up -d --wait
echo "127.0.0.1 redpanda postgres" | sudo tee -a /etc/hosts
```

`tests/test_barrage.py` starts a second server on port 10000, so that port must be free.

`tests/test_perfmon.py` needs the Graphviz `dot` program on your `PATH`.
