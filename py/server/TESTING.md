# Testing the Deephaven Python server package

The tests in `tests/` run in two ways:

- **In Docker**, with `./gradlew :Integrations:test-py-deephaven`. This is the suite that `check` runs. It uses the Python version in the server Docker image.
- **On the embedded server**, with `./gradlew :py-server:testEmbedded`. This runs the same tests on any Python version, without the server Docker image. The nightly `Nightly Python Versions CI` workflow runs it on every supported version.

This page covers the second way.

## Prerequisites

- [uv](https://docs.astral.sh/uv/). It creates the venv and downloads the Python version if it is not installed.
- A JDK, 17 or later. Set `JAVA_HOME` if `java` on your `PATH` is a different version.
- Docker, to build the `deephaven-server` wheel. The tests themselves do not use it.

## Run the tests

```bash
./gradlew :py-server:testEmbedded -PpythonVersion=3.14t
```

- `-PpythonVersion` takes any version uv accepts, such as `3.9`, `3.14` or `3.14t`. A version ending in `t` is the free-threaded build. The default is `3.12`.
- `-PpyTests` runs only some tests. It takes a comma-separated list of test names, such as `-PpyTests=tests.test_csv,tests.test_time`.

The task builds the `deephaven-server` wheel and the Java test classes, then calls `run-tests-embedded.sh`. Once those are built, you can call the script directly, which skips Gradle:

```bash
py/server/run-tests-embedded.sh 3.14t tests.test_csv
```

The JUnit XML reports go to `py/server/build/test-results/test-embedded/<version>/`. The venv and a copy of the tests go to `py/server/build/test-embedded/<version>/`. The script deletes both at the start of each run.

## What the script does

1. It creates a venv with `uv venv --python <version>`.
2. It installs `py/server` and `py/client` from source and the `deephaven-server` wheel from `py/embedded-server/build/wheel`. It then installs the soft dependencies `numba`, `adbc-driver-manager`, `adbc-driver-postgresql` and `connectorx` one at a time, and skips any that have no wheel for that version.
3. It sets `DEEPHAVEN_TEST_EMBEDDED=true`, so `test_helper` starts the JVM from the `deephaven-server` wheel instead of a Docker classpath. `DEEPHAVEN_CLASSPATH` adds the test config, the test calendars and three Java test classes.
4. It sets `TZ=Etc/UTC`, because `tests/test_time.py` expects that time zone name.
5. For a free-threaded version, it sets `PYTHON_GIL=0`. Without it, importing `grpcio` through `pydeephaven` turns the GIL back on for the rest of the run.
6. It runs the tests from a copy of `tests/`, because some tests write files into the working directory, and leftovers from an earlier run make them fail.

## Kafka and Postgres tests

`tests/test_kafka_consumer.py`, `tests/test_kafka_producer.py` and `tests/test_dbc.py` need Redpanda and Postgres. Without them, those tests fail. To run them, start the containers and map their host names to your machine:

```bash
docker compose -f Integrations/docker-compose.yml up -d --wait
echo "127.0.0.1 redpanda postgres" | sudo tee -a /etc/hosts
```

`tests/test_barrage.py` starts a second server on port 10000, so that port must be free.

`tests/test_perfmon.py` needs the Graphviz `dot` program on your `PATH`.
