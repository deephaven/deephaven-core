# Testing the Deephaven Python packages

The server tests in `py/server/tests` and the client tests in `py/client/tests` each run in two ways:

- **In Docker**, with `./gradlew :Integrations:test-py-deephaven` and `./gradlew :py-client:testPyClient`. These are the suites that `check` runs. They use the Python version in the server Docker image and in the `python` registry image.
- **Without Docker**, with `py/server/run-tests-embedded.sh` and `py/client/run-tests-embedded.sh`. These run the same tests in a venv you set up, on any Python version. The server tests start the JVM from the embedded server, the `deephaven-server` package. The client tests run against the `server-jetty` distribution, the same server the Docker image runs. The `Nightly Python Versions CI` workflow runs both on every supported version, through `.github/scripts/py-versions-test.sh`, which makes a fresh venv for each version.

This page covers the second way. It does not measure coverage. For coverage reports, run the Docker tasks with `-Pcoverage.enabled=true`.

## Prerequisites

- [uv](https://docs.astral.sh/uv/). It creates the venv, downloads the Python version if needed, and installs missing test dependencies.
- A JDK, 17 or later. Set `JAVA_HOME` if `java` on your `PATH` is a different version.
- Docker, to build the `deephaven-server` wheel for the server tests. The tests themselves do not use it.

## Run the tests

1. Build what the scripts need. Rebuild after you change Java code.

   ```bash
   ./gradlew :py-embedded-server:assemble :Integrations:testClasses :engine-time:testClasses
   ./gradlew :server-jetty-app:installDist
   ```

   The first line is for the server tests: the `deephaven-server` wheel and the Java test classes. The second is for the client tests: the `server-jetty` distribution.

2. Make a venv with the Deephaven packages, once per Python version. `py/server/setup.py` and `py/client/setup.py` read their version from `DEEPHAVEN_VERSION`, which must match the wheel, because `deephaven-server` pins `deephaven-core` to it.

   ```bash
   export DEEPHAVEN_VERSION="$(basename py/embedded-server/build/wheel/deephaven_server-*.whl | cut -d- -f2)"
   uv venv --managed-python --python 3.14t .venv-3.14t
   source .venv-3.14t/bin/activate
   uv pip install py/embedded-server/build/wheel/deephaven_server-*.whl -e py/server -e py/client \
       --config-settings editable_mode=compat
   ```

   The version can be anything uv accepts, such as `3.9`, `3.14` or `3.14t`. A version ending in `t` is the free-threaded build. `--managed-python` gets uv's own build at the newest patch release, the one CI uses. With `-e`, the venv uses `py/server` and `py/client` as they are on disk, so Python edits need no reinstall. `editable_mode=compat` is needed for `py/server`: `deephaven-plugin` installs a `deephaven/plugin` folder into the venv, and with the default editable mode Python finds that folder instead of `py/server/deephaven`. One venv serves both suites.

3. With the venv active, run a script, optionally with test names:

   ```bash
   py/server/run-tests-embedded.sh
   py/server/run-tests-embedded.sh tests.test_csv tests.test_time
   py/client/run-tests-embedded.sh tests.test_session
   ```

   Without test names, a script runs all of `tests/`.

The JUnit XML reports go to `build/test-results/test-embedded/<version>/` under `py/server` or `py/client`. A copy of the tests goes to `build/test-embedded/<version>/`. Each script deletes both at the start of a run. It never deletes or replaces your venv.

The two suites both use port 10000, so run them one at a time.

## What the scripts do

Both scripts first check that a venv is active, that it has the Deephaven packages, and that the build outputs exist. Then each one installs any package from the `requirements-test.txt` next to it that the venv lacks. The packages the venv already has are pinned during that install, so it never upgrades or downgrades them. A package that cannot install, for example one with no wheel for that Python version, is skipped, and the tests that need it skip or fail on their own.

The server script then:

1. Sets `DEEPHAVEN_TEST_EMBEDDED=true`, so `test_helper` starts the JVM from the `deephaven-server` package instead of a Docker classpath. `DEEPHAVEN_CLASSPATH` adds the test config, the test calendars and three Java test classes.
2. Sets `TZ=Etc/UTC`, because `tests/test_time.py` expects that time zone name.
3. For a free-threaded Python, sets `PYTHON_GIL=0`. Without it, importing `grpcio` through `pydeephaven` turns the GIL back on for the rest of the run.
4. Runs the tests from a fresh copy of `tests/`, because some tests write files into the working directory, and leftovers from an earlier run make them fail.

The client script then:

1. Starts the `server-jetty` distribution on port 10000. That is the same start script and jars the server Docker image runs: Java is the main program and finds Python through `VIRTUAL_ENV`, your venv. So the server runs on the same Python. It uses the same settings as the Docker-based `testPyClient` task: anonymous and PSK logins, with the PSK `deephaven`. Its log goes to `server.log` next to the test reports.
2. Runs the client tests against that server, then stops the server.

## Kafka and Postgres tests

In the server suite, `tests/test_kafka_consumer.py`, `tests/test_kafka_producer.py` and `tests/test_dbc.py` need Redpanda and Postgres. Without them, those tests fail. To run them, start the containers and map their host names to your machine:

```bash
docker compose -f Integrations/docker-compose.yml up -d --wait
echo "127.0.0.1 redpanda postgres" | sudo tee -a /etc/hosts
```

`tests/test_barrage.py` starts a second server on port 10000, so that port must be free.

`tests/test_perfmon.py` needs the Graphviz `dot` program on your `PATH`.
