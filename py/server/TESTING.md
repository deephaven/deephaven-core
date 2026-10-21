# Testing the Deephaven Python server package

The tests in `py/server/tests` run in two ways:

- **In Docker**, with `./gradlew :Integrations:test-py-deephaven`. This is the suite that `check` runs. It uses the Python version in the server Docker image.
- **Without Docker**, with `py/server/run-tests-venv.sh`. It runs the same tests in a venv you set up, on any Python version. The server jars come from the `deephaven-server` package, the embedded server. The `Nightly Python Versions CI` workflow runs it on every supported version, through `.github/scripts/py-versions-test.sh`, which makes a fresh venv for each version.

This page covers the second way. It does not measure coverage. For coverage reports, run the Docker task with `-Pcoverage.enabled=true`. For the client tests, see [`py/client/TESTING.md`](../client/TESTING.md).

## Prerequisites

- A JDK, 17 or later. Set `JAVA_HOME` if `java` on your `PATH` is a different version.
- Docker, to build the `deephaven-server` wheel. The tests themselves do not use it.
- A tool to make the venv. The steps below use [uv](https://docs.astral.sh/uv/), which also downloads the Python version if needed. Any venv works.

## Run the tests

1. Build the `deephaven-server` wheel. Rebuild after you change Java code.

   ```bash
   ./gradlew :py-embedded-server:assemble
   ```

2. Make a venv with the Deephaven packages and the test dependencies, once per Python version. `py/server/setup.py` and `py/client/setup.py` read their version from `DEEPHAVEN_VERSION`, which must match the wheel, because `deephaven-server` pins `deephaven-core` to it.

   ```bash
   export DEEPHAVEN_VERSION="$(basename py/embedded-server/build/wheel/deephaven_server-*.whl | cut -d- -f2)"
   uv venv --managed-python --python 3.14t .venv-3.14t
   source .venv-3.14t/bin/activate
   uv pip install py/embedded-server/build/wheel/deephaven_server-*.whl -e py/server -e py/client \
       -r py/server/requirements-test.txt --config-settings editable_mode=compat
   ```

   The version can be anything uv accepts, such as `3.9`, `3.14` or `3.14t`. A version ending in `t` is the free-threaded build. `--managed-python` gets uv's own build at the newest patch release, the one CI uses. With `-e`, the venv uses `py/server` and `py/client` as they are on disk, so Python edits need no reinstall. `editable_mode=compat` is needed for `py/server`: `deephaven-plugin` installs a `deephaven/plugin` folder into the venv, and with the default editable mode Python finds that folder instead of `py/server/deephaven`. The server tests need `py/client` too, because `tests/test_barrage.py` uses it.

3. With the venv active, run the script, optionally with test names:

   ```bash
   py/server/run-tests-venv.sh
   py/server/run-tests-venv.sh tests.test_csv tests.test_time
   ```

   Without test names, the script runs all of `tests/`.

The JUnit XML reports go to `py/server/build/test-results/test-venv/<version>/`. A copy of the tests goes to `py/server/build/test-venv/<version>/`. The script deletes both at the start of a run. It never installs anything into your venv, or changes or deletes it.

## What the script does

1. Checks that a venv is active, that it has `deephaven-core`, `deephaven-server` and `pydeephaven`.
2. Warns about any package from `requirements-test.txt` that the venv lacks. The tests that need it skip or fail on their own. Only `unittest-xml-reporting` is required, because it writes the reports.
3. Puts the jars from the venv's `deephaven-server` package on `DEEPHAVEN_CLASSPATH`, where the Docker image has `/opt/deephaven/server/lib`. `test_helper` then starts the JVM the same way in both. The classpath also gets the test config and the test calendars, which are plain files in the source tree. `DEEPHAVEN_DATA_DIR` and `DEEPHAVEN_CACHE_DIR` point at folders under `build/`, instead of the image's `/data` and `/cache`.
4. Sets `TZ=Etc/UTC`, because `tests/test_time.py` expects that time zone name.
5. For a free-threaded Python, sets `PYTHON_GIL=0`. Without it, importing `grpcio` through `pydeephaven` turns the GIL back on for the rest of the run.
6. Runs the tests from a fresh copy of `tests/`, because some tests write files into the working directory, and leftovers from an earlier run make them fail.

## Kafka and Postgres tests

`tests/test_kafka_consumer.py`, `tests/test_kafka_producer.py` and `tests/test_dbc.py` need Redpanda and Postgres. Without them, those tests fail. To run them, start the containers and map their host names to your machine:

```bash
docker compose -f Integrations/docker-compose.yml up -d --wait
echo "127.0.0.1 redpanda postgres" | sudo tee -a /etc/hosts
```

`tests/test_barrage.py` starts a second server on port 10000, so that port must be free. The client tests use the same port, so don't run both suites at once.

`tests/test_perfmon.py` needs the Graphviz `dot` program on your `PATH`.
