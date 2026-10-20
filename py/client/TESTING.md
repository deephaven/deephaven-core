# Testing the Deephaven Python client

The tests in `py/client/tests` run in two ways:

- **In Docker**, with `./gradlew :py-client:testPyClient`. This is the suite that `check` runs. It uses the Python version in the `python` registry image.
- **Without Docker**, with `py/client/run-tests-embedded.sh`. It runs the same tests in a venv you set up, on any Python version, against the `server-jetty` distribution, the same server the Docker image runs. The `Nightly Python Versions CI` workflow runs it on every supported version, through `.github/scripts/py-versions-test.sh`, which makes a fresh venv for each version.

This page covers the second way. It does not measure coverage. For coverage reports, run the Docker task with `-Pcoverage.enabled=true`. For the server tests, see [`py/server/TESTING.md`](../server/TESTING.md).

## Prerequisites

- A JDK, 17 or later. Set `JAVA_HOME` if `java` on your `PATH` is a different version.
- A tool to make the venv. The steps below use [uv](https://docs.astral.sh/uv/), which also downloads the Python version if needed. Any venv works.

## Run the tests

1. Build the `server-jetty` distribution. Rebuild after you change Java code.

   ```bash
   ./gradlew :server-jetty-app:installDist
   ```

2. Make a venv with `pydeephaven`, `deephaven-core` and the test dependencies, once per Python version. The server runs Python from this venv, so it needs `deephaven-core` too. `py/server/setup.py` and `py/client/setup.py` read their version from `DEEPHAVEN_VERSION`. Any version works here. These steps take the server's.

   ```bash
   export DEEPHAVEN_VERSION="$(basename server/jetty-app/build/install/server-jetty/lib/deephaven-engine-table-*.jar .jar | sed 's/^deephaven-engine-table-//')"
   uv venv --managed-python --python 3.14t .venv-3.14t
   source .venv-3.14t/bin/activate
   uv pip install -e py/server -e py/client -r py/client/requirements-test.txt --config-settings editable_mode=compat
   ```

   The version can be anything uv accepts, such as `3.9`, `3.14` or `3.14t`. A version ending in `t` is the free-threaded build. With `-e`, Python edits need no reinstall. `editable_mode=compat` is needed for `py/server`: `deephaven-plugin` installs a `deephaven/plugin` folder into the venv, and with the default editable mode Python finds that folder instead of `py/server/deephaven`. A venv made for the server tests in `py/server/TESTING.md` works here too, once you add `-r py/client/requirements-test.txt`.

3. With the venv active, run the script, optionally with test names:

   ```bash
   py/client/run-tests-embedded.sh
   py/client/run-tests-embedded.sh tests.test_session
   ```

   Without test names, the script runs all of `tests/`.

The JUnit XML reports and the server's `server.log` go to `py/client/build/test-results/test-embedded/<version>/`. A copy of the tests goes to `py/client/build/test-embedded/<version>/`. The script deletes both at the start of a run. It never installs anything into your venv, or changes or deletes it.

## What the script does

1. Checks that a venv is active, that it has `pydeephaven` and `deephaven-core`, and that the server distribution is built.
2. Warns about any package from `requirements-test.txt` that the venv lacks. The tests that need it skip or fail on their own. Only `unittest-xml-reporting` is required, because it writes the reports.
3. For a free-threaded Python, sets `PYTHON_GIL=0`. Without it, importing `grpcio` turns the GIL back on for the rest of the run.
4. Starts the `server-jetty` distribution on port 10000, bound to 127.0.0.1. Java is the main program and finds Python through `VIRTUAL_ENV`, your venv, so the server runs on the same Python. It uses the same settings as the Docker-based `testPyClient` task: anonymous and PSK logins, with the PSK `deephaven`.
5. Runs the client tests against that server, then stops the server.

The server tests use port 10000 too, so don't run both suites at once.
