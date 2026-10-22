# Testing the Deephaven Python client

The tests in `py/client/tests` run in two ways:

- **In Docker**, with `./gradlew :py-client:testPyClient`. This is the suite that `check` runs. It tests against the server Docker image, and the client runs on the Python version in the `python` registry image.
- **Without Docker**, with `py/run-tests-venv.sh client`. It starts a server from the `deephaven-server` package, the embedded server, in a venv you set up, and runs the same tests against it, on any Python version. The `Nightly Python Versions CI` workflow runs it on every supported version, through `.github/scripts/py-versions-test.sh`, which makes a fresh venv for each version.

This page covers the second way. It does not measure coverage. For coverage reports, run the Docker task with `-Pcoverage.enabled=true`. For the server tests, see [`py/server/TESTING.md`](../server/TESTING.md). Both use the same script and can share one venv.

## Prerequisites

- A JDK, 17 or later. Set `JAVA_HOME` if `java` on your `PATH` is a different version.
- Docker, to build the `deephaven-server` wheel. The tests themselves do not use it.
- A tool to make the venv. The steps below use [uv](https://docs.astral.sh/uv/), which also downloads the Python version if needed. Any venv works.

## Run the tests

1. Build the `deephaven-server` wheel.

   ```bash
   ./gradlew :py-embedded-server:assemble
   ```

2. Make a venv with the Deephaven packages and the test dependencies, once per Python version. The server runs Python from this venv, so it needs `deephaven-core` as well as `pydeephaven`. `py/server/setup.py` and `py/client/setup.py` read their version from `DEEPHAVEN_VERSION`, which must match the wheel, because `deephaven-server` pins `deephaven-core` to it.

   ```bash
   export DEEPHAVEN_VERSION="$(basename py/embedded-server/build/wheel/deephaven_server-*.whl | cut -d- -f2)"
   uv venv --managed-python --python 3.14t .venv-3.14t
   source .venv-3.14t/bin/activate
   uv pip install py/embedded-server/build/wheel/deephaven_server-*.whl -e py/server -e py/client \
       --config-settings editable_mode=compat
   uv pip install -r py/client/requirements-test.txt
   ```

   The test dependencies install in a separate step, because one package with no wheel for your Python fails the whole command. If that happens, install the other packages from `requirements-test.txt` by name. The tests that need the missing one skip or fail on their own.

   The version can be anything uv accepts, such as `3.10`, `3.14` or `3.14t`. A version ending in `t` is the free-threaded build. With `-e`, Python edits need no reinstall. `editable_mode=compat` is needed for `py/server`: `deephaven-plugin` installs a `deephaven/plugin` folder into the venv, and with the default editable mode Python finds that folder instead of `py/server/deephaven`. A venv made for the server tests works here too, once you install `py/client/requirements-test.txt` into it.

3. With the venv active, run the script, optionally with test names:

   ```bash
   py/run-tests-venv.sh client
   py/run-tests-venv.sh client tests.test_session
   ```

   Without test names, the script runs all of `tests/`.

The JUnit XML reports and the server's `server.log` go to `py/client/build/test-results/test-venv/<version>/`. A copy of the tests goes to `py/client/build/test-venv/<version>/`. The script deletes both at the start of a run. It never installs anything into your venv, or changes or deletes it.

After a Java change, rebuild the wheel and reinstall it into the venv, as [`py/server/TESTING.md`](../server/TESTING.md#after-you-change-java-code) shows. The server runs on the jars installed in the venv.

## What the script does

1. Checks that a venv is active and that it has `deephaven-core`, `deephaven-server` and `pydeephaven`.
2. Warns about any package from `requirements-test.txt` that the venv lacks. The tests that need it skip or fail on their own. Only `unittest-xml-reporting` is required, because it writes the reports.
3. For a free-threaded Python, sets `PYTHON_GIL=0`. Without it, importing `grpcio` turns the GIL back on for the rest of the run.
4. Starts a server with the venv's `deephaven server` command on port 10000, bound to 127.0.0.1, so it runs on the same Python. It uses the same settings as the Docker-based `testPyClient` task: anonymous and PSK logins, with the PSK `deephaven`.
5. Runs the client tests against that server, then stops the server.

The server tests use port 10000 too, so don't run both suites at once.
