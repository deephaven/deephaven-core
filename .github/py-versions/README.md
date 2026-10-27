# Python versions CI

The `Nightly Python Versions CI` workflow (`.github/workflows/nightly-py-versions-ci.yml`) runs the tests in `py/server/tests` and `py/client/tests` on every supported Python version, without Docker. The `check` task still runs the same tests in Docker, through `:Integrations:test-py-deephaven` and `:py-client:testPyClient`, on one Python version.

This folder holds the pieces the workflow uses:

- `run-tests-venv.sh <server|client> [test ...]` runs one suite in the active venv. It installs nothing and never changes the venv.
- `requirements-server.txt` and `requirements-client.txt` list the packages each suite needs beyond `deephaven-core`, `deephaven-server` and `pydeephaven`. Versions are open, so each run gets the newest releases.

For each Python version, `.github/scripts/py-versions-test.sh` makes a fresh venv with uv, installs the three Deephaven wheels and the requirements file, then calls `run-tests-venv.sh`.

- **server**: `test_helper` starts the JVM in the test process, on the jars from the venv's `deephaven-server` package. The Docker image has the same jars in `/opt/deephaven/server/lib`.
- **client**: the script starts a server with the venv's `deephaven server` command on port 10000, bound to 127.0.0.1, with anonymous and PSK logins, PSK `deephaven`. Then it runs the client tests against it.

It does not measure coverage. For coverage, run the Docker tasks with `-Pcoverage.enabled=true`.

## Reproduce a failure locally

Run the CI driver with the failing suite and Python version. It needs uv, a JDK 17 or later, and Docker to build the wheels.

```bash
./gradlew :py-embedded-server:assemble :py-server:assemble :py-client:assemble
.github/scripts/py-versions-test.sh server 3.14t
.github/scripts/py-versions-test.sh server 3.14t tests.test_csv tests.test_time
```

The venv goes to `py/<suite>/build/py-versions/<version>/venv`. The JUnit XML reports, and the client suite's `server.log`, go to `py/<suite>/build/test-results/test-venv/<version>/`. The driver deletes the venv and the reports at the start of each run.

To iterate without a rebuild, activate that venv and call the script directly. Python edits under `py/server/tests` and `py/client/tests` need no reinstall, since the script copies the tests fresh each run.

```bash
source py/server/build/py-versions/3.14t/venv/bin/activate
.github/py-versions/run-tests-venv.sh server tests.test_csv
```

Edits to `py/server/deephaven` or `py/client/pydeephaven`, and Java changes, need the wheels rebuilt and the driver run again.

## Kafka and Postgres tests

`tests/test_kafka_consumer.py`, `tests/test_kafka_producer.py` and `tests/test_dbc.py` need Redpanda and Postgres. Without them, those tests fail. To run them, start the containers and map their host names to your machine, as the workflow does:

```bash
docker compose -f Integrations/docker-compose.yml up -d --wait
echo "127.0.0.1 redpanda postgres" | sudo tee -a /etc/hosts
```

`tests/test_barrage.py` starts a second server on port 10000, and the client suite uses the same port, so don't run both suites at once. `tests/test_perfmon.py` needs the Graphviz `dot` program on your `PATH`.
