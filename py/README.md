# Deephaven Python Source

This folder contains Deephaven Community Core's Python source code, tests, and more. Source code can be found in `server/deephaven` and unit tests can be found in `server/tests`. Other content includes `jpy`, the Python client, wheels, and more.

## Requirements

Development and contribution requires _at least_ Python 3.8 or later. We recommend always staying up-to-date on the latest Python version.

## Virtual environments

Deephaven Python development should always be done from a [virtual environment](https://docs.python.org/3/library/venv.html). See [PEP 405](https://peps.python.org/pep-0405/) for the motivation behind virtual environments (venvs). Creating and activating a venv takes two shell commands.

From the root directory of your `deephaven-core` clone:

```sh
python3 -m venv .venv # Create a venv. Only needed once.
source .venv/bin/activate # Activate the virtual environment.
```

Once finished with a virtual environment, exiting is done with a single command.

```sh
deactivate # Exit the venv when finished
```

## Local Development

Using a `pip` editable install is recommended so that Python changes are reflected when your server is restarted. These instructions are for the Python server package in `py/server` but should apply to any other Python packages.

Install the Python server package. This will install the dependencies as well as the autocomplete dependencies.

```sh
DEEPHAVEN_VERSION=0.23.0 pip install -e "py/server[autocomplete]"
```

**Note:** Pip may report an error that the version is not what is expected if you have installed the locally built version before. Adjust the version to match what `pip` expects.

After installing, a `pip list` should include `deephaven-core` with an editable project location listed.

Now you can start the Java server by following the instructions [here](../server/jetty-app/README.md). Any time you make Python changes, restart the server to apply them.

## Local Build

You can use these instructions if you do not plan on making frequent changes to your local Python environment or need to try things in a more production-like environment locally.

First, run the following command to build the server and embedded-server if needed. The embedded-server is used to start [pip-installed Deephaven](https://deephaven.io/core/docs/how-to-guides/configuration/native-application/#python-embedded-server).

```sh
./gradlew :py-server:assemble

# If using embedded-server
# You can run both tasks w/ ./gradlew :task1 :task2 if you need both
./gradlew :py-embedded-server:assemble
```

Then run the following commands to install the server package. Note that the `wheel` folder should only contain 1 file, so you should be able to tab-complete the wheel file.

**Note:** Adding the `--no-deps` flag on subsequent installs will skip re-installing other dependencies.

```sh
pip install --force py/server/build/wheel/deephaven_core-<version>-py3-none-any.whl

# If using embedded-server
# You can combine as pip install core.whl server.whl if you need both
pip install --force py/embedded-server/build/wheel/deephaven_server-<version>-py3-none-any.whl
```

**If you make Python code changes that affect the wheel, you will need to re-install it.**

Start the Java server by following the instructions [here](../server/jetty-app/README.md).

## Debugging

To attach a remote debugger and set breakpoints, use the `-Pdebug` option.

```sh
./gradlew :py-server:assemble -Pdebug
```

## Tests

Python unit tests must be run from the root directory of the cloned repository.

```sh
cd ..
./gradlew integrations:test-py-deephaven
```