# Deephaven Python Development

## Requirements

- Python >=3.8

## Setting up Virtual Environment

For any of the options below, you should first set up a virtual environment in Python. Once created, you must activate the virtual environment every time you open a shell. 

```sh
python3 -m venv .venv # Create. Only needed once
source .venv/bin/activate # Activate the virtual environment
deactivate # Exit virtual environment when done
```

**Note:**

- This is not an exhaustive guide to managing Python environments.
- Depending on your OS and how your PATH is set up, you may need to use `python3`, or a path to the explicit Python version you want to use.
- You may set up a "permanent" virtual environment location.
- You'll need to re-install the wheel whenever you make Python code changes that affect the wheel.
- `pip` can be a pain if you are trying to (re-)install a wheel with the same version number as before.
  - A `pip install --force-reinstall --no-deps "py/server/build/wheel/deephaven_core-<version>-py3-none-any.whl[autocomplete]"` may do the trick.
- You can install other Python packages in your venv using `pip install <some-other-package>`.
- You can set up multiple virtual environments and switch between them as necessary using `source /path/to/other-venv/bin/activate`.
- You can de-activate the virtual environment by running `deactivate`.
- You can use the `VIRTUAL_ENV` environment variable instead of sourcing/activating virtual environments: `VIRTUAL_ENV=/my/venv ./gradlew server-jetty-app:run`.

## Local Development

Using a `pip` editable install is recommended so that Python changes are reflected when your server is restarted. These instructions are for the Python server package in `py/server` but should apply to any other Python packages.

Install the Python server package. This will install the dependencies as well as the autocomplete dependencies.

```sh
DEEPHAVEN_VERSION=0.23.0 pip install -e "py/server[autocomplete]"
```

**Note:** Pip may report an error that the version is not what is expected if you have installed the locally built version before. Adjust the version to match what `pip` expects.

After installing, you can run `pip list` and you should see the package `deephaven-core` with an editable project location listed.

Now you can start the Java server by following the instructions [here](../server/jetty-app/README.md). Any time you make Python changes, restart the server to apply them.

## Local Build

You can use these instructions if you do not plan on making frequent changes to your local Python environment or need to try things in a more production-like environment locally.

First, run the following command to build the server and embedded-server if needed. The embedded-server is used to start [Deephaven from Python](https://deephaven.io/core/docs/how-to-guides/configuration/native-application/#python-embedded-server).

```sh
./gradlew :py-server:assemble

# If using embedded-server
# You can run both tasks w/ ./gradlew :task1 :task2 if you need both
./gradlew :py-embedded-server:assemble
```

Then run the following commands to install the server package. Note that the `wheel` folder should only contain 1 file, so you should be able to tab-complete the wheel file.

**Note:** Add the `--no-deps` flag on subsequent installs to skip re-installing other dependencies.

```sh
pip install --force py/server/build/wheel/deephaven_core-<version>-py3-none-any.whl

# If using embedded-server
# You can combine as pip install core.whl server.whl if you need both
pip install --force py/embedded-server/build/wheel/deephaven_server-<version>-py3-none-any.whl
```

Start the Java server by following the instructions [here](../server/jetty-app/README.md).

## Tests

This section needs info about how to run tests.
