---
title: How do I import one Python script into another in the Deephaven IDE?
sidebar_label: How do I import one Python script into another in the Deephaven IDE?
---

<em>I have a Python script that defines some functions, classes, variables, or other objects. Can I use these in a new Python script from the Deephaven IDE?</em>

<p></p>

Yes. Python allows importing one script into another in the same way that a Python package would be imported, so long as those two scripts are in the same directory. Take the following example:

```python skip-test
# script1.py


def get_5():
    return 5


a = get_5()
```

```python skip-test
# script2.py

from script1 import *

b = get_5() + 1
c = a + b

print(a, b, c)
```

Here, `script2.py` imports all of the objects created in `script1.py` and uses them in its body. Naturally, this is expected to work from the Deephaven IDE. However, one extra step must be performed before this works as expected.

When the Deephaven server launches, it starts a Python interpreter that it will use to execute Python code. In order for `script2.py` in the above example to run, the Python interpreter must know about the existence of `script1.py`. Usually this is handled by default, because the Python interpreter is running from the same directory that the scripts are stored in. However, this is _not_ the case in Deephaven. Scripts are stored in a separate directory known as the [data directory](../../conceptual/docker-data-volumes.md#the-data-mount-point), and the interpeter does not know about the data directory by default. Therefore, **you must tell the Python interpreter where the data directory is located.** There are two ways to accomplish this, depending on whether the Deephaven server has already been started.

> [!WARNING]
> Adding the data directory to the Python path creates a security vulnerability, as it allows any browser connected to a server to edit files in the data directory. This should only be done in a secure environment.

## Method 1: After starting the server

If the Deephaven server has already been started, use the `append` method of the [`sys.path`](https://docs.python.org/3/library/sys.html#sys.path) list - you just need the path of the data directory.

### Docker

In Docker-run Deephaven, the data directory is `/data/storage/notebooks` by default. So, add that path to the set of paths known to the interpreter:

```python skip-test
import sys

sys.path.append("/data/storage/notebooks")
```

Then, the above example will run as expected:

![Running a Python script in the Deephaven IDE](../../assets/reference/community-questions/import-script-docker-1.gif)

### Pip

Using pip-installed Deephaven makes this slightly more complicated because the data directory depends on the operating system. However, the data directory prints to the console when the Deephaven server starts, so it can be copied from there. Then, you must append `"/storage/notebooks"` to the end of the path in the call to `sys.path.append`.

In this example, the console output indicates that the data directory is `/Users/username/Library/Application Support/io.Deephaven-Data-Labs.deephaven`. So, the code to add the path looks like this:

```python skip-test
import sys

sys.path.append(
    "/Users/username/Library/Application Support/io.Deephaven-Data-Labs.deephaven/storage/notebooks"
)
```

The scripts will now run with no problem:

<LoopedVideo src='../../assets/reference/community-questions/import-script-pip-1.mp4' />

### Remove an incorrect path

If you mistakenly add the wrong path to your Python session, use `sys.path.pop` to remove it, then add the correct one. Here is an example:

```python skip-test
import sys

# note the lack of /storage/notebooks - this won't work!
sys.path.append(
    "/Users/username/Library/Application Support/io.Deephaven-Data-Labs.deephaven"
)

# use sys.path.pop to remove the most recently added path
sys.path.pop()

# now, add the correct path
sys.path.append(
    "/Users/username/Library/Application Support/io.Deephaven-Data-Labs.deephaven/storage/notebooks"
)
```

## Method 2: Before starting the server

You can append the data directory to the interpreter's path before starting a Deephaven server using the `PYTHONPATH` environment variable.

### Docker

In Docker-run Deephaven, the data directory is `/data/storage/notebooks` by default. Add this path to the Docker image using a `docker-compose.yml` file or with the `--env` flag for the `docker run` command.

Here is a sample `docker-compose.yml` file that appends the data directory to `PYTHONPATH`:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - PYTHONPATH=${PYTHONPATH}:/data/storage/notebooks
      - START_OPTS=-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
```

Start the server using `docker compose up`, and the script import will work as expected:

![Starting a server with `docker compose up`](../../assets/reference/community-questions/import-script-docker-2.gif)

### Pip

The data directory for pip-installed Deephaven can be appended to the `PYTHONPATH` environment variable before starting the server. This requires setting the environment variable _before_ starting the Python console and the Deephaven server. That information is given when the server is started.

![A user sets the environment variable, then starts the Python console and a Deephaven server](../../assets/reference/community-questions/import-script-pip-2.gif)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
