---
title: Why am I getting a "Java class not found" error when starting deephaven-server?
---

<em>I'm getting a `ValueError: Java class 'io.deephaven.python.server.EmbeddedServer' not found` error when trying to start the Deephaven server using the `deephaven-server` wheel. What's causing this?</em>

<p></p>

## The problem

You see an error like this when running `deephaven server`:

```text
ValueError: Java class 'io.deephaven.python.server.EmbeddedServer' not found
```

This error typically indicates that you're using an incompatible Java version.

## The solution

**You must use Java 17 or later** when running `deephaven-server` version 0.36.0 and above. See the [pip install prerequisites](../../getting-started/pip-install.md#prerequisites) and [version matrix](../version-matrix.md) for supported Java versions.

The Deephaven server wheel requires Java 17 or later because:

- Jetty 12 (used by Deephaven) requires Java 17 or later.
- Jetty 11 reached end-of-life, prompting the upgrade.

### How to fix

1. **Check your Java version:**

   ```bash
   java -version
   ```

2. **Install Java 17 or later** if needed:

   On Ubuntu/Debian:

   ```bash
   apt-get install openjdk-17-jdk
   ```

   On macOS:

   ```bash
   brew install openjdk@17
   ```

3. **Ensure Java 17 or later is being used** by your environment before starting the server. If you have multiple Java versions installed, set `JAVA_HOME` to point to a Java 17 or later installation:

   On Linux:

   ```bash
   export JAVA_HOME=/path/to/java17
   export PATH=$JAVA_HOME/bin:$PATH
   ```

   On macOS (with Homebrew):

   ```bash
   export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
   export PATH=$JAVA_HOME/bin:$PATH
   ```

   On Windows (Command Prompt):

   ```cmd
   set JAVA_HOME=C:\Path\To\Java17
   set PATH=%JAVA_HOME%\bin;%PATH%
   ```

   On Windows (PowerShell):

   ```powershell
   $env:JAVA_HOME = "C:\Path\To\Java17"
   $env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
   ```

   To make these changes permanent, add them to your shell configuration file (for example, `~/.bashrc`, `~/.zshrc`, or `~/.bash_profile`). Run `java -version` again to confirm the change took effect.

### Example Dockerfile

If you're using Docker, make sure to install the correct Java version:

```dockerfile
FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update

RUN apt-get install -y \
    python3.12 \
    python3.12-venv \
    python3.12-dev \
    g++ \
    openjdk-17-jdk  # Use Java 17, not Java 11

# Create and activate virtual environment
RUN python3.12 -m venv /venv
RUN /venv/bin/pip install deephaven-server

# Start server
CMD ["/venv/bin/deephaven", "server"]
```

## Additional notes

- The `pydeephaven` client package does not require Java. Only the server-side packages (`deephaven-server`, and the `deephaven-core` engine package it installs) need a JVM.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
