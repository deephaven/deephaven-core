---
title: Why am I getting a "Java class not found" error when starting deephaven-server?
sidebar_label: Why am I getting a "Java class not found" error when starting deephaven-server?
---

<em>I'm getting a cryptic `ValueError: Java class 'io.deephaven.python.server.EmbeddedServer' not found` error when trying to start the Deephaven server using the `deephaven-server` wheel. What's causing this?</em>

<p></p>

## The Problem

If you see an error like this when running `deephaven server`:

```python
ValueError: Java class 'io.deephaven.python.server.EmbeddedServer' not found
```

This cryptic error message typically indicates that you're using an incompatible Java version.

## The Solution

**You must use Java 17 or later** when running `deephaven-server` version 0.36.0 and above.

The Deephaven server wheel requires Java 17+ because:
- Jetty 12 (used by Deephaven) requires Java 17 or later
- Jetty 11 reached end-of-life, prompting the upgrade

### How to Fix

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

3. **Ensure Java 17 is being used** by your environment before starting the server.

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

## Additional Notes

- The `deephaven-core` wheel (for client-side use) does not require Java to be installed.
- The `deephaven-server` wheel requires Java 17+ to be available in your environment.
- When creating a `Server` instance in Python (e.g., `from deephaven_server.server import Server`), you must have Java 17+ installed and available.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
