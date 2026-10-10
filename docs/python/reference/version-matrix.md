---
title: Version support matrix
---

This page lists the Python versions, server Python dependencies, Java (JDK) versions, operating systems and CPU architectures, and client library platforms that Deephaven Community supports and tests.

## Python versions

A :white_check_mark: indicates that the Deephaven version supports the Python version, and a blank cell indicates that it does not.

| Deephaven version |     Python 3.8     |     Python 3.9     |    Python 3.10     |    Python 3.11     |    Python 3.12     |    Python 3.13     |
| ----------------- | :----------------: | :----------------: | :----------------: | :----------------: | :----------------: | :----------------: |
| 0.40.x            | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| 41.x and later    |                    | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |

Deephaven tests primarily with Python 3.12.

Plugins in the [deephaven-plugins](https://github.com/deephaven/deephaven-plugins) repository support Python 3.9 through 3.13, and Deephaven tests them with each of those versions.

## Server Python dependencies

When the Deephaven server uses Python rather than Groovy as its query language, the server and web UI have hard dependencies on the Python packages `jpy`, `deephaven-plugin`, `numpy`, `pandas`, and `pyarrow` (Apache Arrow). The server requires every hard dependency to run.

The server also has soft dependencies on `numba` and `jedi`, which are optional packages that turn on extra features. To install `jedi`, use the `autocomplete` extra: `pip install "deephaven-core[autocomplete]"`. In 0.40.x and 41.x, `pip` installs `numba` automatically on Python versions earlier than 3.13. Soft dependencies might not work on every supported Python version, though Deephaven makes a best effort to support them.

The [`deephaven-core` package metadata on PyPI](https://pypi.org/project/deephaven-core/) declares the hard dependencies, including minimum versions for `jpy`, `deephaven-plugin`, and `pandas`. `pip` resolves these dependencies at install time. Deephaven cannot test every combination of dependency versions, and some combinations conflict with each other.

## Java (JDK) versions

Each Deephaven release requires a minimum JDK version. Deephaven aims to support that version and every later Java long-term support (LTS) version.

A :white_check_mark: indicates that Deephaven tests that release against the JDK version. For each release, the leftmost checked column is its minimum JDK version. A blank cell to the left of it is unsupported, and a blank cell to the right of it is untested.

In 0.40.x through 42.x, JDK 11 support covers the Deephaven libraries and the Jetty 11 and Netty server variants. The default Jetty 12 server and the pip-installed `deephaven-server` package require JDK 17 or later.

| Deephaven version |    JDK 11 (LTS)    |    JDK 17 (LTS)    |    JDK 21 (LTS)    |    JDK 25 (LTS)    |
| ----------------- | :----------------: | :----------------: | :----------------: | :----------------: |
| 0.40.x            | :white_check_mark: | :white_check_mark: | :white_check_mark: |                    |
| 41.x              | :white_check_mark: | :white_check_mark: | :white_check_mark: |                    |
| 42.x              | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| 43.x              |                    | :white_check_mark: | :white_check_mark: | :white_check_mark: |

The table shows only LTS versions. Deephaven also tested 0.40.x and 41.x against JDK 24, a non-LTS release.

Deephaven builds and tests with OpenJDK packages. Deephaven does not regularly run or test with GraalVM.

## Operating system and CPU architecture

Deephaven Community is built on OS-neutral, multiplatform technologies such as Java and Python. Even so, package dependencies and differences between execution environments make it hard and slow to guarantee support for many platforms.

Well-tested platforms:

- Linux x86_64, particularly Ubuntu 24.04. Deephaven tests on this platform regularly.
- macOS on Arm CPUs. Many Deephaven developers use this platform daily.

If a package change in a core OS distribution breaks Deephaven Community on either platform, Deephaven developers notice quickly.

Deephaven does not test the following platforms extensively but expects them to work:

- RHEL 8 x86_64 and Fedora 39 x86_64. Deephaven does not regularly test the server on these platforms.
- Windows x86_64 under [Windows Subsystem for Linux 2 (WSL 2)](https://learn.microsoft.com/en-us/windows/wsl/install). Some users run Deephaven on this platform.
- macOS x86_64.
- Docker on any host that supports it, running a Deephaven-published image.

:::note
Deephaven publishes its images for both Linux x86_64 and Linux Arm64, so Docker uses a native image on Apple silicon Macs. Running an image built for a different CPU architecture than the host requires emulation, which can noticeably reduce performance. For example, a Linux x86_64 image on an Apple silicon Mac runs under emulation.
:::

## Client libraries

This section covers the Python, C++, and R clients. It does not list the Java, JavaScript, Go, or C# (.NET) clients. For the Java and JavaScript clients, see [Java client](../how-to-guides/java-client.md) and [JavaScript API](../how-to-guides/use-jsapi.md).

Deephaven has two Python clients:

- `pydeephaven` works with both [static](../conceptual/table-types.md#static-tables) and [ticking](../conceptual/table-types.md#standard-streaming-tables) tables on the server, and retrieves table data as Apache Arrow snapshots.
- `pydeephaven-ticking` adds listeners that receive each update to a ticking table, and depends on Deephaven's C++ client code through Cython.

| Client                | How it is distributed                                                                              | Platforms built or tested                                                                                                                                                                                                                 | Not supported      |
| --------------------- | -------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| `pydeephaven`         | Platform-agnostic wheel on PyPI                                                                    | Any platform that supports its dependencies, mainly `grpcio`, `numpy`, `pandas`, and `pyarrow`                                                                                                                                            | None               |
| `pydeephaven-ticking` | Prebuilt [`manylinux2014`](https://peps.python.org/pep-0599/) wheels on PyPI for Linux x86_64 only | Linux x86_64, and Windows x86_64 when built from source. Client tests run regularly on Fedora 39 with Python 3.12 against a Deephaven server in Docker. The tests can also run on RHEL 8, outside regular continuous integration testing. | macOS, Linux Arm64 |
| C++ client            | No binary packages; build from source                                                              | Ubuntu 22.04 x86_64, tested regularly. The `pydeephaven-ticking` tests also exercise `dhcore`, the C++ client's core data library, on Fedora 39 x86_64. Windows x86_64.                                                                   | macOS              |
| R client              | Not published on CRAN (the Comprehensive R Archive Network) or elsewhere; build from source        | Ubuntu 22.04 x86_64 with the current R 4.x release from CRAN                                                                                                                                                                              | Windows, macOS     |

Build instructions:

- [C++ client on Linux](https://github.com/deephaven/deephaven-core/blob/main/cpp-client/BUILDING.md)
- [C++ client and `pydeephaven-ticking` on Windows](https://github.com/deephaven/deephaven-core/blob/main/cpp-client/README-windows.md)
- [R client on Linux](https://github.com/deephaven/deephaven-core/blob/main/R/rdeephaven/BUILDING.md)

## Related documentation

- [Install and run with Docker](../getting-started/docker-install.md)
- [Install and run Deephaven with pip](../getting-started/pip-install.md)
- [Build and run Deephaven from source code](../getting-started/launch-build.md)
- [Python client quickstart](../getting-started/pyclient-quickstart.md)
