---
title: Version support matrix
---

## Python versions

Note that deephaven-core supports Python 3.8+ but is tested with Python 3.10 (soft dependencies might not work, but our best effort is made to ensure they do).

Any plugins housed in deephaven-plugins work for 3.8+ and are tested with Python 3.8, 3.9, 3.10, 3.11, and 3.12.

| Deephaven Version |     Python 3.8     |     Python 3.9     |    Python 3.10     |    Python 3.11     |    Python 3.12     |
| ----------------- | :----------------: | :----------------: | :----------------: | :----------------: | :----------------: |
| Any               | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |

## Java JDK

We aim to support all Java LTS versions greater than or equal to our minimum Java LTS version, as well as the latest Java version, based on the timing of the specific release. A :white_check_mark: indicates that Deephaven has been tested against the JDK version.

| Deephaven Version |    JDK 11 (LTS)    |    JDK 17 (LTS)    |    JDK 21 (LTS)    |  JDK 22 (latest)   |
| ----------------- | :----------------: | :----------------: | :----------------: | :----------------: |
| 0.33.x            | :white_check_mark: | :white_check_mark: | :white_check_mark: |                    |
| 0.34.0            | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |

We build and test using OpenJDK packages. We do not regularly run or test with GraalVM.

## Operating System and CPU architecture

deephaven-core is based on OS-neutral, multiplatform technologies like Java and Python; however, package dependencies and differences in execution environments mean that guaranteeing a wide range of platforms is challenging and time-consuming. We regularly test in Linux x86_64, particularly Ubuntu 22.04. Many of our developers use MacOS with Arm CPUs. These platforms are generally well-tested, and we will notice quickly if some change in package versions in core distributions of the OS makes deephaven-core stop working.

In terms of other systems where we don’t extensively test ourselves but a priori expect things to work:

- We have some test subsets that regularly run on RHEL 8 x86_64 and Fedora 38 x86_64; while much narrower in coverage, these tests provide a basic check that the server can start and a client can run simple queries against the server.
- Windows x86_64 under WSL 2; we know some users running on this platform.
- MacOS x86_64.
- Docker on any platform supporting it, on an image of a Deephaven-supported platform. Note: running an image for a different CPU architecture than the host (e.g., a Linux x86_64 image on a MacOS M1 host) may see a noticeable performance impact due to the need for emulation.

## Notes on specific dependencies

- Deephaven Server and web UI when built with a Python REPL have hard dependencies on Python packages for jpy, numpy, pandas and pyarrow (Apache Arrow), and soft dependencies on numba and jedi. We specify version requirements in the traditional Python fashion; for the reasons noted earlier, we do not test every version combination and it is possible (and known to have happened in the past) that particular versions may cause conflicts.
- We have two flavors of Python client: "pydeephaven" with regular Arrow support for static tables, and "pydeephaven_ticking" adding support for ticking tables.
  - The platform-agnostic "pydeephaven" wheel is uploaded to PyPi on every release. It can be installed on any platform that supports its dependencies, the main ones being numpy, pandas, and pyarrow.
  - The "pydeephaven_ticking" package depends on Deephaven’s C++ client code via Cython. We build and test "pydeephaven_ticking" wheels for Linux x86_64 using manylinux2014 tools, and these also get uploaded to PyPi on every release. At this time, we do not support building "pydeephaven_ticking" for platforms other than Linux (e.g., Windows or MacOS). We do not build or test ourselves "pydeephaven_ticking" on Linux arm.
- We do not build binary packages for our C++ client, but we do provide instructions for building it from source. The code is regularly tested on Ubuntu 22.04 x86_64, and basic smoke tests are run for RHEL 8 x86_64 and Fedora 38 x86_64. At this time we do not support building our C++ client for platforms other than Linux and Windows (e.g., MacOS).
- We do not build packages for our R client or publish it on CRAN, but we do provide instructions for building from source. The code is regularly tested on Ubuntu 22.04 x86_64 and R 4.3.1 "Beagle Scouts". At this time, we do not support building our R client for platforms other than Linux (e.g., Windows or MacOS).
