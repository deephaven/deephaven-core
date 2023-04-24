# Deephaven Community Core

![Deephaven Data Labs Logo](docs/images/Deephaven_GH_Logo.svg)

[Quick start for Docker user guide](https://deephaven.io/core/docs/tutorials/quickstart)

Deephaven Community Core is a real-time, time-series, column-oriented analytics engine
with relational database features.
Queries can seamlessly operate upon both historical and real-time data.
Deephaven includes an intuitive user experience and visualization tools.
It can ingest data from a variety of sources, apply computation and analysis algorithms
to that data, and build rich queries, dashboards, and representations with the results.

Deephaven Community Core is an open version of [Deephaven Enterprise](https://deephaven.io),
which functions as the data backbone for prominent hedge funds, banks, and financial exchanges.

[![Join the chat at https://gitter.im/deephaven/deephaven](https://badges.gitter.im/deephaven/deephaven.svg)](https://gitter.im/deephaven/deephaven?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
![Build CI](https://github.com/deephaven/deephaven-core/actions/workflows/build-ci.yml/badge.svg?branch=main)
![Quick CI](https://github.com/deephaven/deephaven-core/actions/workflows/quick-ci.yml/badge.svg?branch=main)
![Docs CI](https://github.com/deephaven/deephaven-core/actions/workflows/docs-ci.yml/badge.svg?branch=main)
![Check CI](https://github.com/deephaven/deephaven-core/actions/workflows/check-ci.yml/badge.svg?branch=main)
![Nightly Check CI](https://github.com/deephaven/deephaven-core/actions/workflows/nightly-check-ci.yml/badge.svg?branch=main)
![Nightly Benchmarks](https://github.com/deephaven/deephaven-core/actions/workflows/nightly-benchmarks.yml/badge.svg?branch=main)

## Supported Languages

| Language      | Server Application | Client Application |
| ------------- | ------------------ | ------------------ |
| Python        | Yes                | Yes                |
| Java / Groovy | Yes                | Yes                |
| C++           | No                 | Yes                |
| JavaScript    | No                 | Yes                |
| gRPC          | -                  | Yes                |

## Run Deephaven

If you want to use Deephaven, see our [Quick start for Docker guide](https://deephaven.io/core/docs/tutorials/quickstart).

Developers interested in modifying source code should read on for development instructions.

## Development Instructions

### Required Dependencies

Developing in Deephaven requires a few software packages.

| Package | Version                       | OS           |
| ------- | ----------------------------- | ------------ |
| Java    | >=11, <20                     | All          |
| Python  | >= 3.8                        | All          |
| Windows | 10 (OS build 20262 or higher) | Only Windows |
| WSL     | 2                             | Only Windows |

> :warning: **On Windows, all commands must be run inside a WSL 2 terminal.**

### Local Development

See the following READMEs for info about developing in various environments within the Deephaven core repository.

- [Java](./server/jetty-app/README.md)
- [Python](./py/README.md)
- [Web](./web/client-ui/README.md)
- [Go](./go/README.md)
- [C++](./cpp-client/README.md)

## Resources

- [Deephaven Community Slack](https://deephaven.io/slack)
- [GitHub Discussions](https://github.com/deephaven/deephaven-core/discussions)
- [Deephaven Community Core docs](https://deephaven.io/core/docs/)
- [Java API docs](https://deephaven.io/core/javadoc/)
- [Python API docs](https://deephaven.io/core/pydoc/)

## Code Of Conduct

This project has adopted the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/0/code_of_conduct/).
For more information see the [Code of Conduct](CODE_OF_CONDUCT.md) or contact [opencode@deephaven.io](mailto:opencode@deephaven.io)
with any additional questions or comments.

## License

Copyright &copy; Deephaven Data Labs. All rights reserved.

Provided under the [Deephaven Community License](LICENSE.md).
