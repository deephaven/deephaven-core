---
title: Deephaven MCP
---

[Deephaven MCP](https://github.com/deephaven/deephaven-mcp) connects Deephaven to your AI development workflow. It uses the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/docs/getting-started/intro), an open-source standard for connecting AI applications to external systems such as databases and filesystems. MCP connects AI applications to services the way USB connects external devices to computers.

With Deephaven MCP, AI models can query tables and run scripts directly in your Deephaven sessions. They can also write Deephaven code for you. The tables the AI builds in your session are ordinary Deephaven tables. When they're built on live data, they [keep updating](../conceptual/table-update-model.md) as new data arrives. While AI models do the busy work, you get to focus on turning that work into actionable insights.

Deephaven MCP has three components:

- **Systems server** — a server that manages and connects to your [Deephaven Community Core](https://deephaven.io/community/) and [Deephaven Enterprise](https://deephaven.io/enterprise/) systems, exposing sessions and tables to your AI tools.
- **Docs server** — a public server hosted by Deephaven that answers natural-language questions about Deephaven, using its documentation to return code examples and explanations.
- **`dhcli`** — a command-line tool that configures and operates your Deephaven systems from the shell.

## Get started

The [Deephaven MCP README on GitHub](https://github.com/deephaven/deephaven-mcp#readme) covers installation, configuration, and usage, including the `dhcli` command-line tool and setup for AI tools such as Claude Desktop, Cursor, VS Code, and Windsurf.

You don't need a running Deephaven server to get started, because `dhcli` can start Community Core sessions for you.

To use a Deephaven server you run yourself, start one with the [Quickstart](../getting-started/quickstart.md). Then add it as a session, following the [Community Core Quick Start](https://github.com/deephaven/deephaven-mcp#community-core-quick-start) in the README.

## Related documentation

- [Quickstart](../getting-started/quickstart.md)
- [Install and run with Docker](../getting-started/docker-install.md)
- [Install and run Deephaven with pip](../getting-started/pip-install.md)
- [Build and run Deephaven from source code](../getting-started/launch-build.md)
- [Incremental update model](../conceptual/table-update-model.md)
