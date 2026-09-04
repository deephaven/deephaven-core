---
title: Deephaven MCP
sidebar_label: MCP
---

Deephaven MCP connects Deephaven to your AI development workflow using the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/docs/getting-started/intro) — an open-source standard for connecting AI applications to external services like databases, filesystems, and Deephaven. AI models can query tables, generate code, and execute scripts directly against your Deephaven sessions.

Deephaven MCP has two components:

- **Systems server** — manages and connects to your [Deephaven Community Core](https://deephaven.io/community/) systems, exposing sessions and tables to your AI tools.
- **Docs server** — answers natural-language questions about Deephaven from its documentation knowledge base, returning code examples and explanations.

## Get started

Installation, configuration, and usage — including the `dhcli` command-line tool and setup for each AI tool (Claude Desktop, Cursor, VS Code, Windsurf, and more) — live in the **[Deephaven MCP README on GitHub](https://github.com/deephaven/deephaven-mcp#readme)**.
