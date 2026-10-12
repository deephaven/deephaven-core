---
title: Deephaven's GitHub Copilot extension (discontinued)
---

> [!WARNING]
> GitHub [disabled all GitHub App-based Copilot Extensions on November 10, 2025](https://github.blog/changelog/2025-09-24-deprecate-github-copilot-extensions-github-apps/) in favor of Model Context Protocol (MCP) servers. The Deephaven Copilot extension on this page is a GitHub App-based extension, so starting a Copilot Chat message with `@deephaven` no longer gets a response. To get Deephaven-specific help in Copilot and other AI tools, use [Deephaven MCP](./mcp.md) instead. Its docs server answers natural-language questions about Deephaven.

Deephaven's [GitHub Copilot extension](https://github.com/apps/deephaven) gave GitHub Copilot access to Deephaven's documentation and code so it could answer Deephaven-specific questions. The rest of this page describes how the extension worked. While it was available, you could use it for things like:

- Writing Deephaven queries using natural language.
- Proofreading, refactoring, and suggesting improvements to your Deephaven code.
- Reviewing, critiquing, and providing feedback on your queries.
- Answering questions about Deephaven's API and features.

## Prerequisites

To use the Deephaven GitHub Copilot extension, you needed:

- A GitHub account.
- [GitHub Copilot](https://github.com/features/copilot) on any plan, from Free to Enterprise.
- Deephaven ([Community Core](/community) or [Enterprise](/enterprise)) to run the code that Copilot suggested, installed with [Docker](../getting-started/docker-install.md), [pip](../getting-started/pip-install.md), [from source](../getting-started/launch-build.md), or as a [production application](../getting-started/production-application.md). The extension itself didn't need a running server.

## Installation

These were the setup steps while the extension was available. Following them now doesn't make `@deephaven` respond. For a working setup, see [Deephaven MCP](./mcp.md).

### 1. Install the Deephaven GitHub Copilot extension

You installed the extension by selecting **Install** at [github.com/apps/deephaven](https://github.com/apps/deephaven) and following the instructions.

### 2. Set up Copilot in your IDE (optional)

You could use Copilot on [github.com](https://github.com/copilot), but we recommended using it from one or more of the IDEs that GitHub Copilot supports. For each IDE, you installed the IDE and then its Copilot plugin:

- [Microsoft Visual Studio Code (VS Code)](https://code.visualstudio.com/) — [Copilot setup](https://code.visualstudio.com/docs/copilot/setup)
- [Microsoft Visual Studio](https://visualstudio.microsoft.com/) — [Copilot setup](https://learn.microsoft.com/en-us/visualstudio/ide/visual-studio-github-copilot-install-and-states?view=vs-2022)
- Any [JetBrains IDE](https://www.jetbrains.com/ides/) — [Copilot plugin](https://plugins.jetbrains.com/plugin/17718-github-copilot)
- [Xcode](https://developer.apple.com/xcode/) — [Copilot for Xcode](https://github.com/github/CopilotForXcode)
- [Eclipse](https://eclipseide.org/) — [GitHub Copilot plugin](https://marketplace.eclipse.org/content/github-copilot)

If you used VS Code, the [Deephaven VS Code extension](https://marketplace.visualstudio.com/items?itemName=deephaven.vscode-deephaven) let you run Copilot's suggestions directly from the IDE by connecting to a running Deephaven server. That extension is separate from the Copilot extension and is still available.

## Usage

While the extension was available, you started a message with `@deephaven` to tell Copilot you were asking a Deephaven-specific question. This worked in Copilot Chat in your IDE and in Copilot on [github.com](https://github.com/copilot). `@deephaven` no longer responds. Use [Deephaven MCP](./mcp.md) instead.

## Examples

The following examples show how the Deephaven GitHub Copilot extension responded to prompts while it was available. For the same kind of help now, use [Deephaven MCP](./mcp.md).

### Knowledge reference

The extension could explain core Deephaven concepts. For example, the following screenshot shows it explaining the difference between a [`DynamicTableWriter`](./table-publisher.md#dynamictablewriter) and a [table publisher](./table-publisher.md#table-publisher):

![Knowledge reference](../assets/how-to/copilot/knowledge_reference.png)

### Natural-language queries

The Deephaven Copilot extension could write entire queries from natural-language descriptions. For example, the following screenshot shows a request for code that creates a ticking table with sine and cosine waves. The response included the code and an explanation of what it does.

Always review generated code before you run it. For example, the plot code at the end of the following response calls `plot` on a `Figure`, but `Figure` has no `plot` method. The method that creates an XY series plot is [`plot_xy`](../reference/plot/plot.md), and for new plotting code we recommend [Deephaven Express](/core/plotly/docs/).

![Natural language query](../assets/how-to/copilot/nat-lang-query.png)

### Semantic search of documentation and code

The extension could summarize documentation or source code. For example, when asked to point to the documentation on [table iterators](./iterate-table-data.md), it returned both a link to the documentation and a summary:

![Semantic search](../assets/how-to/copilot/semantic.png)

### File context

The Deephaven Copilot extension could use your open files as context. You could ask it to review, generate, or critique code based on a file, the current selection, or the contents of the editor. In the following screenshot, Copilot explains the query in the open editor file:

![File context](../assets/how-to/copilot/filecontext.png)

## Related documentation

- [Deephaven MCP](./mcp.md)
- [Deephaven VS Code extension](https://marketplace.visualstudio.com/items?itemName=deephaven.vscode-deephaven)
