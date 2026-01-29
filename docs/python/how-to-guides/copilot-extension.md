---
title: Deephaven's GitHub Copilot extension
sidebar_label: Copilot extension
---

Deephaven's [GitHub Copilot extension](https://github.com/apps/deephaven) gives users direct access to an AI model trained specifically on Deephaven's documentation and codebase. You can leverage the extension for things like:

- Auto-completion for Deephaven-specific code snippets.
- Writing Deephaven queries using natural language.
- Proofreading, refactoring, and suggesting improvements to your Deephaven code.
- Reviewing, critiquing, and providing feedback on your queries.
- Answering questions about Deephaven's API and features.
- And more!

## Prerequisites

There are only two prerequisites to use the Deephaven GitHub Copilot extension:

- [GitHub Copilot](https://github.com/features/copilot) - You'll need a GitHub account to use Copilot. Any tier of GitHub Copilot will work, from the free tier all the way to the enterprise tier.
- Deephaven - [Community Core](/community) or [Enterprise](/enterprise).

## Recommendations

On top of the [prerequisites](#prerequisites), it's recommended to install one or more of the IDEs supported by GitHub Copilot. They include:

- [Microsoft Visual Studio Code (VS Code)](https://code.visualstudio.com/)
- [Microsoft Visual Studio](https://visualstudio.microsoft.com/)
- Any [JetBrains IDE](https://www.jetbrains.com/ides/)
- [XCode](https://developer.apple.com/xcode/)
- [Neovim](https://neovim.io/)
- [Azure Data Studio](https://azure.microsoft.com/en-us/products/data-studio)
- [Eclipse](https://eclipseide.org/)

If you use Visual Studio Code, it's also recommended you install the [Deephaven Extension](https://marketplace.visualstudio.com/items?itemName=deephaven.vscode-deephaven), as it allows you to connect to running Deephaven servers to execute queries from the IDE directly. This makes using and testing Copilot's suggestions simpler and easier.

## Installation

Installation is simple - follow these steps to get started:

1. Install GitHub Copilot

Head to [github.com/features/copilot](https://github.com/features/copilot) and get started with the plan of your choice - Free, Pro, Business, or Enterprise. If you already have this installed, you can skip this step.

2. Install Deephaven

If you do not yet have Deephaven installed, there are several ways to get started.

For Python:

- [Docker](../getting-started/docker-install.md)
- [pip](../getting-started/pip-install.md)
- [Build from source](../getting-started/launch-build.md)
- [Production application](../getting-started/production-application.md)

For Java/Groovy:

- [Docker](https://deephaven.io/core/groovy/docs/getting-started/docker-install)
- [Build from source](https://deephaven.io/core/groovy/docs/getting-started/launch-build)
- [Production application](https://deephaven.io/core/groovy/docs/getting-started/production-application)

3. Install the Deephaven GitHub Copilot extension

Head to [github.com/apps/deephaven](https://github.com/apps/deephaven), and click the big green **Install** button. Follow the instructions to install the extension.

4. Install any recommended IDEs you wish to use

See the [Recommendations](#recommendations) section above for a list of IDEs that support GitHub Copilot. Follow the instructions provided for whichever IDE(s) you choose to install.

5. Install GitHub Copilot in your IDE

Copilot is a plugin for any of the IDEs listed [above](#recommendations). See the following links for the Copilot plugin in each:

- [Visual Studio Code](https://code.visualstudio.com/docs/copilot/setup)
- [Visual Studio](https://learn.microsoft.com/en-us/visualstudio/ide/visual-studio-github-copilot-install-and-states?view=vs-2022)
- [JetBrains IDEs](https://plugins.jetbrains.com/plugin/17718-github-copilot)
- [XCode](https://github.com/github/CopilotForXcode)
- [Neovim](https://github.com/github/copilot.vim)
- [Azure Data Studio](https://learn.microsoft.com/en-us/azure-data-studio/extensions/github-copilot-extension-overview)
- [Eclipse](https://marketplace.eclipse.org/content/copilot4eclipse)

Once these steps are completed, you're ready to go!

## Usage

When querying the Deephaven GitHub Copilot extension, preface your queries with `@deephaven` to tell it you're asking a Deephaven-specific question.

## Examples

The following examples show how to use the Deephaven GitHub Copilot extension effectively.

### Knowledge reference

Copilot is a great tool for learning about core concepts. For example, ask Deephaven about the difference between a [Dynamic Table Writer](./table-publisher.md#dynamictablewriter) and a [Table Publisher](./table-publisher.md#table-publisher):

![Knowledge reference](../assets/how-to/copilot/knowledge_reference.png)

### Natural language queries

Have it write entire queries for you based on natural language descriptions. For example, ask it to write code that creates a ticking table with sine and cosine waves. Not only do you get the code, you also get an explanation and plot code to show that it works as intended:

![Natural language query](../assets/how-to/copilot/nat-lang-query.png)

### Semantic search of documentation and code

Want a succinct summary of documentation and/or source code? Just ask! For example, ask it to point you to the documentation on a [Table iterator](./iterate-table-data.md). Not only do you get the link to the documentation, it also provides a summary:

![Semantic search](../assets/how-to/copilot/semantic.png)

### File context

The tool has file context, meaning you can ask it to review, generate, or critique code based on file input, the current selection, what's in the editor, and more. For example, here's Copilot explaining this query:

![File context](../assets/how-to/copilot/filecontext.png)

## Troubleshooting

You may find that the Deephaven GitHub Copilot extension does not work as expected. If you type `@`, and `@deephaven` does not appear as an option, it means that the extension is not registered in your chat. There are some common reasons for this:

- You are not logged into GitHub in your IDE. IDEs may log you out of GitHub after a period of inactivity or if you restart your IDE. Make sure you are logged in.
- The Deephaven extension is not installed. Ensure you've installed the extension by following the instructions presented in this guide.
- Use of another extension in a chat may prevent the Deephaven extension from appearing as an option. Try starting a new chat and typing `@` to see if the Deephaven extension appears.
- If using Copilot from GitHub's website, you may have to type `@deephaven` and ask a question before the extension appears.
- If using an IDE, you may need to restart it for the extension to appear.

## Related documentation

- [Run with Docker](../getting-started/docker-install.md)
- [Run with pip](../getting-started/pip-install.md)
- [Build from source](../getting-started/launch-build.md)
- [Build from the production application](../getting-started/production-application.md)
