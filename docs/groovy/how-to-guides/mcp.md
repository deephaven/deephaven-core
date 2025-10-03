---
title: Deephaven MCP
sidebar_label: MCP
---

This guide walks you through the installation, setup, and use of Deephaven's MCP integration.

## What is MCP?

[Model Context Protocol (MCP)](https://modelcontextprotocol.io/docs/getting-started/intro) is an open-source standard for connecting AI applications to other services like Deephaven. People who use MCP do so to connect the AI models they use to things like databases, workflows, and filesystems to perform operations on them via natural language.

MCP connects AI applications to services like USB connects external devices to computers.

## What is Deephaven MCP?

Deephaven MCP implements the MCP standard to provide seamless integration between Deephaven and your AI development workflow. While AI models query tables, generate code, and do the busy work, you get to focus on turning that work into actionable insights.

Deephaven MCP has two components:

### Systems server

The systems server manages and connects to any pre-defined [Deephaven Community Core](/community/) and [Deephaven Enterprise](/enterprise/) systems. With the systems server, you can:

- List, monitor, and get detailed status of all configured Deephaven sessions
- Connect and manage Deephaven Enterprise Core+ deployments
- Create and destroy Enterprise Core+ sessions
- Retrieve schemas, metadata, and actual data from tables in sessions
- Execute both Python and Groovy scripts
- Query installed packages
- Dynamically reload and refresh session configurations

### Docs server

The docs server connects to Deephaven's documentation knowledge base so that AI models can query it for information about Deephaven. Ask questions in natural language to get specific answers with code examples and explanations with a high degree of confidence.

## Prerequisites

In order to use Deephaven MCP, you must have the following:

- Python 3.11 or later.
- Access to one or more Deephaven Community Core or Deephaven Enterprise Core+ systems.

## Installation

### Recommended (uv)

The recommended way to set up Deephaven MCP is through the Python [uv](https://pypi.org/project/uv/) package.

```bash
pip install uv
```

#### Create and activate a virtual environment

The following command creates a virtual environment in the `.venv` directory using Python 3.11. Replace `3.11` to your preferred version (such as `3.12`) if it differs, or the full path to a Python executable.

```bash
uv venv .venv -p 3.11

# On MacOS and Linux
source .venv/bin/activate

# On Windows
.venv\Scripts\activate
```

#### Install Deephaven MCP

```bash
# For Community Core only
uv pip install deephaven-mcp

# If you also plan to use Deephaven MCP for Enterprise Core+
uv pip install "deephaven-mcp[coreplus]"
```

### Alternative (pip)

You can also install Deephaven MCP with [pip](https://pypi.org/project/pip/).

#### Create and activate a virtual environment

Ensure that your Python version is `3.11` or later.

```bash
python -m venv .venv

# On MacOS and Linux
source .venv/bin/activate

# On Windows
.venv\Scripts\activate
```

#### Install Deephaven MCP

```bash
# For Community Core only
pip install deephaven-mcp

# If you also plan to use Deephaven MCP for Enterprise Core+
pip install "deephaven-mcp[coreplus]"
```

## Setup

There are two steps to setting up Deephaven MCP.

### Create a configuration file

> [!NOTE]
> It is recommended to set configuration file permissions to `600`, as they can contain sensitive information such as credentials.

A configuration file defines the Deephaven sessions that the Deephaven MCP [systems server](#systems-server) makes available to your AI tools. This file, written in JSON, should be called `deephaven_mcp.json`, and it can be placed wherever you like in your filesystem.

The following example configuration file is configured for a single Deephaven Community Core server running locally on port `10000` and using [pre-shared key authentication](./authentication/auth-psk.md) with the pre-shared key `YOUR_PASSWORD_HERE`:

```json
{
  "community": {
    "sessions": {
      "my_groovy_session": {
        "host": "localhost",
        "port": 10000,
        "session_type": "groovy",
        "auth_type": "io.deephaven.authentication.psk.PskAuthenticationHandler",
        "auth_token": "YOUR_PASSWORD_HERE"
      }
    }
  }
}
```

The following example configuration file is configured for two Deephaven Community Core servers. The first is the same as the previous example, whereas the second is a Python session running externally on a different port with anonymous authentication:

```json
{
  "community": {
    "sessions": {
      "my_local_groovy_session": {
        "host": "localhost",
        "port": 10000,
        "session_type": "groovy",
        "auth_type": "io.deephaven.authentication.psk.PskAuthenticationHandler",
        "auth_token": "YOUR_PASSWORD_HERE"
      },
      "external_anonymous_session": {
        "host": "my.external.host.com",
        "port": 10001,
        "session_type": "python"
      }
    }
  }
}
```

This file can contain any number of Deephaven Community Core and Deephaven Enterprise Core+ sessions. For a full list of configuration parameters for each session, see the [Appendix](#systems-server-configuration-parameters).

### Configure your AI tool

Your AI tool configuration should look like this:

```json
{
  "mcpServers": {
    "deephaven-systems": {
      "command": "/full/path/to/your/.venv/bin/dh-mcp-systems-server",
      "args": [],
      "env": {
        "DH_MCP_CONFIG_FILE": "/full/path/to/your/deephaven_mcp.json",
        "PYTHONLOGLEVEL": "INFO"
      }
    },
    "deephaven-docs": {
      "command": "/full/path/to/your/.venv/bin/mcp-proxy",
      "args": [
        "--transport=streamablehttp",
        "https://deephaven-mcp-docs-prod.dhc-demo.deephaven.io/mcp"
      ]
    }
  }
}
```

Where `/full/path/to/your/.venv/` is the full filepath to the Python virtual environment [you created earlier](#installation).

Depending on which AI tool you use, your configuration steps vary slightly. For more on each AI tool, see the [Appendix](#ai-tool-configuration).

## Use Deephaven MCP

If you just [configured](#configure-your-ai-tool) (or changed the configuration of) your AI tool for MCP, you must restart it for the configuration changes to take effect.

Test the connection by asking your AI tool:

```text
Are the Deephaven MCP servers working? Can you list available sessions?
```

If that works, you're all set! Try Deephaven MCP for:

- Listing available sessions
- Listing tables available across sessions
- Getting details about environments in sessions
- Writing queries
- Analyzing data
- Interacting with Deephaven documentation for customized help
- Executing Python and/or Groovy scripts across multiple sessions
- Analyzing table schemas
- Monitoring session health, package versions, and more

## Appendix

### Systems server configuration parameters

The following parameters are available for the systems server:

| Field                | Type    | Required When | Description                                                                                                          |
| -------------------- | ------- | ------------- | -------------------------------------------------------------------------------------------------------------------- |
| `host`               | string  | Optional      | Hostname or IP address of the Deephaven Community Core session (e.g., `"localhost"`)                                 |
| `port`               | integer | Optional      | Port number for the session connection (e.g., `10000`)                                                               |
| `auth_type`          | string  | Optional      | Authentication type: `"Anonymous"` (default), `"Basic"`, or custom authenticator strings                             |
| `auth_token`         | string  | Optional      | Authentication token. For `"Basic"` auth: `"username:password"` format. Mutually exclusive with `auth_token_env_var` |
| `auth_token_env_var` | string  | Optional      | Environment variable name containing the auth token (e.g., `"MY_AUTH_TOKEN"`). More secure than hardcoding tokens    |
| `never_timeout`      | boolean | Optional      | If `true`, attempts to configure the session to never time out                                                       |
| `session_type`       | string  | Optional      | Type of session to create: `"groovy"` or `"python"`                                                                  |
| `use_tls`            | boolean | Optional      | Set to `true` if the connection requires TLS/SSL                                                                     |
| `tls_root_certs`     | string  | Optional      | Absolute path to PEM file with trusted root CA certificates for TLS verification                                     |
| `client_cert_chain`  | string  | Optional      | Absolute path to PEM file with client's TLS certificate chain (for mTLS)                                             |
| `client_private_key` | string  | Optional      | Absolute path to PEM file with client's private key (for mTLS)                                                       |

### AI tool configuration

The process for configuring your AI tool depends on which you use. This section covers several of the most popular ones.

#### Claude Desktop

Open **Claude Desktop -> Settings -> Developer -> Edit Config**. It should open a JSON file, to which you can add the [configuration](#configure-your-ai-tool) above.

#### Cursor

For project-specific configuration, edit **`.cursor/mcp.json`** in the project root. For global configuration, edit **`~/.cursor/mcp.json`**. Add the [configuration](#configure-your-ai-tool) above.

#### Visual Studio Code

Run the **MCP: Add Server** command from the Command Pallete. Then select **Workspace Settings** to create the `.vscode/mcp.json` file. Alternatively, you can create this file manually in your project root. Add the [configuration](#configure-your-ai-tool) above.

#### Windsurf

Go to **Windsurf -> Settings -> Windsurf Settings -> Cascade -> MCP Servers -> Manage MCPs -> View Raw Config**, which opens `~/.codeium/windsurf/mcp_config.json`. Add the [configuration](#configure-your-ai-tool) above.

### Troubleshooting

This section contains the most commonly observed issues and solutions.

#### spawn uv ENOENT

This error appears in your AI tool's logs, and often as a popup in its UI.

If your AI tool fails with this error, check your [AI tool configuration](#configure-your-ai-tool) and make sure it uses the full path to the venv, and that the path is correct.

#### Connection failed

This error appears in the MCP server logs. It most commonly happens when connecting to an external Deephaven server. Check your internet connection, server URLs, firewall (if applicable), and VPN (if applicable).

#### Config not found

This happens when the MCP server starts up if it cannot find the [`deephaven_mcp.json`](#create-a-configuration-file) file. Verify the full path to the file.

#### Permission denied

This happens during command execution if the MCP server does not have permission to access the file or directory. Ensure your Python installation, as well as packages like uv, have the proper permissions.

#### Python version error

This happens if you are using an unsupported Python version, such as `3.10` or earlier.

#### JSON parse error

This error appears in youR AI tool's logs. It indicates a syntax error in your one of your configuration files.

#### Module not found: deephaven_mcp

This error appears in the MCP server logs, and it happens if the virtual environment is not activated with the proper dependencies installed.

#### Port already in use

This error appears in the server startup logs. It happens when the `PORT` environment variable conflicts with another port in use, or if a conflicting process uses a Deephaven MCP port.

#### Invalid session_id format

This is an MCP tool response to an invalid format for `session_id`. The correct format is `{type}:{source}:{session_name}`.

## Related documentation

<!-- TODO: What else goes here? -->

- [Copilot plugin](./copilot-extension.md)
