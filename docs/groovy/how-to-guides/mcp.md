---
title: Deephaven MCP
sidebar_label: MCP
---

This guide walks you through the installation, setup, and use of Deephaven's MCP integration.

## What is MCP?

[Model Context Protocol (MCP)](https://modelcontextprotocol.io/docs/getting-started/intro) is an open-source standard for connecting AI applications to other services like Deephaven. People who use MCP do so to connect the AI models they use to things like databases, workflows, and filesystems to perform operations on them via natural language.

MCP connects AI applications to services like USB connects external devices to computers.

## Why use Deephaven MCP?

Deephaven MCP implements the MCP standard to provide seamless integration between Deephaven Community Core and your AI development workflow. While AI models query tables, generate code, and do the busy work, you get to focus on turning that work into actionable insights.

## Install Deephaven MCP

Installing Deephaven MCP is as simple as any other Python package. Install it with [pip](https://pypi.org/project/pip/) from a [virtual environment](https://docs.python.org/3/library/venv.html).

```bash
python -m venv .venv

# On MacOS and Linux
source .venv/bin/activate

# On Windows
.venv\Scripts\activate
```

```bash
pip install deephaven-mcp
```

## Set up Deephaven MCP

There are two steps to setting up Deephaven MCP.

### Create a configuration file

> [!NOTE]
> It is recommended to set configuration file permissions to `600`, as they can contain sensitive information such as credentials.

Your AI tool will know how to communicate with Deephaven through a configuration file. This file, written in JSON, should be called `deephaven_mcp.json`, and it can be placed wherever you like in your filesystem.

The following example configuration file is configured for a Deephaven server running locally on port `10000` and using [pre-shared key authentication](./authentication/auth-psk.md) with the pre-shared key `YOUR_PASSWORD_HERE`:

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

### Configuration parameters

In the `sessions` block of your configuration file, you'll define each Deephaven Community session you want to give your AI tools access to. For each session, you can specify the following parameters:

#### `host`

The `host` is the hostname or IP address where the Deephaven server is running. The [example](#create-a-configuration-file) above uses `localhost` since the Deephaven server is running locally.

#### `port`

The `port` is the port number the Deephaven server is running on.

#### `session_type`

The `session_type` is the server-side API language the server is using. Valid options are `python` and `groovy`.

#### `auth_type`

The `auth_type` is the authentication protocol the server is using. For [anonymous authentication](./authentication/auth-anon.md), you need not provide an `auth_type`. For any other authentication protocol, provide the fully qualified class name of the authentication protocol.

#### `auth_token`

The `auth_token` is the pre-shared key in plaintext if using [pre-shared key](./authentication/auth-psk.md) authentication.

#### `auth_token_env_var`

The `auth_token_env_var` is the name of an environment variable that contains your pre-shared key for [pre-shared key](./authentication/auth-psk.md) authentication.

#### `use_tls`

The `use_tls` parameter is a boolean that indicates whether to use TLS to connect to the Deephaven server. If not using TLS, you need not provide this parameter.

#### `tls_root_certs`

The `tls_root_certs` parameter is the path to the root certificate(s) to use when connecting to the Deephaven server over TLS.

#### `client_cert_chain`

The `client_cert_chain` parameter is the path to the client certificate(s) to use when connecting to the Deephaven server over TLS.

#### `client_private_key`

The `client_private_key` parameter is the path to the private key to use when connecting to the Deephaven server over TLS.

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

Where `/full/path/to/your/.venv/` is the full filepath to the virtual environment directory

Depending on which AI tool you use, your configuration steps vary slightly. This section covers each.

#### Claude Desktop

Open **Claude Desktop -> Settings -> Developer -> Edit Config**. It should open a JSON file, to which you can add the [configuration](#configure-your-ai-tool) above.

#### Cursor

For project-specific configuration, edit **`.cursor/mcp.json`** in the project root. For global configuration, edit **`~/.cursor/mcp.json`**. Add the [configuration](#configure-your-ai-tool) above.

#### Visual Studio Code

Run the **MCP: Add Server** command from the Command Pallete. Then select **Workspace Settings** to create the `.vscode/mcp.json` file. Alternatively, you can create this file manually in your project root. Add the [configuration](#configure-your-ai-tool) above.

#### Windsurf

Go to **Windsurf -> Settings -> Windsurf Settings -> Cascade -> MCP Servers -> Manage MCPs -> View Raw Config**, which opens `~/.codeium/windsurf/mcp_config.json`. Add the [configuration](#configure-your-ai-tool) above.

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

## Related documentation

<!-- TODO: Fill this out once I flesh this doc out a bit. -->
