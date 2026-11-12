---
title: Create your own plugin
---

Python plugins are Python packages that extend the server's functionality, the UI, a client API, or a combination of these. They can be as simple as a single Python file or as complex as a multi-file package with dependencies. Plugins are packaged like any other Python package.

This guide walks you through creating a plugin in Deephaven. Specifically, it covers creating a bidirectional plugin that extends the capabilities of Deephaven's [Python client API](/core/client-api/python). A bidirectional plugin keeps a message stream open between the client and an object on the server, so the two sides can keep exchanging messages. This guide uses that stream to define custom remote procedure call (RPC) methods that clients call to work with an object on a running server and get objects back.

A bidirectional plugin like the one in this guide has a server-side component, implemented with the [`deephaven.plugin`](/core/pydoc/code/deephaven.plugin.html) module, and a client-side component, implemented with the [`pydeephaven.experimental.plugin_client`](/core/client-api/python/code/pydeephaven.experimental.plugin_client.html) module.

Not every plugin needs a message stream. A fetch-only plugin sends its object to the client once, and the client can't send messages back. To build one in Python, subclass [`FetchOnlyObjectType`](/core/pydoc/code/deephaven.plugin.object_type.html#deephaven.plugin.object_type.FetchOnlyObjectType) and implement its [`to_bytes`](/core/pydoc/code/deephaven.plugin.object_type.html#deephaven.plugin.object_type.FetchOnlyObjectType.to_bytes) method instead. The [Groovy version of this guide](/core/groovy/docs/how-to-guides/create-plugins/) walks through a fetch-only plugin in Java or Groovy.

## Create the plugin

### Project layout

This example creates a plugin called `ExampleService`. You can call your plugin whatever you'd like.

> [!NOTE]
> The directory name does not need to match the plugin name. This guide uses `ExampleServicePlugin` for the directory name, but the plugin itself is called `ExampleService`.

The plugin has server- and client-side components. Create the following directory and file structure in your project folder:

```plaintext
ExampleServicePlugin/
├── server/
│   ├── pyproject.toml
│   └── example_plugin_server/
│       └── __init__.py
└── client/
    ├── pyproject.toml
    └── example_plugin_client/
        └── __init__.py
```

> [!NOTE]
> This guide assumes the use of the [`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) configuration for packaging. You can also use `setup.cfg` or `setup.py` if you prefer.

### Server-side plugin

All server-side plugin code lives in the `server` folder. This folder contains a TOML file for packaging and a subfolder containing the actual plugin implementation.

#### Server plugin implementation

This example plugin has four parts, all of which go in `server/example_plugin_server/__init__.py`:

- The object the plugin manages (`ExampleService`).
- The message stream handler, which passes messages between the client and the server (`ExampleServiceMessageStream`).
- The object type, which tells the server how to recognize and connect to the object (`ExampleServicePlugin`).
- The registration class, which registers the object type with the server (`ExampleServicePluginRegistration`).

Let's start with the object the plugin manages. In this case, the service has two methods.

```python skip-test
from deephaven.table import Table


class ExampleService:
    """Example service that echoes strings and tables."""

    def hello_string(self, data: str) -> str:
        """Returns a string containing the input data."""
        return f"Hello client. You said: {data}"

    def hello_table(self, table: Table, data: str) -> Table:
        """Returns a table generated from the input table and the input data."""
        return table.update(["Client = data", "Server = `Hello client!`"])
```

Next, define the message stream handler, a subclass of [`MessageStream`](/core/pydoc/code/deephaven.plugin.object_type.html#deephaven.plugin.object_type.MessageStream) that passes messages between the client and the server.

```python skip-test
from deephaven.plugin.object_type import MessageStream
from typing import List, Any
import traceback
import json


class ExampleServiceMessageStream(MessageStream):
    """
    MessageStream implementation for ExampleService.
    The server calls its on_data method when the client sends a message.
    """

    def __init__(self, service: ExampleService, client_connection: MessageStream):
        self.service = service
        self.client_connection = client_connection

        # The server must send an initial message; send an empty payload to acknowledge the connection
        self.client_connection.on_data(b"", [])

    def on_data(self, payload: bytes, references: List[Any]):
        """Called when the client sends a message to the server."""

        # Deserialize the input JSON bytes
        input_string = bytes(payload).decode("utf-8")
        print(f"Received data from client: {input_string}")
        inputs = json.loads(input_string)

        # Initialize the result payload and references to Deephaven objects
        result_payload = {}
        result_references = []

        try:
            if inputs["method"] == "hello_string":
                print(f'Calling hello_string("{inputs["data"]}")')
                result_payload["result"] = self.service.hello_string(inputs["data"])
            elif inputs["method"] == "hello_table":
                print(f'Calling hello_table({references[0]}, "{inputs["data"]}")')
                result_payload["result"] = ""
                result_references = [
                    self.service.hello_table(references[0], inputs["data"])
                ]
            else:
                print(f"Unknown message type: {inputs['method']}")
                raise NotImplementedError(f"Unknown message type: {inputs['method']}")
        except Exception:
            result_payload["error"] = traceback.format_exc()
            print(f"Error processing message: {result_payload['error']}")

        print(f"Sending result to client: {result_payload}")

        # Serialize the result payload to JSON bytes
        json_string = json.dumps(result_payload).encode("utf-8")
        self.client_connection.on_data(
            payload=json_string, references=result_references
        )

    def on_close(self):
        """Called when the client closes the connection."""
        print("Client connection closed.")
```

Then define the object type, a subclass of [`BidirectionalObjectType`](/core/pydoc/code/deephaven.plugin.object_type.html#deephaven.plugin.object_type.BidirectionalObjectType) that ties the object to its message stream. It implements:

- The [`name`](/core/pydoc/code/deephaven.plugin.object_type.html#deephaven.plugin.object_type.ObjectType.name) property, which returns the type name, `ExampleService`.
- The [`is_type`](/core/pydoc/code/deephaven.plugin.object_type.html#deephaven.plugin.object_type.ObjectType.is_type) method, which tells the server whether an object is an `ExampleService`.
- The [`create_client_connection`](/core/pydoc/code/deephaven.plugin.object_type.html#deephaven.plugin.object_type.BidirectionalObjectType.create_client_connection) method, which returns a new `ExampleServiceMessageStream` each time a client connects to an `ExampleService`.

```python skip-test
from deephaven.plugin.object_type import MessageStream, BidirectionalObjectType


class ExampleServicePlugin(BidirectionalObjectType):
    """Plugin for ExampleService."""

    @property
    def name(self) -> str:
        """Get the name of the service."""
        return "ExampleService"

    def is_type(self, object) -> bool:
        """Check if an object is an ExampleService."""
        return isinstance(object, ExampleService)

    def create_client_connection(
        self, obj: ExampleService, connection: MessageStream
    ) -> MessageStream:
        """Create a connection to an ExampleService instance."""
        return ExampleServiceMessageStream(obj, connection)
```

Finally, add a registration class, a subclass of [`Registration`](/core/pydoc/code/deephaven.plugin.html#deephaven.plugin.Registration), to the same file. The registration code is much simpler than the implementation. The server calls its [`register_into`](/core/pydoc/code/deephaven.plugin.html#deephaven.plugin.Registration.register_into) method at startup to register `ExampleServicePlugin`:

```python skip-test
from deephaven.plugin import Registration, Callback


class ExampleServicePluginRegistration(Registration):
    """Registration for ExampleServicePlugin."""

    @classmethod
    def register_into(cls, callback: Callback) -> None:
        """Register the ExampleServicePlugin."""
        callback.register(ExampleServicePlugin)
```

This example handles one class of object, `ExampleService`, and any number of its instances. To support more classes, do one of the following:

- Check for each class in [`is_type`](/core/pydoc/code/deephaven.plugin.object_type.html#deephaven.plugin.object_type.ObjectType.is_type) and [`create_client_connection`](/core/pydoc/code/deephaven.plugin.object_type.html#deephaven.plugin.object_type.BidirectionalObjectType.create_client_connection).
- Define a separate object type for each class, and register each one in [`register_into`](/core/pydoc/code/deephaven.plugin.html#deephaven.plugin.Registration.register_into) with its own [`callback.register`](/core/pydoc/code/deephaven.plugin.html#deephaven.plugin.Callback.register) call.

#### Complete server module

Altogether, the `__init__.py` should look like this:

<details>
<summary>__init__.py</summary>

```python skip-test
"""
This module provides a server-side plugin for accessing an ExampleService object.
"""

from deephaven.plugin.object_type import BidirectionalObjectType, MessageStream
from deephaven.plugin import Registration, Callback
from typing import List, Any
from deephaven.table import Table
import traceback
import json


class ExampleService:
    """Example service that echoes strings and tables."""

    def hello_string(self, data: str) -> str:
        """Returns a string containing the input data."""
        return f"Hello client. You said: {data}"

    def hello_table(self, table: Table, data: str) -> Table:
        """Returns a table generated from the input table and the input data."""
        return table.update(["Client = data", "Server = `Hello client!`"])


class ExampleServiceMessageStream(MessageStream):
    """
    MessageStream implementation for ExampleService.
    The server calls its on_data method when the client sends a message.
    """

    def __init__(self, service: ExampleService, client_connection: MessageStream):
        self.service = service
        self.client_connection = client_connection

        # The server must send an initial message; send an empty payload to acknowledge the connection
        self.client_connection.on_data(b"", [])

    def on_data(self, payload: bytes, references: List[Any]):
        """Called when the client sends a message to the server."""

        # Deserialize the input JSON bytes
        input_string = bytes(payload).decode("utf-8")
        print(f"Received data from client: {input_string}")
        inputs = json.loads(input_string)

        # Initialize the result payload and references to Deephaven objects
        result_payload = {}
        result_references = []

        try:
            if inputs["method"] == "hello_string":
                print(f'Calling hello_string("{inputs["data"]}")')
                result_payload["result"] = self.service.hello_string(inputs["data"])
            elif inputs["method"] == "hello_table":
                print(f'Calling hello_table({references[0]}, "{inputs["data"]}")')
                result_payload["result"] = ""
                result_references = [
                    self.service.hello_table(references[0], inputs["data"])
                ]
            else:
                print(f"Unknown message type: {inputs['method']}")
                raise NotImplementedError(f"Unknown message type: {inputs['method']}")
        except Exception:
            result_payload["error"] = traceback.format_exc()
            print(f"Error processing message: {result_payload['error']}")

        print(f"Sending result to client: {result_payload}")

        # Serialize the result payload to JSON bytes
        json_string = json.dumps(result_payload).encode("utf-8")
        self.client_connection.on_data(
            payload=json_string, references=result_references
        )

    def on_close(self):
        """Called when the client closes the connection."""
        print("Client connection closed.")


class ExampleServicePlugin(BidirectionalObjectType):
    """Plugin for ExampleService."""

    @property
    def name(self) -> str:
        """Get the name of the service."""
        return "ExampleService"

    def is_type(self, object) -> bool:
        """Check if an object is an ExampleService."""
        return isinstance(object, ExampleService)

    def create_client_connection(
        self, obj: ExampleService, connection: MessageStream
    ) -> MessageStream:
        """Create a connection to an ExampleService instance."""
        return ExampleServiceMessageStream(obj, connection)


class ExampleServicePluginRegistration(Registration):
    """Registration for ExampleServicePlugin."""

    @classmethod
    def register_into(cls, callback: Callback) -> None:
        """Register the ExampleServicePlugin."""
        callback.register(ExampleServicePlugin)
```

</details>

#### Package the server plugin

Package the server-side code with `server/pyproject.toml`:

```toml
[build-system]
requires = ["setuptools"]
build-backend = "setuptools.build_meta"

[project]
name = "example_plugin_server"
version = "0.0.1"
dependencies = ["deephaven-plugin>=0.6.0", "deephaven-core>=42.7"]

[project.entry-points."deephaven.plugin"]
registration_cls = "example_plugin_server:ExampleServicePluginRegistration"
```

> [!NOTE]
> The server-side `pyproject.toml` file must declare an entry point in the `deephaven.plugin` group named `registration_cls` that points to the registration class. The server ignores entry points with any other name.

You can test the server side before you write the client: see [Use the plugin](#use-the-plugin).

### Client-side plugin

The client-side code and packaging live in the `client` folder of the project.

#### Client plugin implementation

As with the server-side code, the client-side code is in a single file, `client/example_plugin_client/__init__.py`. It defines `ExampleServiceProxy`, which wraps a [`PluginClient`](/core/client-api/python/code/pydeephaven.experimental.plugin_client.html#pydeephaven.experimental.plugin_client.PluginClient) and gives the client the same `hello_string` and `hello_table` methods as the server-side object. Each method writes a request to the plugin client's `req_stream` and reads the reply from its `resp_stream`.

[Client-side testing](#client-side-testing) shows how to get a [`PluginClient`](/core/client-api/python/code/pydeephaven.experimental.plugin_client.html#pydeephaven.experimental.plugin_client.PluginClient) from [`Session.plugin_client`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.plugin_client).

The two sides agree on a small protocol:

- The client sends JSON with `method` and `data` keys. Tables travel separately from the JSON payload, as references. A reference is a handle to an object on the server.
- The server replies with JSON containing `result` or `error`, plus any result tables as references.
- When the connection opens, the server must send an initial message. This example sends an empty payload, which the proxy reads and discards in its constructor.

```python skip-test
from pydeephaven import Table
from pydeephaven.experimental import plugin_client
from pydeephaven.ticket import ServerObject
import json


class ExampleServiceProxy(ServerObject):
    """
    This class provides a client-side interface to the ExampleService server-side object.

    When you call a method on this class, it sends a message to the server-side object
    and returns the result. The inputs are serialized to JSON bytes and sent to the
    server, and the result is deserialized from JSON and returned.
    """

    def __init__(self, plugin_client: plugin_client.PluginClient):
        super().__init__(type=plugin_client.type, ticket=plugin_client.ticket)

        self.plugin_client = plugin_client

        # Consume the first (empty) payload from the server to acknowledge successful connection
        next(self.plugin_client.resp_stream)

    def hello_string(self, data: str) -> str:
        """Call ExampleService.hello_string() on the server.
        Returns a string containing the input data."""

        inputs = {"method": "hello_string", "data": data}

        # serialize the inputs to JSON bytes
        input_bytes = json.dumps(inputs).encode("utf-8")

        # no input references
        input_references = []

        self.plugin_client.req_stream.write(input_bytes, input_references)
        result_bytes, result_references = next(self.plugin_client.resp_stream)

        # Deserialize the results from JSON bytes
        results = json.loads(result_bytes.decode("utf-8"))

        if "error" in results:
            raise Exception(results["error"])

        # return the result string
        return results["result"]

    def hello_table(self, table: Table, data: str) -> Table:
        """Call ExampleService.hello_table() on the server.
        Returns a table generated from the input table and the input data."""

        inputs = {"method": "hello_table", "data": data}

        # serialize the inputs to JSON bytes
        input_bytes = json.dumps(inputs).encode("utf-8")

        # input references
        input_references = [table]

        self.plugin_client.req_stream.write(input_bytes, input_references)
        result_bytes, result_references = next(self.plugin_client.resp_stream)

        # Deserialize the results from JSON bytes
        results = json.loads(result_bytes.decode("utf-8"))

        if "error" in results:
            raise Exception(results["error"])

        # fetch and return the result table
        return result_references[0].fetch()
```

#### Package the client plugin

As with the server-side plugin, you package the client-side plugin with a TOML file, `client/pyproject.toml`:

```toml
[build-system]
requires = ["setuptools"]
build-backend = "setuptools.build_meta"

[project]
name = "example_plugin_client"
version = "0.0.1"
dependencies = ["pydeephaven>=42.7", "pandas"]
```

> [!NOTE]
> The client-side plugin for this example requires no registration.

## Use the plugin

Once you have written and packaged the server-side and client-side code, you can test the plugin. Install the server-side package, then follow [Server-side testing](#server-side-testing) to start a server that exposes an `ExampleService` and [Client-side testing](#client-side-testing) to call it from a client.

### Installation

Install the server-side package along with Deephaven, using the method that matches how you run the server.

#### Install with pip

If you start the server with pip-installed Deephaven, install Deephaven and the plugin in a virtual environment. The server also needs Java 17 or later, with `JAVA_HOME` set. See [Install and run Deephaven with pip](../getting-started/pip-install.md) for details. The following commands create a virtual environment and install the necessary packages, including the plugin.

```bash
# Run these from the root folder of the project
rm -rf ./venv-server
python3.12 -m venv ./venv-server
source ./venv-server/bin/activate
python -m pip install -U pip
python -m pip install -U deephaven-server
python -m pip install ./server
```

#### Install with Docker

If you start the server with Docker, use Docker Compose to manage the server and any other services. Save the following Dockerfile in the root folder of the project, `ExampleServicePlugin/`. It builds an image from Deephaven's server image and installs the plugin:

```Dockerfile
FROM ghcr.io/deephaven/server:latest

COPY ./server /server

RUN pip install -U pip
RUN pip install /server
```

> [!IMPORTANT]
> The Docker Compose file below sets the [pre-shared key](./authentication/auth-psk.md) to `YOUR_PASSWORD_HERE` for demonstration purposes. The client needs this key when connecting to the server. Change this to a more secure key in production.

Save the following Docker Compose file in the same folder. It runs Deephaven using the image built by the above Dockerfile:

```yaml
services:
  deephaven:
    build: .
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -Dauthentication.psk=YOUR_PASSWORD_HERE
```

With those two files in hand, `docker compose up --build` builds the image with the plugin installed and starts the server. Use `--build` again whenever you change the plugin code.

### Server-side testing

To test the server side, create an `ExampleService` object on the server so a client can reach it. Follow the subsection that matches how you installed the plugin.

#### Test a pip-installed server

If you installed Deephaven with pip, save the following script in the project root and run it with `python` while `venv-server` is active. The script starts a Deephaven server, creates an instance of `ExampleService`, and keeps the server running until you press Ctrl+C.

The script imports the plugin package only after the server starts. The package imports `deephaven` modules, and Python can't import those until the server is running.

The `-Dauthentication.psk` JVM argument sets the [pre-shared key](./authentication/auth-psk.md) that clients use to connect. Outside of testing, replace `YOUR_PASSWORD_HERE` with a secure key:

```python skip-test
import sys
from deephaven_server import Server

# Start the Deephaven server
server = Server(port=10000, jvm_args=["-Dauthentication.psk=YOUR_PASSWORD_HERE"])
server.start()

# Import the plugin package only after the server starts
from example_plugin_server import ExampleService

example_service = ExampleService()

# Keep the server running until the user presses Ctrl+C
print("Press Ctrl+C to exit")

try:
    while True:
        input()
except KeyboardInterrupt:
    print("Exiting Deephaven...")
    sys.exit(0)
```

#### Test a Docker server

If you run Deephaven in Docker, create an instance of `ExampleService` so that a client can interact with it. Open the Deephaven IDE at `http://localhost:10000/ide/`, log in with the pre-shared key from the Docker Compose file, and run the following code:

```python skip-test
from example_plugin_server import ExampleService

example_service = ExampleService()
```

### Client-side testing

To test the client-side plugin, run the following commands from the root of the project to create a client virtual environment, separate from the server, and install the client package:

```bash
rm -rf ./venv-client
python3.12 -m venv ./venv-client
source ./venv-client/bin/activate
python -m pip install -U pip
python -m pip install ./client
```

> [!IMPORTANT]
> Replace `YOUR_PASSWORD_HERE` with the pre-shared key you set when starting the server, either in the Docker Compose file or in the `jvm_args` passed to `Server`.

While the server from [Server-side testing](#server-side-testing) is running, save the following script in the project root and run it with `python` while `venv-client` is active. The script uses the client-side plugin in three steps:

1. It looks up the server's `example_service` variable in the [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session) object's `exportable_objects` property. This property maps each exportable variable name to a [`ServerObject`](/core/client-api/python/code/pydeephaven.ticket.html#pydeephaven.ticket.ServerObject), which holds the object's type and a ticket. A ticket is a client's reference to an object on the server.
2. It wraps the [`ServerObject`](/core/client-api/python/code/pydeephaven.ticket.html#pydeephaven.ticket.ServerObject) in a plugin client with [`Session.plugin_client`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.plugin_client).
3. It passes the plugin client to `ExampleServiceProxy` and calls the proxy's two methods.

```python skip-test
"""
Example client script that connects to a Deephaven server
and calls methods on a server-side ExampleService object.
"""

from pydeephaven import Session
from example_plugin_client import ExampleServiceProxy

session = Session(
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="YOUR_PASSWORD_HERE",  # Replace this with your PSK
)

# Print the objects the server can export
print("")
print(f"Exportable Objects: {list(session.exportable_objects.keys())}")

# Get the ServerObject for the server's "example_service" variable
example_service_obj = session.exportable_objects["example_service"]

# Wrap the ServerObject as a PluginClient
example_service_plugin_client = session.plugin_client(example_service_obj)

# Create a proxy object for the ExampleService
example_service = ExampleServiceProxy(example_service_plugin_client)

print("")
print("Calling ExampleService.hello_string()")
result = example_service.hello_string("Hello server!")
print(f"Result: {result}")

print("")
print("Calling ExampleService.hello_table()")
table_in = session.empty_table(10).update("X = i")
table_result = example_service.hello_table(table_in, "Hello server!")
print("Result:")
print(table_result.to_arrow().to_pandas())

# Close the session so that Python can exit
session.close()
```

The script prints `Result: Hello client. You said: Hello server!`, then a 10-row table with the input column `X` plus the `Client` column (`Hello server!`) and the `Server` column (`Hello client!`) that the server added.

## Share plugin objects between sessions

Sharing is useful when several client sessions need to work with the same server-side plugin object. Every session can already look up a variable in the server's [query scope](./query-scope.md), such as `example_service`, by name through `exportable_objects`. An object with no name, such as a table the server sent to one session as a reference, is visible only to the session that received it.

To share an object like that, the session that has it [publishes](../reference/client-api/session/publish.md) it, and other sessions [fetch](../reference/client-api/session/fetch.md) it. To keep the code short, the following example publishes `example_service`.

### Publish a plugin object

[Fetching](../reference/client-api/session/fetch.md) a plugin object gives your session an export ticket, which only that session can use. [Publishing](../reference/client-api/session/publish.md) the export ticket to a [shared ticket](/core/client-api/python/code/pydeephaven.ticket.html#pydeephaven.ticket.SharedTicket) lets any session [fetch](../reference/client-api/session/fetch.md) the same object.

The following script [fetches](../reference/client-api/session/fetch.md) `example_service`, [publishes](../reference/client-api/session/publish.md) the export ticket to a new shared ticket, and prints the shared ticket as a hex string that other sessions can use:

```python skip-test
from pydeephaven import Session
from pydeephaven.ticket import SharedTicket

# Connect to the server
session = Session(
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="YOUR_PASSWORD_HERE",
)

# Get the ServerObject for the server's "example_service" variable
example_service_obj = session.exportable_objects["example_service"]

# Fetch the plugin object to get an export ticket
export_ticket = session.fetch(example_service_obj)

# Publish to a shared ticket
shared_ticket = SharedTicket.random_ticket()
session.publish(export_ticket, shared_ticket)

# Print the shared ticket so other sessions can access the same plugin object
print(f"Shared ticket: {shared_ticket.bytes.hex()}")

# Keep this session open while other sessions use the shared ticket
input("Press Enter to release the shared object and exit...")
session.close()
```

Leave this script running while you run the fetch script below, and copy the hex string it prints.

> [!IMPORTANT]
> Shared tickets remain valid only as long as the publishing session keeps the object alive. When the publishing session closes or releases the object, the shared ticket becomes invalid.

### Fetch a shared plugin object

Another session can [fetch](../reference/client-api/session/fetch.md) the shared plugin object using the shared ticket. In the following script, replace `SHARED_TICKET_HEX` with the hex string that the publishing script printed:

```python skip-test
from pydeephaven import Session
from pydeephaven.ticket import ServerObject, SharedTicket
from example_plugin_client import ExampleServiceProxy

# Connect to the server
sub_session = Session(
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="YOUR_PASSWORD_HERE",
)

# Rebuild the shared ticket from the hex string that the publishing script printed
shared_ticket = SharedTicket(bytes.fromhex("SHARED_TICKET_HEX"))

# Create a ServerObject reference with the appropriate type
server_obj = ServerObject(type="ExampleService", ticket=shared_ticket)

# Create a plugin client to interact with the shared object
sub_plugin_client = sub_session.plugin_client(server_obj)

# Now you can use the plugin client as usual
example_service = ExampleServiceProxy(sub_plugin_client)

result = example_service.hello_string("Hello from another session!")
print(result)

sub_session.close()
```

The `type` string must match the server plugin's [`name`](/core/pydoc/code/deephaven.plugin.object_type.html#deephaven.plugin.object_type.ObjectType.name) property, `ExampleService`. If you change the type name in the plugin, change it here too.

> [!NOTE]
> An earlier version of this example is in the [plugin-bidirectional-example repository](https://github.com/deephaven-examples/plugin-bidirectional-example). Its client imports a `pydeephaven` module that no longer exists, so use the code on this page.

## Related documentation

- [Configure and use pre-shared key authentication](./authentication/auth-psk.md)
- [Create a JavaScript plugin](./create-js-plugins.md)
- [Install and use plugins](./install-use-plugins.md)
- [Install and run Deephaven with pip](../getting-started/pip-install.md)
- [Install and run Deephaven with Docker](../getting-started/docker-install.md)
- [Python client quickstart](../getting-started/pyclient-quickstart.md)
- [`fetch`](../reference/client-api/session/fetch.md)
- [`publish`](../reference/client-api/session/publish.md)
- [`deephaven.plugin` Pydoc](/core/pydoc/code/deephaven.plugin.html)
- [`deephaven.plugin.object_type` Pydoc](/core/pydoc/code/deephaven.plugin.object_type.html)
- [`pydeephaven.experimental.plugin_client` Pydoc](/core/client-api/python/code/pydeephaven.experimental.plugin_client.html)
- [`pydeephaven.session` Pydoc](/core/client-api/python/code/pydeephaven.session.html)
- [`pydeephaven.ticket` Pydoc](/core/client-api/python/code/pydeephaven.ticket.html)
