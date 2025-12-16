---
title: Create your own plugin
---

This guide walks you through creating a plugin in Deephaven. Specifically, it covers creating a bidirectional plugin that extends the capabilities of Deephaven's [Python client API](/core/client-api/python). Bidirectional plugins allow users to create custom RPC methods that enable clients to interact with and return objects on a running server.

Plugins in Deephaven are just Python packages that extend the server's functionality, the UI, a client API, or a combination of these. They can be as simple as a single Python file or as complex as a multi-file package with dependencies. Plugins are packaged like any other Python package.

All plugins have both a server-side and a client-side component. Server-side plugin code is implemented with the [`deephaven.plugin`](/core/pydoc/code/deephaven.plugin.html) module, and client-side plugin code is implemented with the [`pydeephaven.experimental.plugin_client`](/core/client-api/python/code/pydeephaven.experimental.plugin_client.html) module.

The plugin example used in this guide can also be found [here](https://github.com/deephaven-examples/plugin-bidirectional-example).

## Plugin structure

This example creates a plugin called `ExampleService`. You can call your plugin whatever you'd like, but be aware that the name must be consistent across both the server and client. If you change the name as you follow along, update it in both the server and client code.

> [!NOTE]
> The directory name does not need to match the plugin name. This guide uses `ExampleServicePlugin` for the directory name, but the plugin itself is called `ExampleService`.

The plugin is broken into server- and client-side components. Create the following directory and file structure in your project folder:

```plaintext
ExampleServicePlugin/
└── server/
    └── pyproject.toml
    └── example_plugin_server/
        └── __init__.py
└── server/
    └── pyproject.toml
    └── example_plugin_client/
        └── __init__.py
```

> [!NOTE]
> This guide assumes the use of the [`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) configuration for packaging. You can also use `setup.cfg` or `setup.py` if you prefer.

### Server-side plugin

All server-side plugin code will live in the `server` folder. This folder contains a TOML file for packaging and a subfolder containing the actual plugin implementation.

#### Plugin implementation

This example plugin can be broken into three parts, all of which go in `server/example_plugin_server/__init__.py`:

- The plugin implementation.
- The object(s) it manages.
- The message stream handler.

Let's start with the plugin implementation itself, which contains all of the required methods for any bidirectional plugins. This example manages only a single object, however, more complex plugins can manage an arbitrary number of objects. In such a case, those objects would be added to the relevant methods.

```python skip-test
from deephaven.plugin.object_type import MessageStream, BidirectionalObjectType
from typing import override


class ExampleServicePlugin(BidirectionalObjectType):
    """Plugin for ExampleService."""

    @property
    @override
    def name(self) -> str:
        """Get the name of the service."""
        return "ExampleService"

    @override
    def is_type(self, object) -> bool:
        """Check if an object is an ExampleService."""
        return isinstance(object, ExampleService)

    @override
    def create_client_connection(
        self, obj: ExampleService, connection: MessageStream
    ) -> MessageStream:
        """Create a connection to an ExampleService instance."""
        return ExampleServiceMessageStream(obj, connection)
```

Now, create the object the plugin manages. In this case, the service will have two methods.

```python skip-test
from deephaven.table import Table


class ExampleService:
    """Example service that echoes strings and tables."""

    def hello_string(self, data: str) -> str:
        """Returns a string containing the input data."""
        return f"Hello client.  You said: {data}"

    def hello_table(self, table: Table, data: str) -> Table:
        """Returns a table generated from the input table and the input data."""
        return table.update(["Client = data", "Server = `Hello client!`"])
```

Lastly, define the message stream handler to pass messages between the client and the server.

```python skip-test
from deephaven.plugin.object_type import MessageStream, BidirectionalObjectType
from typing import List, Any, override
import traceback
import json


class ExampleServiceMessageStream(MessageStream):
    """
    MessageStream implementation for ExampleService.
    This will be called when the client sends a message to the server.
    """

    def __init__(self, service: ExampleService, client_connection: MessageStream):
        self.service = service
        self.client_connection = client_connection

        # Send an empty payload to the client to acknowledge successful connection
        self.client_connection.on_data(b"", [])

    @override
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
        except Exception as e:
            result_payload["error"] = traceback.format_exc()
            print(f"Error processing message: {result_payload['error']}")

        print(f"Sending result to client: {result_payload}")

        # Serialize the result payload to JSON bytes
        json_string = json.dumps(result_payload).encode("utf-8")
        self.client_connection.on_data(
            payload=json_string, references=result_references
        )

    @override
    def on_close(self):
        """Called when the client closes the connection."""
        print("Client connection closed.")
```

#### Plugin registration

The registration code is simpler than the implementation.

```python skip-test
from deephaven.plugin import Registration, Callback
from typing import override


class ExampleServicePluginRegistration(Registration):
    """Registration for ExampleServicePlugin."""

    @classmethod
    @override
    def register_into(cls, callback: Callback) -> None:
        """Register the ExampleServicePlugin."""
        callback.register(ExampleServicePlugin)
```

Altogether, the `__init__.py` should look like this:

<details>
<summary>__init__.py</summary>

```python skip-test
"""
This module provides a server-side plugin for accessing an ExampleService object.
"""

from deephaven.plugin.object_type import BidirectionalObjectType, MessageStream
from deephaven.plugin import Registration, Callback
from typing import List, Any, override
from deephaven.table import Table
import traceback
import json


class ExampleService:
    """Example service that echoes strings and tables."""

    def hello_string(self, data: str) -> str:
        """Returns a string containing the input data."""
        return f"Hello client.  You said: {data}"

    def hello_table(self, table: Table, data: str) -> Table:
        """Returns a table generated from the input table and the input data."""
        return table.update(["Client = data", "Server = `Hello client!`"])


class ExampleServiceMessageStream(MessageStream):
    """
    MessageStream implementation for ExampleService.
    This will be called when the client sends a message to the server.
    """

    def __init__(self, service: ExampleService, client_connection: MessageStream):
        self.service = service
        self.client_connection = client_connection

        # Send an empty payload to the client to acknowledge successful connection
        self.client_connection.on_data(b"", [])

    @override
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
        except Exception as e:
            result_payload["error"] = traceback.format_exc()
            print(f"Error processing message: {result_payload['error']}")

        print(f"Sending result to client: {result_payload}")

        # Serialize the result payload to JSON bytes
        json_string = json.dumps(result_payload).encode("utf-8")
        self.client_connection.on_data(
            payload=json_string, references=result_references
        )

    @override
    def on_close(self):
        """Called when the client closes the connection."""
        print("Client connection closed.")


class ExampleServicePlugin(BidirectionalObjectType):
    """Plugin for ExampleService."""

    @property
    @override
    def name(self) -> str:
        """Get the name of the service."""
        return "ExampleService"

    @override
    def is_type(self, object) -> bool:
        """Check if an object is an ExampleService."""
        return isinstance(object, ExampleService)

    @override
    def create_client_connection(
        self, obj: ExampleService, connection: MessageStream
    ) -> MessageStream:
        """Create a connection to an ExampleService instance."""
        return ExampleServiceMessageStream(obj, connection)


class ExampleServicePluginRegistration(Registration):
    """Registration for ExampleServicePlugin."""

    @classmethod
    @override
    def register_into(cls, callback: Callback) -> None:
        """Register the ExampleServicePlugin."""
        callback.register(ExampleServicePlugin)
```

</details>

#### Packaging

As mentioned previously, this project uses `pyproject.toml` to package the server-side code, which goes in `server/pyproject.toml`:

```toml
[build-system]
requires = ["setuptools"]
build-backend = "setuptools.build_meta"

[project]
name = "example_plugin_server"
version = "0.0.1"
dependencies = ["deephaven-plugin>=0.6.0", "deephaven-core>=0.36.1"]

[project.entry-points."deephaven.plugin"]
registration_cls = "example_plugin_server:ExampleServicePluginRegistration"
```

> [!NOTE]
> The server-side `pyproject.toml` file must register an entry point for the plugin.

#### Testing

You can test the plugin with only the server-side wiring implemented to make sure it registers with the server on startup. To see how, refer to the [Server-side testing](#server-side-testing) section.

### Client-side plugin

The client-side code and packaging live in the `client` folder of the project.

#### Plugin implementation

Like with the server-side code, the client-side code will be housed in a single file, `client/example_plugin_client/__init__.py`. The client-side implementation will mirror the server-side implementation to call the methods on the server-side object.

```python skip-test
import io
from typing import Any, List
from pydeephaven import Table
from pydeephaven.experimental import plugin_client, server_object
import json


class ExampleServiceProxy(server_object.ServerObject):
    """
    This class provides a client-side interface to the ExampleService server-side object.

    When you call a method on this class, it sends a message to the server-side object
    and returns the result. The inputs are serialized to JSON bytes and sent to the
    server, and the result is deserialized from JSON and returned.
    """

    def __init__(self, plugin_client: plugin_client.PluginClient):
        self.type_ = plugin_client.type_
        self.ticket = plugin_client.ticket

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

#### Packaging

Like with the server-side plugin, the client-side plugin is packaged with a TOML file, `client/pyproject.toml`:

```toml
[build-system]
requires = ["setuptools"]
build-backend = "setuptools.build_meta"

[project]
name = "example_plugin_client"
version = "0.0.1"
dependencies = ["pydeephaven>=0.36.1", "pandas"]
```

> [!NOTE]
> The client-side plugin for this example requires no registration.

## Use the plugin

Once you have completed all of the client—and server-side wiring, you can test the plugin. The following subsections cover testing the server using different launch methods.

### Installation

To test the plugin, you must first install it, along with Deephaven and any other required packages.

#### pip-installed Deephaven

If starting the server with pip-installed Deephaven, it is highly recommended to do so in a virtual environment. The following commands create a virtual environment and install the necessary packages, including the plugin.

```bash
# Run these from the root folder of the project
rm -rf ./venv-server
python3.12 -m venv ./venv-server
source ./venv-server/bin/activate
python -m pip install -U pip
python -m pip install -U deephaven-server
python -m pip install ./server
```

#### Docker

If starting the server with Docker, it's recommended to use Docker Compose to manage the server and any other services. The following Dockerfile builds a Docker image based on Deephaven's base server image, and installs the plugin:

```Dockerfile
FROM ghcr.io/deephaven/server:latest

COPY ./server /server

RUN pip install -U pip
RUN pip install /server
```

The following Docker Compose file runs Deephaven using the image built by the above Dockerfile:

> [!IMPORTANT]
> The Dockerfile below sets the pre-shared key to `YOUR_PASSWORD_HERE` for demonstration purposes. The client needs this key when connecting to the server. It is recommended that this key be changed to something more secure.

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

With those two files in hand, `docker compose up` installs the plugin and starts the server.

### Server-side testing

#### pip-installed Deephaven

If running Deephaven from Python, the following Python script will start a Deephaven server, create an instance of `ExampleService`, and keep the server running until ctrl+C is pressed:

```python skip-test
import sys
from example_plugin_server import ExampleService


example_service = ExampleService()

# Keep the server running until the user presses Control-C
print("Press Control-C to exit")

try:
    while True:
        input()
except KeyboardInterrupt:
    print("Exiting Deephaven...")
    sys.exit(0)
```

#### Docker

When running from Docker, you need only create an instance of `ExampleService` so that a client may interact with it:

```python skip-test
from example_plugin_server import ExampleService

example_service = ExampleService()
```

### Client-side testing

Then, to test the client-side plugin, run the following commands from the root of the project to create another virtual environment and install the necessary packages:

```bash
rm -rf ./venv-client
python3.12 -m venv ./venv-client
source ./venv-client/bin/activate
python -m pip install -U pip
python -m pip install ./client
```

With that said and done, the following Python script will use the client-side plugin:

> [!IMPORTANT]
> Replace `YOUR_PASSWORD_HERE` with the pre-shared key set when starting the server if you changed it.

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

# Get a ticket for an ExampleService object from the server named "example_service"
example_service_ticket = session.exportable_objects["example_service"]

# Wrap the ticket as a PluginClient
example_service_plugin_client = session.plugin_client(example_service_ticket)

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

## Share plugin objects between sessions

Plugin objects created in one session can be shared with other sessions using the [`publish`](/core/docs/reference/client-api/session/publish/) and [`fetch`](/core/docs/reference/client-api/session/fetch/) methods. This is useful when you want multiple client sessions to interact with the same server-side plugin object.

### Publishing a plugin object

To share a plugin object, first fetch it to get an export ticket, then publish it to a shared ticket:

```python skip-test
from pydeephaven import Session
from pydeephaven.ticket import SharedTicket

# Connect to the server
session = Session(
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="YOUR_PASSWORD_HERE",
)

# Get the plugin object ticket
example_service_ticket = session.exportable_objects["example_service"]

# Create a plugin client
plugin_client = session.plugin_client(example_service_ticket)

# Fetch the plugin object to get an export ticket
export_ticket = session.fetch(plugin_client)

# Publish to a shared ticket
shared_ticket = SharedTicket.random_ticket()
session.publish(export_ticket, shared_ticket)

# Now other sessions can use this shared_ticket to access the same plugin object
```

### Fetching a shared plugin object

Another session can fetch the shared plugin object using the shared ticket:

```python skip-test
from pydeephaven import Session
from pydeephaven.ticket import ServerObject, SharedTicket

# Connect to the server
sub_session = Session(
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="YOUR_PASSWORD_HERE",
)

# Use the shared ticket from the publishing session
# (In practice, you would pass this ticket between sessions)
shared_ticket = SharedTicket.random_ticket()  # Use the actual shared ticket

# Create a ServerObject reference with the appropriate type
server_obj = ServerObject(
    type="example_plugin_server.ExampleService", ticket=shared_ticket
)

# Create a plugin client to interact with the shared object
sub_plugin_client = sub_session.plugin_client(server_obj)

# Now you can use the plugin client as usual
from example_plugin_client import ExampleServiceProxy

example_service = ExampleServiceProxy(sub_plugin_client)

result = example_service.hello_string("Hello from another session!")
print(result)

sub_session.close()
```

> [!IMPORTANT]
> Shared tickets remain valid only as long as the publishing session keeps the object alive. When the publishing session closes or releases the object, the shared ticket becomes invalid.

## Related documentation

- [Install and use plugins](./install-use-plugins.md)
- [`publish`](/core/docs/reference/client-api/session/publish/) - Publish server objects to shared tickets
- [`fetch`](/core/docs/reference/client-api/session/fetch/) - Fetch server objects by ticket
