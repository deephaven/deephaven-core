# deephaven-pyecho

An example Deephaven **Python** `ObjectType` plugin — the Python counterpart to the Java `plugin/echo` example. It
echoes a wrapped object back to the client, and demonstrates how a plugin can apply the server's **authorization
transform** to the references it exports.

This is example/reference code; it is not part of the shipped Deephaven server.

## What it demonstrates

A plugin hands server objects (tables, etc.) to the client by passing them as references to `MessageStream.on_data`.
By default those references are exported untransformed. `PyEcho` instead calls
`deephaven.plugin_authorization.transform()` on each reference before exporting it, so the object carries the
**viewing user's** ACLs rather than the query owner's.

Because it transforms its own references, `PyEchoType` declares:

```python
authorization_export_behavior = "manual"
```

which opts out of the server's automatic transformation (so ACLs are not applied twice). The alternative — letting the
server transform automatically — is `authorization_export_behavior = "transform"` with no manual `transform()` call.

`transform()` uses the authorization context active *when it runs*. Inside a `deephaven.ui` `@ui.component` the plugin
renders per-viewer, so the echoed object is transformed for the viewer. At the top level it runs in the query owner's
context.

## Build

This is a standard PEP 517 setuptools package. From this directory (`plugin/pyecho`):

```sh
pip install build                 # one-time, if you don't already have it
python -m build                   # writes dist/deephaven_pyecho-<version>-py3-none-any.whl and .tar.gz
```

The wheel and sdist are written to `dist/`.

## Install

`PyEcho` runs inside a Deephaven server, so install it into the same Python environment as `deephaven` /
`deephaven-plugin` (the entry point in `setup.cfg` is what makes the server discover it).

Install the built wheel:

```sh
pip install dist/deephaven_pyecho-<version>-py3-none-any.whl
```

Or install directly from source (no separate build step needed):

```sh
pip install /path/to/deephaven-core/plugin/pyecho      # or: pip install -e . for an editable dev install
```

Restart the server (or re-run plugin registration) after installing so the plugin is picked up.

## Use

Wrap an object in `PyEcho` and assign it so a client can open it:

```python
from deephaven_pyecho import PyEcho
from deephaven import empty_table

# When a user opens my_echo, PyEcho echoes back a reference to the (transformed) table.
my_echo = PyEcho(empty_table(10).update(["I = i", "J = i % 2"]))
```

`PyEcho.create_client_connection` (and therefore `transform()`) runs in the context of whoever opens the object. Opened
directly as above, that is the query owner's context. To transform for a *viewer*, the object must be opened in the
viewer's context — i.e. created on a code path that runs per-viewer (for example, produced from within a `deephaven.ui`
`@ui.component`, which re-renders for each viewer). Under Deephaven Community the transform is the identity, so the
echoed table is unchanged; under an authorization provider that applies ACLs, the viewer sees only their permitted
rows/columns.

## Example: open it from a client

This script uses the `pydeephaven` client to open the plugin and read the echoed message — an (empty) payload plus the
transformed reference to the wrapped table — then fetches that reference as a table. Run it against a Deephaven server
that has `deephaven-pyecho` installed.

```python
from pydeephaven import Session

session = Session()  # host="localhost", port=10000, plus auth as needed

# Create a PyEcho on the server wrapping a table.
session.run_script(
    """
from deephaven_pyecho import PyEcho
from deephaven import empty_table

my_echo = PyEcho(empty_table(10).update(["I = i", "J = i % 2"]))
"""
)

# Open the plugin. On connect, PyEcho echoes its wrapped object back as the first message.
plugin_client = session.plugin_client(session.exportable_objects["my_echo"])

# resp_stream yields (payload_bytes, [references]). PyEcho sends an empty payload and one reference.
payload, refs = next(plugin_client.resp_stream)
print("echo payload:", payload)                  # b"" - PyEcho echoes an empty payload
print("reference types:", [r.type for r in refs])  # ['Table'] - the transformed table reference
assert len(refs) == 1 and refs[0].type == "Table"

# Fetch the transformed reference as a table and read it.
table = refs[0].fetch()
print("echoed table size:", table.size)          # 10 under Community (identity transform);
                                                  # fewer under an authorization provider that filters rows
print(table.to_arrow())

plugin_client.close()
session.close()
```

Under Deephaven Community the transform is the identity, so the fetched table matches the source (10 rows). Under an
authorization provider that applies ACLs — and when the plugin is opened in the viewer's context — the fetched table
reflects only the rows/columns that viewer is permitted to see.
