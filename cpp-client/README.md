# Deephaven C++ Client

The Deephaven C++ client is a native library for talking to a Deephaven Core server. It lets a C++
program create and query tables on the server, pull table data into memory as Apache Arrow tables,
push local data up to the server, and subscribe to ticking tables and receive a stream of updates.
It communicates with the server over gRPC + Apache Arrow Flight, using the Barrage protocol for
ticking data.

The build produces two libraries:

- **`dhcore`** — the client-side data model (tables, columns, chunks, row sets, types) and the
  ticking/Barrage state machine. It does not depend on Arrow or gRPC.
- **`dhclient`** — the user-facing client API (`Client`, `TableHandleManager`, `TableHandle`, …),
  built on `dhcore` plus Arrow Flight, gRPC, and protobuf.

The source lives one directory down, in `cpp-client/deephaven/`.

This library is not only used from C++: the [R client](../R/README.md) (`R/rdeephaven`) is an Rcpp
binding over `dhclient`, and `py/client-ticking` is a Cython binding over `dhcore`.

## Documentation

| Document | What it is |
|----------|------------|
| [`BUILDING.md`](BUILDING.md) | How to build and install the client and its dependencies on Linux. **Start here.** |
| [`README-windows.md`](README-windows.md) | How to build the C++ and Python clients on Windows 10/11. |
| [`DESIGN.md`](DESIGN.md) | Design and implementation of the client — architecture, code layout, the ticking pipeline, conventions, and per-file summaries. Written for engineers and coding agents who need to *change* the code. |
| [`doc/`](doc) | Source for the user-facing API reference (Sphinx + Doxygen), published at <https://docs.deephaven.io/core/client-api/cpp/>. |
| [`deephaven/examples/`](deephaven/examples) | Small standalone programs demonstrating the API; each builds against the installed client. |
| Repo root [`AGENTS.md`](../AGENTS.md) | The Java engine and server that this client talks to. |

## Quick start

Build and install the dependencies and the client per [`BUILDING.md`](BUILDING.md), then, with a
Deephaven server running:

```cpp
#include "deephaven/client/client.h"

using deephaven::client::Client;

auto client  = Client::Connect("localhost:10000");
auto manager = client.GetManager();
auto table   = manager.EmptyTable(10).Update("ABC = ii + 100");
std::cout << table.Stream(true) << '\n';
```

That is the core of `deephaven/examples/hello_world/main.cc`; after a successful build it is runnable
as `deephaven/build/examples/hello_world/hello_world`.
