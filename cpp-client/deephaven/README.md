# Deephaven C++ Client — Design & Implementation

Orientation document for engineers and coding agents who need to change code in
`cpp-client/deephaven/`. It describes *how the client works* and *where things live*, so you can go
straight to the right file instead of discovering the layout by grepping.

Companion docs:

- `cpp-client/README.md` — build/install instructions (Linux); `cpp-client/README-windows.md` (Windows).
- `cpp-client/doc/*.rst` + `cpp-client/doc/Doxyfile` — user-facing API reference (Sphinx/Doxygen),
  published at <https://docs.deephaven.io/core/client-api/cpp/>.
- Repo root `AGENTS.md` — the Java engine/server side of the system.

**How to use this file:** the section headings below are stable anchors. `grep -n '^## ' cpp-client/deephaven/README.md`
to get the map, then read only the sections you need.

---

## Table of contents

| § | Section | Read it when… |
|---|---------|---------------|
| 1 | [Directory map](#1-directory-map) | you need to find a file |
| 2 | [Build, install, test](#2-build-install-test) | you need to compile or run tests |
| 3 | [Layered architecture](#3-layered-architecture) | you are new to the codebase |
| 4 | [Public API surface (dhclient)](#4-public-api-surface-dhclient) | you are adding/altering a user-visible API |
| 5 | [Connection, RPC, tickets, threads](#5-connection-rpc-tickets-threads) | you touch auth, sessions, lifetime, shutdown |
| 6 | [How a table op becomes an RPC](#6-how-a-table-op-becomes-an-rpc) | you are adding a table operation |
| 7 | [Data model (dhcore)](#7-data-model-dhcore) | you touch types, columns, chunks, row sets |
| 8 | [Snapshot path (static data)](#8-snapshot-path-static-data) | you touch `ToArrowTable` / `ToClientTable` / Arrow conversion |
| 9 | [Ticking path (Barrage subscriptions)](#9-ticking-path-barrage-subscriptions) | you touch subscriptions / ticking |
| 10 | [Table state: key space vs index space](#10-table-state-key-space-vs-index-space) | you touch `ImmerTableState` / `SpaceMapper` |
| 11 | [Interop C ABI and downstream consumers](#11-interop-c-abi-and-downstream-consumers) | you change a public signature in `dhcore` |
| 12 | [Conventions, style, gotchas](#12-conventions-style-gotchas) | before you write any code |
| 13 | [Recipes](#13-recipes) | you have a concrete task |
| 14 | [File index](#14-file-index) | you want a one-line summary of a file |

---

## 1. Directory map

```
cpp-client/deephaven/
  CMakeLists.txt          top-level: builds dhcore, dhclient, tests, examples; install/export rules
  cmake/deephavenConfig.cmake   installed CMake package config (find_package(deephaven))
  dhcore/                 "core": data model + ticking state machine. NO gRPC/Arrow/protobuf deps.
    include/public/deephaven/dhcore/**   installed headers, visible to the end user
    include/private/deephaven/dhcore/**  internal headers (ticking internals, immer helpers)
    src/**                C++ implementation files
    flatbuf/deephaven/flatbuf/Barrage_generated.h   generated from Barrage.fbs (checked in, do not edit)
    third_party/          vendored: flatbuffers (private), date (private), roaring (roaring.c), fmt
  dhclient/               "client": gRPC + Arrow Flight + protobuf; the user-facing API.
    include/public/deephaven/client/**   installed headers, visible to the end user
    include/private/deephaven/client/**  internal headers for impl classes, Server, subscription internals
    src/**                C++ implementation files
  tests/                  Catch2 (`third_party/catch.hpp`) integration tests; need a live server
  examples/               small standalone programs, each with its own CMakeLists
```

Two libraries are produced:

| Target | Kind | Depends on | Public headers |
|--------|------|-----------|----------------|
| `deephaven::dhcore` (`dhcore`, plus `dhcore_static`) | shared + static | `immer` only (+ vendored fmt/flatbuffers/date/roaring) | `deephaven/dhcore/**`, `deephaven/third_party/fmt/**` |
| `deephaven::client` (`dhclient`) | shared | `dhcore`, Arrow, Arrow Flight, gRPC, protobuf, Threads | `deephaven/client/**` |

**The dhcore/dhclient split is deliberate and load-bearing.** `dhcore` contains everything needed to
hold a table client-side and to apply Barrage update messages, *without* depending on Arrow or gRPC.
That is what lets `py/client-ticking` (Cython) statically link `dhcore_static` and drive the ticking
machinery with data it obtains from pyarrow, and what lets the R client link both. Do not introduce
an Arrow, gRPC, or protobuf dependency into `dhcore`.

`include/public` vs `include/private`: `public` is on the target's `PUBLIC` include path and is
installed; `private` is `PRIVATE` and never installed. Header include paths always start with
`deephaven/` (`deephaven/client/...` or `deephaven/dhcore/...`), for both public and private headers.

---

## 2. Build, install, test

Dependencies are built once by `cpp-client/build-dependencies.sh` into a prefix directory (the
convention is `$DHCPP`), which also writes an `env.sh` you must `source` (it sets `CMAKE_PREFIX_PATH`,
`LD_LIBRARY_PATH`, `NCPUS`). Full instructions: `cpp-client/README.md`.

```bash
source $DHCPP/env.sh
cd cpp-client/deephaven
cmake -S . -B build \
  -DCMAKE_INSTALL_LIBDIR=lib -DCMAKE_CXX_STANDARD=17 \
  -DCMAKE_INSTALL_PREFIX=${DHCPP} -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_SHARED_LIBS=ON
VERBOSE=1 cmake --build build --target install -- -j$NCPUS
```

Useful CMake options: `-DDHCORE_ONLY=ON` (skip dhclient/tests/examples — fast loop when only
touching the core), `-DSANITIZE_ADDRESS=ON`.

Notes:
- Warnings are errors: `-Wall -Werror` on Linux for all three targets. Windows uses `/W3`.
- Windows has the additional flags `/bigobj` (to increase the max number of sections in
  an object file, and `/MP` to compile using multiple cores.
- The protobuf/gRPC C++ stubs are generated **at build time** by `dhclient/CMakeLists.txt` from
  `proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/*.proto` (11 files) into
  `${CMAKE_BINARY_DIR}/dhclient/proto`. Adding a new `.proto` requires editing `PROTO_FILES`.
- `Barrage_generated.h` is checked in and *not* regenerated by the build.
- Build type must be consistent with the dependency build (protobuf Debug/Release ABIs differ).

**Tests** (`tests/`, Catch2, single binary `dhclient_tests`) are integration tests: they need a
running Deephaven server. `tests/src/test_util.cc` reads env vars `DH_HOST` (default `localhost`)
and `DH_PORT` (default `10000`).

```bash
cd build/tests && ./dhclient_tests                    # all
./dhclient_tests "[simple]"                           # by tag
./dhclient_tests "Close plays nice with destructor"   # by name
```

`TableMakerForTests::Create()` builds the canonical 5-column test table (ImportDate/Ticker/Open/
Close/Volume); `TableComparerForTests::Compare(expected_TableMaker, actual)` is the standard
assertion helper (overloads for `TableHandle`, `ClientTable`, `arrow::Table`).

**Gradle/CI**: `cpp-client/build.gradle` builds a Docker image (`cppClient`), runs the tests against a
containerized server (`testCppClient`), and converts Catch2 output to JUnit via `cpp-tests-to-junit.sh`.
Spotless enforces the copyright header on all `*.cc`/`*.h` except `Barrage_generated.h`,
`third_party/**`, `*.pb.*` — run `./gradlew spotlessApply` from the repo root.

---

## 3. Layered architecture

```
 user code
   │  deephaven/client/client.h : Client, TableHandleManager, TableHandle, Aggregate, SortPair
   ▼
 ── pimpl boundary ─────────────────────────────────────────────────────────────
   │  ClientImpl, TableHandleManagerImpl, TableHandleImpl, AggregateImpl, UpdateByOperationImpl
   ▼
 Server  (dhclient/.../server/server.h)
   │  owns gRPC stubs (Table/Session/Console/Config/Application/InputTable) + arrow::flight::FlightClient
   │  owns ServerSharedState: session token, ticket allocator, keepalive thread
   ▼
 wire: gRPC (proto/) for table ops · Arrow Flight DoGet/DoPut for bulk data · Barrage (flatbuffers
       over Flight DoExchange) for ticking
   ▲
   │  dhcore: ClientTable / ColumnSource / Chunk / RowSequence / Schema / ElementType
   │  dhcore ticking: BarrageProcessor → ImmerTableState → TickingUpdate → TickingCallback
```

Key design decisions:

- **Everything user-facing is pimpl.** `Client`, `TableHandleManager`, `TableHandle`, `Aggregate`,
  `AggregateCombo`, `UpdateByOperation` each hold a single `std::shared_ptr<XxxImpl>`. Public
  headers therefore do not include protobuf, gRPC, or (mostly) Arrow. Exceptions that *do* expose
  Arrow: `client/flight.h`, `client/utility/arrow_util.h`, `client/utility/table_maker.h`.
  This is intended to support a pay-as-you-go style: end-user code that does not explicitly need Arrow
  functionality does not need to reference Arrow include files, directly or indirectly.
- **Tables live on the server.** A `TableHandle` is a *handle* (an exported ticket), not data.
  Operations are RPCs that create new server-side tables and return new handles. Data only comes to
  the client through Flight (snapshot) or Barrage (subscription).
- **Value semantics**: `Client` and `TableHandleManager` are move-only; `TableHandle` is copyable
  (shared ownership of the ticket).
- **Errors are exceptions.** `std::runtime_error` with a message decorated by
  `DEEPHAVEN_LOCATION_STR` / `DEEPHAVEN_LOCATION_EXPR` (function, file, line). Arrow `Status`/`Result`
  are converted by `OkOrThrow` / `ValueOrThrow` (`client/utility/arrow_util.h`).
- **Logging** is Abseil logging (`absl/log/log.h`): `LOG(INFO)`, `LOG(ERROR)`, `VLOG(2)`.
  `client/utility/logging.h` runs `log_verbosity_init()` via a static initializer.

---

## 4. Public API surface (dhclient)

`dhclient/include/public/deephaven/client/`:

| Header | Contents |
|--------|----------|
| `client.h` (~1900 lines) | `Client`, `TableHandleManager`, `TableHandle`, `Aggregate`/`AggregateCombo`, `SortPair`, `SortDirection`, free `Agg*` helpers |
| `client_options.h` | `ClientOptions` (auth, TLS, session type, gRPC options, extra headers) + header-name constants |
| `flight.h` | `FlightWrapper` — raw Arrow Flight access (`GetFlightStreamReader`, `AddHeaders`, `FlightClient`) |
| `update_by.h` | `UpdateByOperation` + ~35 factory functions (`CumSum`, `Ema*`, `Rolling*`, …), `MathContext`, `BadDataBehavior`, `DeltaControl`, `OperationControl` |
| `utility/table_maker.h` | `TableMaker` — build a small table locally and DoPut it to the server |
| `utility/arrow_util.h` | `ArrowUtil` type/schema conversions, `OkOrThrow`, `ValueOrThrow` |
| `utility/misc_types.h` | `DurationSpecifier`, `TimePointSpecifier` (variants), `OnCloseCb`/`OnCloseCbId` |
| `interop/*.h` | `extern "C"` ABI for .NET-style bindings (§11) |

Entry point:

```cpp
auto client  = Client::Connect("localhost:10000", ClientOptions().SetSessionType("python"));
auto manager = client.GetManager();
auto t       = manager.EmptyTable(10).Update("ABC = ii + 100");
std::cout << t.Stream(true) << '\n';       // pretty-print (ostream adaptor)
```

`TableHandleManager` creates root tables: `EmptyTable`, `FetchTable` (by name from the server scope),
`TimeTable`, `InputTable`, `NewTicket`/`MakeTableHandleFromTicket` (for manual Flight DoPut),
`RunScript`, `CreateFlightWrapper`.

`TableHandle` has the derived operations: `Select`/`View`/`Update`/`UpdateView`/`LazyUpdate`/
`DropColumns`, `Where`/`WhereIn`, `Sort`, `Head`/`Tail`, aggregations (`By` with `AggregateCombo`,
plus `SumBy`, `AvgBy`, `LastBy`, `CountBy`, `PercentileBy`, `HeadBy`/`TailBy`, …), joins
(`NaturalJoin`, `ExactJoin`, `CrossJoin`, `Aj`, `Raj`, `LeftOuterJoin`), `Merge`, `Ungroup`,
`SelectDistinct`, `UpdateBy`, `AddTable`/`RemoveTable` (input tables), `BindToVariable`, and the
data-access methods `ToArrowTable`, `ToClientTable`, `GetFlightStreamReader`, `Subscribe`/`Unsubscribe`,
`Schema`, `NumRows`, `IsStatic`, `Stream`/`ToString`.

**Variadic string overloads.** Nearly every method taking `std::vector<std::string>` has a
variadic sibling built on `internal::ConvertToString`, which accepts any mix of `const char*`,
`std::string_view`, `std::string`. When you add a vector-taking method, add the variadic template
next to it for consistency.

---

## 5. Connection, RPC, tickets, threads

`Server` (`dhclient/include/private/deephaven/client/server/server.h`, `src/server/server.cc`) is the
single owner of the connection.

- `Server::CreateFromTarget(target, options)` builds credentials (TLS or insecure), a
  `grpc::Channel`, six service stubs, and an `arrow::flight::FlightClient`. The Flight location is
  derived from the same `host:port` string with a `grpc://` or `grpc+tls://` scheme (and `:443` is
  appended when TLS is on and no port was given).
- `ServerSharedState` (`server_shared_state.h`) is shared between `Server` and the Flight
  middleware: mutex/condvar, session token, ticket allocator (`nextFreeTicketId_`), the set of
  outstanding tickets, `cancelled_` flag, expiration interval, and the keepalive thread.
- `Server::SendRpc(callback)` is the choke point for every unary RPC: it creates a
  `grpc::ClientContext`, injects headers (authorization + extra headers), refuses to run if
  `cancelled_`, invokes the callback, converts a non-OK `grpc::Status` into a `std::runtime_error`,
  and harvests a refreshed authorization token from the server's initial metadata.
- **Auth over Flight** is handled by `BearerMiddleware` / `BearerMiddlewareFactory`
  (`server/bearer_middleware.{h,cc}`), an `arrow::flight::ClientMiddleware` that adds
  `authorization: Bearer <token>` on the way out and updates the token on the way in. Code paths
  that build `FlightCallOptions` by hand (`FlightWrapper::AddHeaders`, `TableHandleImpl::Schema`,
  `SubscribeState::InvokeHelper`) deliberately **skip** the `authorization` and `envoy-prefix`
  headers to avoid duplicating what the middleware already does.
- **Tickets** are client-allocated (`Server::NewTicket()` → monotonically increasing int32 in the
  client's namespace) and tracked in `outstanding_tickets_`. `~TableHandleImpl` calls
  `Server::Release(ticket)`. `Server::Shutdown()` releases all remaining tickets (with
  `disregard_cancellation_state = true`), sets `cancelled_`, and joins the keepalive thread.
- **Keepalive**: a dedicated thread (`SendKeepaliveMessages` / `KeepaliveHelper`) sends a
  `GetConfigurationConstants` RPC as a handshake before `nextHandshakeTime_` and refreshes the token.

**Threads in a running client:** (1) the keepalive thread; (2) the *flight executor* thread; (3) the
*client executor* thread (currently created and shut down but otherwise unused — the only live
`Executor` use is `FlightExecutor()` in `TableHandleImpl::Subscribe`); (4) one `UpdateProcessor`
thread per active subscription. `Executor` (`utility/executor.{h,cc}`) is a trivial single-thread
work queue; exceptions thrown by queued functions are logged and swallowed.

**Shutdown order** (`Client::Close()`, also run from `~Client`): `ClientImpl::Shutdown` first runs
on-close callbacks (so callbacks may still use the client), then `TableHandleManagerImpl::Shutdown`
cancels all subscriptions, shuts both executors, and shuts down `Server`. After `Close()`, using any
derived `TableHandleManager`/`TableHandle` is unspecified behavior.

---

## 6. How a table op becomes an RPC

Every operation follows the same three-line shape. `TableHandleImpl::SelectOrUpdateHelper` is the
canonical example (`dhclient/src/impl/table_handle_impl.cc`):

```cpp
auto *server = managerImpl_->Server().get();
SelectOrUpdateRequest req;
*req.mutable_result_id() = server->NewTicket();              // 1. allocate the result ticket
*req.mutable_source_id()->mutable_ticket() = ticket_;        // 2. name the source table
for (auto &cs : column_specs) *req.mutable_column_specs()->Add() = std::move(cs);
ExportedTableCreationResponse resp;
server->SendRpc([&](grpc::ClientContext *ctx) {              // 3. one RPC through SendRpc
  return (server->TableStub()->*which_method)(ctx, req, &resp);
});
return TableHandleImpl::Create(managerImpl_, std::move(resp));  // 4. wrap ticket + size + is_static
```

Notes:
- `Select`/`Update`/`LazyUpdate`/`View` differ only by a pointer-to-member-function
  (`selectOrUpdateMethod_t`) passed to the shared helper.
- `ExportedTableCreationResponse` carries `result_id.ticket`, `size`, `is_static` — these become
  `TableHandleImpl::num_rows_` / `is_static_` and are what `TableHandle::NumRows()` / `IsStatic()`
  return (they are a snapshot from creation time, not live values).
- Aggregations go through `ComboAggregateRequest`; `DefaultAggregateByType` / `DefaultAggregateByDescriptor`
  wrap the single-aggregate convenience methods (`SumBy`, `AvgBy`, …).
- `Where` uses `UnstructuredFilterTableRequest` (string filters).
- `impl/util.h` has `MoveVectorData` for moving a `std::vector<std::string>` into a repeated proto field.
- `TableHandleImpl::Schema()` is the odd one out: it is fetched lazily over **Flight `GetSchema`**
  (not gRPC), memoized in a `std::shared_future` guarded by `mutex_` + `schema_request_sent_`, so
  concurrent callers share one round trip and one exception.

---

## 7. Data model (dhcore)

All under `deephaven/dhcore/`. This is the vocabulary shared by snapshots, ticking, Cython and R.

### Types — `types.h`, `src/types.cc`

- `ElementTypeId::Enum` — 12 scalar types: `kChar, kInt8, kInt16, kInt32, kInt64, kFloat, kDouble,
  kBool, kString, kTimestamp, kLocalDate, kLocalTime`. (It is a plain `enum`, not `enum class`,
  for Cython's benefit.)
- `ElementType` = `{ list_depth, element_type_id }`. `list_depth == 0` is a scalar, `1` is
  `list<T>`, etc. `WrapList()` / `UnwrapList()`. Only depths 0 and 1 are supported end-to-end.
- `DeephavenConstants` — the sentinel null values (`kNullInt = INT32_MIN`, `kNullLong = INT64_MIN`,
  `kNullDouble = -DBL_MAX`, `kNullChar = UINT16_MAX`, …) plus min/max. `DeephavenTraits<T>` exposes
  `kNullValue` and `kIsNumeric` per type.
  **Null representation differs by type**: numeric types encode null in-band as the sentinel value
  (`kIsNumeric == true`); `bool`, `std::string`, `DateTime`, `LocalDate`, `LocalTime` and containers
  carry null out-of-band in a parallel `bool` array. Almost every generic routine in the codebase is
  parameterized on this distinction — see the `kIsNumeric` branches in `ImmerColumnSourceImpls::FillChunk`
  and `ArrowProcessingStyle` in `arrow_column_source.h`.
- `DateTime` (nanos since epoch, `IsBlittableToInt64()`), `LocalDate` (millis), `LocalTime` (nanos).

### Chunks — `chunk/chunk.h`, `chunk/chunk_traits.h`, `chunk/chunk_maker.h`

`GenericChunk<T>` is a typed, shared-ownership data buffer (`data()`, `begin()`, `end()`, `Take`,
`Drop`, `CreateView`). Aliases: `Int32Chunk`, `DoubleChunk`, `StringChunk`, `BooleanChunk`,
`UInt64Chunk`, `DateTimeChunk`, … `AnyChunk` is the type-erased holder (`Unwrap()`); `Chunk` is the
abstract base with the visitor hook. `TypeToChunk<T>::type_t` maps element type → chunk type.
`ChunkMaker::CreateChunkFor(column_source, size)` allocates the right chunk for a column source.

### RowSequence — `container/row_sequence.h`, `src/container/row_sequence.cc`

An ordered set of row keys, stored as intervals. Abstract; `CreateEmpty()`, `CreateSequential(begin,end)`,
`Take(n)`, `Drop(n)`, `Size()`, `Empty()`, `ForEachInterval(cb)`, and `RowSequenceIterator` for
key-at-a-time traversal. Build one with `RowSequenceBuilder` (`Add`, `AddInterval`, `Build`).
`Take`/`Drop` are the workhorses of the Barrage chunk loop.

### ColumnSource — `column/column_source.h`

The read interface for a column:

```cpp
virtual void FillChunk(const RowSequence &rows, Chunk *dest, BooleanChunk *optional_null_flags) const;
virtual void FillChunkUnordered(const UInt64Chunk &rows, Chunk *dest, BooleanChunk *optional_nulls) const;
virtual const ElementType &GetElementType() const;
virtual void AcceptVisitor(ColumnSourceVisitor *) const;
```

`MutableColumnSource` adds `FillFromChunk` / `FillFromChunkUnordered`. `GenericColumnSource<T>` gives
per-type aliases (`Int32ColumnSource`, `StringColumnSource`, `ContainerBaseColumnSource`, …).
`ColumnSourceVisitor` has one `Visit` per element type — **adding an element type means updating
this visitor and all its implementers**.

Implementations:

| Class | Header | Backing store | Used for |
|-------|--------|---------------|----------|
| `GenericArrayColumnSource<T>` | `column/array_column_source.h` | growable owned array | mutable local columns |
| `NumericBufferColumnSource<T>` / `GenericBufferColumnSource<T>` | `column/buffer_column_source.h` | borrowed pointer (no ownership) | zero-copy views (Cython) |
| `ContainerColumnSource<T>` | `column/container_column_source.h` | `shared_ptr<ContainerBase>[]` | list-typed columns |
| `NumericImmerColumnSource<T>` / `GenericImmerColumnSource<T>` | `private/.../immerutil/immer_column_source.h` | `immer::flex_vector` | ticking snapshots |
| `GenericArrowColumnSource<Style,…>` | `private/.../client/arrowutil/arrow_column_source.h` | `std::vector<shared_ptr<arrow::Array>>` | incoming Flight/Barrage data (dhclient only) |

### Containers — `container/container.h`, `container/container_util.h`

`ContainerBase` / `Container<T>` model a single list-valued cell: `data()`, `size()`, `IsNull(i)`,
`AsContainer<T>()`, plus `ContainerVisitor`.

### Schema and ClientTable — `clienttable/schema.h`, `clienttable/client_table.h`

`Schema` = parallel `names` / `ElementType`s + a name→index map; immutable, created via
`Schema::Create`. `ClientTable` is the abstract client-side table: `GetRowSequence()`,
`GetColumn(index)` / `GetColumn(name, strict)`, `GetColumnIndex`, `NumRows`, `NumColumns`, `Schema()`,
plus `Stream(...)`/`ToString(...)` pretty-printers (`internal::TableStreamAdaptor`). There are three
implementations: `ArrowClientTable` (snapshot, dhclient), the anonymous `MyTable` inside
`src/ticking/immer_table_state.cc` (ticking snapshot), and whatever Cython builds.

---

## 8. Snapshot path (static data)

```
TableHandle::ToClientTable()
  → GetFlightStreamReader()            (FlightWrapper::GetFlightStreamReader → Flight DoGet on the ticket)
  → arrow::Table
  → ArrowClientTable::Create(arrow_table)
      → ArrowUtil::MakeDeephavenSchema(arrow schema)   [arrow type → ElementType]
      → ArrowArrayConverter::ChunkedArrayToColumnSource per column

TableHandle::ToArrowTable(cooked = true)     // default
  → ToClientTable() then ArrowUtil::MakeArrowTable()   // normalizes away Dictionary / RunEndEncoded
TableHandle::ToArrowTable(false)
  → raw arrow::Table straight from Flight (may contain encoded arrays)
```

`dhclient/src/arrowutil/arrow_array_converter.cc` (~1000 lines) is the type-dispatch hub. Its
notable machinery:

- `Reconstituter` — downcasts an `arrow::ChunkedArray`'s chunks into `vector<shared_ptr<ConcreteArray>>`.
- `EncodedChunkDecoder<Derived>` (CRTP) + `DictionaryChunkDecoder`, `RunEndChunkDecoder`,
  `RunEndDictionaryChunkDecoder` — decode Arrow's compressed encodings into plain arrays. The server
  may send dictionary-encoded, run-end-encoded, or run-end-encoded-*of*-dictionary columns; the
  client always materializes them, resolving the index/run-end integer types once so the inner copy
  loops are fully typed.
- `ChunkedArrayToColumnSourceVisitor` — the top-level `arrow::TypeVisitor` producing the right
  `GenericArrowColumnSource` instantiation.

Type mapping (`ArrowUtil::GetElementType` / `GetArrowType` in `src/utility/arrow_util.cc`):

| Deephaven | Arrow |
|-----------|-------|
| `kChar` | `UInt16` |
| `kInt8/16/32/64`, `kFloat`, `kDouble`, `kBool`, `kString` | the obvious Arrow types |
| `kTimestamp` | `Timestamp(NANO, "UTC")` |
| `kLocalDate` | `Date64` |
| `kLocalTime` | `Time64(NANO)` |
| `list<T>` | `List<arrow T>` |

Incoming timestamps/times in other units are rescaled by `internal::ScaleFromUnit`
(`arrow_column_source.h`); the `ArrowProcessingStyle` enum (`kNormal`, `kBooleanOrString`,
`kTimestamp`, `kLocalDate`, `kLocalTime`) selects the copy/null strategy.

**Pushing data up**: `TableMaker` (`utility/table_maker.h`) builds Arrow arrays with per-type
`internal::ColumnBuilder<T>` specializations (including `std::optional<T>` for nullable and
`std::vector<T>` for list columns), attaches Deephaven type metadata to each field
(`DeephavenMetadataConstants::Keys::Type()` / `ComponentType()`), and `MakeTable(manager)` does a
Flight DoPut against a fresh ticket. `MakeArrowTable()` returns the local table instead.
Manual route: `manager.NewTicket()` → `ArrowUtil::ConvertTicketToFlightDescriptor` → your own DoPut →
`manager.MakeTableHandleFromTicket(ticket)`.

---

## 9. Ticking path (Barrage subscriptions)

User contract (`dhcore/ticking/ticking.h`): implement `TickingCallback` (`OnTick(TickingUpdate)`,
`OnFailure(std::exception_ptr)`) and pass it to TableHandle::Subscribe; there is also a C-style overload
`Subscribe(on_tick_fn, user_data, on_error_fn, user_data)` but its use is discouraged and it
might be removed. `Unsubscribe(handle)` (or client shutdown) stops it.

`TickingUpdate` describes one cycle as a chain of immutable snapshots plus the deltas between them:

```
Prev() == BeforeRemoves()  --RemovedRows()-->  AfterRemoves() == BeforeAdds()
   --AddedRows()-->  AfterAdds() == BeforeModifies()
   --ModifiedRows()[per column] / AllModifiedRows()-->  AfterModifies() == Current()
```

Snapshots share structure (immer), so holding onto old `TickingUpdate`s is cheap-ish and thread-safe.
Pointer equality is meaningful: if nothing was removed, `AfterRemoves() == BeforeRemoves()`.
`AllModifiedRows()` is computed lazily and memoized in `internal::OnDemandState`.

### Wire-up (`dhclient/src/subscription/subscribe_thread.cc`)

1. `TableHandleImpl::Subscribe` first calls `Schema()` (synchronous Flight `GetSchema`), then
   `SubscriptionThread::Start(...)`, then registers the handle with `TableHandleManagerImpl`.
2. `Start` posts a `SubscribeState` onto the **flight executor** thread and blocks on a
   `std::promise`. So subscription *setup* errors surface synchronously to the caller.
3. `SubscribeState::InvokeHelper` opens a Flight `DoExchange` whose `FlightDescriptor` is `CMD` with
   the 4-byte Deephaven magic number `0x6E687064` (`"dphn"`), then writes one metadata message:
   `BarrageProcessor::CreateSubscriptionRequest(ticket_bytes, size)` — a `BarrageMessageWrapper`
   wrapping a `BarrageSubscriptionRequest` with options
   `{Stringify, use_deephaven_nulls = true, min_update_interval_ms = 0, batch_size = 4096,
   max_message_size = 0, columns_as_list = true}`.
4. `UpdateProcessor::StartThread` spawns a dedicated thread running `RunForeverHelper`, and the
   `UpdateProcessor` *is* the `SubscriptionHandle` returned to the user (`Cancel()` cancels the
   reader, closes the writer, joins the thread).
5. `RunForeverHelper` loops on `FlightStreamReader::Next()`. For each chunk it calls `UnwrapList` on
   every column (because `columns_as_list = true`, each column arrives as a `ListArray` of length 1
   whose single element is the real array — this is how Barrage sends columns of *differing* lengths
   in one Arrow record batch), converts each to a `ColumnSource`, and feeds
   `BarrageProcessor::ProcessNextChunk(sources, sizes, app_metadata, metadata_size)`.
   A returned `std::optional<TickingUpdate>` with a value means the cycle is complete → `OnTick`.
   Exceptions are routed to `OnFailure` unless the processor was cancelled.

### The state machine (`dhcore/src/ticking/barrage_processor.cc`)

`BarrageProcessorImpl` is a four-state machine; one Barrage *cycle* may span several Arrow messages,
and one Arrow message may complete several phases:

| State | Class | Responsibility |
|-------|-------|----------------|
| `kAwaitingMetadata` | `AwaitingMetadata` | parse the flatbuffer metadata; apply removes and shifts; register adds |
| `kAwaitingAdds` | `AwaitingAdds` | consume add data chunks until `added_rows_remaining_` is empty |
| `kAwaitingModifies` | `AwaitingModifies` | consume per-column modify data until all columns are satisfied |
| `kBuildingResult` | `BuildingResult` | assemble the `TickingUpdate`, reset, return to `kAwaitingMetadata` |

`AwaitingMetadata::ProcessNextChunk` validates the magic number and message type, then reads five
compressed index streams from `BarrageUpdateMetadata` via `IndexDecoder::ReadExternalCompressedDelta`
(`ticking/index_decoder.{h,cc}`, with `DataInput` as the byte reader): `removed_rows`,
`shift_start/end/dest`, `added_rows`, plus `modified_rows` per column from `mod_column_nodes`.

**The order is fixed and matters: removes → shifts → adds → modifies.** Removes and shifts can be
applied immediately (no payload needed); adds/modifies wait for data chunks.

The `begins`/`ends` vectors are the plumbing that lets phases share one Arrow message: each phase
advances `begins[i]` to `ends[i]` for the columns it consumed, and returns an empty `optional` to
mean "give me another message". `BuildingResult` throws if `begins != ends` (leftover data) — that
check is your friend when debugging protocol drift.

---

## 10. Table state: key space vs index space

This distinction is the single most important concept in the ticking code.

- **Key space** — the server's row keys. Sparse, arbitrary `uint64_t`s, shifted around by the server.
- **Index space** (a.k.a. position space) — dense `0..n-1` positions in the client's local copy.

`SpaceMapper` (`private/.../ticking/space_mapper.h`, `src/ticking/space_mapper.cc`) owns the mapping,
implemented as a `roaring::Roaring64Map` of present keys where a key's index is its *rank* in the set:
`AddRange`, `EraseRange`, `ApplyShift`, `AddKeys(keys) → indices`, `ConvertKeysToIndices`,
`ZeroBasedRank`, `Cardinality`.

`ImmerTableState` (`private/.../ticking/immer_table_state.h`, `src/ticking/immer_table_state.cc`)
holds `Schema` + one `AbstractFlexVectorBase` per column + the `SpaceMapper`:

| Method | Space | Note |
|--------|-------|------|
| `AddKeys(rows_key_space)` | key → index | reserves positions; data not yet present |
| `AddData(sources, begins, ends, rows_index_space)` | index | fills in the reserved positions; may be called in slices |
| `Erase(rows_key_space)` | key → index | returns the erased positions |
| `ConvertKeysToIndices(keys)` | key → index | for modifies |
| `ModifyData(col, src, begin, end, rows_index_space)` | index | per-column |
| `ApplyShifts(first, last, dest)` | key | closed range `[first,last]` moved to start at `dest` |
| `Snapshot()` | — | materializes a `ClientTable` (`MyTable`) from the current flex vectors |

`AddKeys` then `AddData` is a deliberate two-step: between them the mapping is ahead of the data.

`AbstractFlexVectorBase` (`private/.../immerutil/abstract_flex_vector.h`) type-erases
`immer::flex_vector<T>`: `Take`, `InPlaceDrop`, `InPlaceAppend`, `InPlaceAppendSource`,
`MakeColumnSource()`. `NumericAbstractFlexVector<T>` keeps one vector (nulls in-band);
`GenericAbstractFlexVector<T>` keeps a parallel `immer::flex_vector<bool>` of null flags.
Because immer vectors are persistent, `Snapshot()` is O(columns), and successive snapshots share
almost all their memory — that's what makes `TickingUpdate`'s seven table pointers affordable.

`ShiftProcessor::ApplyShiftData` (`ticking/shift_processor.{h,cc}`) walks the three transposed shift
`RowSequence`s in the correct direction (forward or backward, to avoid clobbering) and invokes a
callback per shift triple.

---

## 11. Interop C ABI and downstream consumers

Note that this ABI was initially written to support a C# client that would do native P/Invoke calls
to the C++ client. Later we decided to write a full native C# client. Accordingly, there are no
users of this ABI at present. It is possible that some existing client (for example the R client)
would be better served by using the C-style ABI.

`dhcore/include/public/deephaven/dhcore/interop/interop_util.h` +
`dhclient/include/public/deephaven/client/interop/*.h` define an `extern "C"` surface intended for
P/Invoke-style bindings (function names like `deephaven_client_TableHandleManager_EmptyTable`,
~98 entry points in `dhclient/src/interop/client_interop.cc`, plus `client_options_interop.cc` and
`update_by_interop.cc`). The building blocks are:

- `NativePtr<T>` — opaque heap pointer wrapper passed across the boundary.
- `InteropBool` — one-byte bool.
- `StringPool` / `StringPoolBuilder` / `StringHandle` / `StringPoolHandle` — batch string marshalling
  (bytes + end offsets, exported and destroyed in one call).
- `ErrorStatus::Run(callback)` — runs the body, converts any exception to a pooled string.
  Every interop function is expected to be `void` and report through `ErrorStatus*`.

`dhcore/src/interop/testapi/basic_interop_interactions.cc` exists solely to let a binding test its
marshalling. Note the in-repo `csharp/client/Dh_NetClient` is a *pure managed port*, not a consumer
of this ABI; keep the ABI compiling regardless.

Other consumers of `dhcore` you can break:

- **`py/client-ticking`** (Cython, `src/pydeephaven_ticking/_core.pxd` / `_core.pyx`) links
  `dhcore_static` and declares extern types from `chunk.h`, `container.h`, `row_sequence.h`,
  `client_table.h`, `schema.h`, `ticking.h`, `buffer_column_source.h`, `column_source.h`,
  `column_source_helpers.h`, `types.h`, `cython_support.h`, `barrage_processor.h`.
  `dhcore/include/public/deephaven/dhcore/utility/cython_support.h` exists purely for it:
  `CythonSupport::Create{Boolean,String,DateTime,LocalDate,LocalTime}ColumnSource` from raw
  pointer/validity buffers, `SlicesToColumnSource`, `ContainerToColumnSource`, `ColumnSourceToString`.
  **Changing any of those signatures requires a matching change in `_core.pxd`.**
- **`R/rdeephaven`** links `-ldhclient -ldhcore` (see `R/rdeephaven/src/Makevars`).

---

## 12. Conventions, style, gotchas

Formatting/naming (`.clang-format`: Google style, indent 2; `.clang-tidy` naming rules):

| Kind | Convention |
|------|------------|
| namespaces | `lower_case`, deeply nested (`deephaven::dhcore::ticking`) |
| classes, structs, methods, free functions | `CamelCase` |
| variables, parameters | `lower_case` |
| private/protected members | `lower_case_` or `camelCase_` — both appear; trailing `_` is the rule |
| enum constants, constexpr/global/static constants | `kCamelCase` |

Idioms you will see everywhere and should imitate:

- **Private-tag constructors**: `struct Private {};` + a public constructor taking it + a static
  `Create(...)` factory returning `shared_ptr`. Lets `make_shared` work while keeping construction
  controlled.
- **`[[nodiscard]]`** on essentially every value-returning method.
- **`DEEPHAVEN_LOCATION_EXPR(x)`** expands to `(DebugInfo{...}, x)` — pass it as the first argument to
  `OkOrThrow` / `ValueOrThrow` / `VerboseCast` / `TrueOrThrow`. `DEEPHAVEN_LOCATION_STR(msg)`
  produces a decorated string for `std::runtime_error`.
- **`VerboseCast<T*>(DEEPHAVEN_LOCATION_EXPR(p))`** instead of bare `dynamic_cast` — throws with the
  demangled actual type on failure.
- **`MakeReservedVector<T>(n)`** instead of `reserve` boilerplate.
- **fmt** is vendored and re-exported: include `deephaven/third_party/fmt/format.h` (never a system fmt).
  Types with `operator<<` opt in via `fmt::ostream_formatter` specializations (see the bottom of
  `row_sequence.h`).
- Visitor pattern for type dispatch: `ColumnSourceVisitor`, `ChunkVisitor`, `ContainerVisitor`,
  `arrow::TypeVisitor`/`ArrayVisitor` wrappers in `arrowutil/arrow_visitors.h`.
- Destructors that do network work (`~Client`, `~TableHandleImpl`) catch and log everything.

Gotchas:

- Adding a source file requires editing the module's `CMakeLists.txt` `ALL_FILES` list (headers are
  listed too, for IDE integration). There is no globbing.
- `-Werror` — an unused parameter or a signed/unsigned comparison will fail the build.
- Public headers must not leak protobuf/gRPC; keep those in `include/private` or in `.cc` files.
- `dhcore` must not gain an Arrow/gRPC dependency (§1).
- `NumRows()`/`IsStatic()` on `TableHandle` are creation-time values from the RPC response.
- The `on_close` callbacks run *before* teardown, so they may still issue RPCs.
- `TableHandleManager::InputTable(initial_table, keys)` creates the input table *and* immediately
  `AddTable`s the initial contents.
- `Aggregate::Count`, `aggCount`, `aggMax`, `aggMin`, `aggSum`, `aggCombo` are the surviving
  lowercase legacy spellings; the rest are `CamelCase`.
- Commented-out declarations mark unimplemented features (`InputTable(schema,…)`, `RangeJoin`).

---

## 13. Recipes

**Add a table operation** (e.g. a new join flavor):

1. Confirm the RPC exists in `proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/table.proto`.
2. Add `std::shared_ptr<TableHandleImpl> Foo(...)` to `impl/table_handle_impl.h` and implement it in
   `src/impl/table_handle_impl.cc` following the §6 shape.
3. Add the public `TableHandle::Foo(...)` in `include/public/deephaven/client/client.h` (with a
   doxygen comment and a variadic overload if it takes column lists) and the thin forwarder in
   `src/client.cc`.
4. Add a test in `tests/src/` (new file → add it to `tests/CMakeLists.txt`), using
   `TableMakerForTests` + `TableComparerForTests`.
5. Consider `dhclient/src/interop/client_interop.cc` if the .NET ABI should expose it.

**Add a new column element type**: `ElementTypeId::Enum` + `kEnumSize` + `kHumanReadableConstants`
(`types.h`, `src/types.cc`) → `DeephavenTraits<T>` → `TypeToChunk<T>` and a chunk alias →
`ColumnSourceVisitor::Visit` overload and every implementer → `ContainerVisitor` →
`column_source_helpers.h` names → the array/buffer/immer/arrow column source families →
`ArrowUtil::GetElementType`/`GetArrowType` → `MakeFlexVectorFromType` in `immer_table_state.cc` →
`TableMaker`'s `ColumnBuilder<T>` → Cython `_core.pxd`. It is a wide change; grep an existing type
such as `kLocalTime` to enumerate the sites.

**Debug a ticking problem**: instrument `AwaitingMetadata::ProcessNextChunk` (there is a disabled
`fmt::print` of adds/removes/shifts behind `if (false)`), and check the invariant throws in
`AwaitingAdds`/`AwaitingModifies`/`BuildingResult` — "excess data in the chunk" and "leftover
caller-provided data" both mean the client's phase accounting disagrees with the server's message
framing. Compare against the Java writer (`extensions/barrage`) and the parallel implementations in
`csharp/client/Dh_NetClient/ticking/BarrageProcessor.cs` and `py/client-ticking`.

**Change auth/headers**: `ClientOptions` (`src/client_options.cc`) → `Server::CreateFromTarget` /
`Server::ForEachHeaderNameAndValue` → `BearerMiddleware`. Remember the three hand-rolled
`FlightCallOptions` sites that intentionally skip `authorization` / `envoy-prefix`
(`FlightWrapper::AddHeaders`, `TableHandleImpl::Schema`, `SubscribeState::InvokeHelper`).

**Regenerate proto stubs**: run `proto/proto-backplane-grpc/src/main/proto/build-cpp-protos.sh`
(see the tail of `cpp-client/README.md`); the build otherwise generates them into the build tree.

---

## 14. File index

### dhclient

| File | Summary |
|------|---------|
| `include/public/.../client.h` | the entire user-facing table API |
| `src/client.cc` | thin pimpl forwarders; `Client::Connect`, `Close`, `ToArrowTable`/`ToClientTable`, ostream adaptors |
| `include/public/.../client_options.h`, `src/client_options.cc` | auth (default/basic/custom), TLS material, session type, gRPC channel options, extra headers |
| `include/public/.../flight.h`, `src/flight.cc` | `FlightWrapper`: DoGet reader for a handle, header injection, raw `FlightClient` |
| `include/public/.../update_by.h`, `src/update_by.cc` | UpdateBy operation builders → `UpdateByRequest::UpdateByOperation` protos |
| `include/private/.../server/server.h`, `src/server/server.cc` | stubs, channel, ticket allocation/release, `SendRpc`, keepalive, shutdown |
| `include/private/.../server/server_shared_state.h`, `src/server/server_shared_state.cc` | state shared with the Flight middleware |
| `src/server/bearer_middleware.cc` | Flight client middleware for bearer-token auth |
| `include/private/.../impl/client_impl.h`, `src/impl/client_impl.cc` | optional console session start, on-close callbacks, shutdown ordering |
| `include/private/.../impl/table_handle_manager_impl.h`, `src/impl/table_handle_manager_impl.cc` | root table creation, script execution, subscription registry, `MakeScopeReference` |
| `include/private/.../impl/table_handle_impl.h`, `src/impl/table_handle_impl.cc` | every table op as an RPC; lazy `Schema()`; `Subscribe` |
| `include/private/.../impl/aggregate_impl.h`, `src/impl/aggregate_impl.cc` | wrappers over `ComboAggregateRequest::Aggregate` |
| `include/private/.../impl/update_by_operation_impl.h`, `src/impl/update_by_operation_impl.cc` | wrapper over the UpdateBy proto |
| `include/private/.../impl/util.h` | `MoveVectorData` (vector → repeated proto field) |
| `src/subscription/subscribe_thread.cc` | DoExchange setup, `UpdateProcessor` thread, `UnwrapList` |
| `include/private/.../subscription/subscription_handle.h` | the `Cancel()` interface returned to users |
| `src/arrowutil/arrow_array_converter.cc` | Arrow array ⇄ `ColumnSource`, dictionary/run-end decoding |
| `src/arrowutil/arrow_client_table.cc` | `ClientTable` over an `arrow::Table` |
| `include/private/.../arrowutil/arrow_column_source.h` | `GenericArrowColumnSource` + `ArrowProcessingStyle` + `ScaleFromUnit` |
| `include/private/.../arrowutil/arrow_visitors.h` | `ArrowTypeVisitor` / `ArrowArrayTypeVisitor` adapters to templated lambdas |
| `include/public/.../utility/table_maker.h`, `src/utility/table_maker.cc` | local table construction + Flight DoPut |
| `include/public/.../utility/arrow_util.h`, `src/utility/arrow_util.cc` | type/schema conversion, `OkOrThrow`/`ValueOrThrow`, ticket→FlightDescriptor |
| `include/private/.../utility/executor.h`, `src/utility/executor.cc` | single-thread work queue |
| `src/utility/logging.cc` | Abseil log verbosity init |
| `src/interop/*.cc`, `include/public/.../interop/*.h` | `extern "C"` ABI (§11) |

### dhcore

| File | Summary |
|------|---------|
| `include/public/.../types.h`, `src/types.cc` | `ElementTypeId`, `ElementType`, null constants, `DeephavenTraits`, `DateTime`/`LocalDate`/`LocalTime` |
| `include/public/.../chunk/chunk.h`, `chunk_traits.h`, `chunk_maker.h` | typed buffers, `TypeToChunk`, chunk allocation |
| `include/public/.../container/row_sequence.h`, `src/container/row_sequence.cc` | interval-based row key sets, iterator, builder |
| `include/public/.../container/container.h`, `container_util.h` | list-valued cells |
| `include/public/.../column/column_source.h` | the `ColumnSource` interfaces + visitor |
| `include/public/.../column/{array,buffer,container}_column_source.h` | owned / borrowed / list-valued implementations |
| `include/public/.../column/column_source_helpers.h`, `column_source_utils.h` | human-readable type names, range assertions |
| `include/public/.../clienttable/schema.h`, `client_table.h` (+ `src/`) | schema and the abstract client table + pretty-printing |
| `include/public/.../ticking/ticking.h`, `src/ticking/ticking.cc` | `TickingCallback`, `TickingUpdate`, `OnDemandState` |
| `include/public/.../ticking/barrage_processor.h`, `src/ticking/barrage_processor.cc` | subscription request creation + the four-state cycle machine |
| `include/private/.../ticking/immer_table_state.h`, `src/ticking/immer_table_state.cc` | the local table state; `MyTable` snapshot; `MakeFlexVectorFromType` |
| `include/private/.../ticking/space_mapper.h`, `src/ticking/space_mapper.cc` | key space ⇄ index space over a Roaring64 bitmap |
| `include/private/.../ticking/index_decoder.h`, `src/ticking/index_decoder.cc` | `DataInput`, `ReadExternalCompressedDelta` |
| `include/private/.../ticking/shift_processor.h`, `src/ticking/shift_processor.cc` | ordered application of shift triples |
| `include/private/.../immerutil/abstract_flex_vector.h`, `immer_column_source.h` (+ `src/`) | type-erased persistent vectors and their column sources |
| `include/public/.../utility/cython_support.h`, `src/utility/cython_support.cc` | column sources from raw buffers, for `py/client-ticking` |
| `include/public/.../utility/utility.h`, `time.h` (+ `src/`) | `DEEPHAVEN_LOCATION_*`, `VerboseCast`, `MakeReservedVector`, `separatedList`, base64, time formatting |
| `include/public/.../interop/**` | interop primitives + the marshalling test API |
| `flatbuf/deephaven/flatbuf/Barrage_generated.h` | generated Barrage flatbuffer accessors (do not edit) |

### tests / examples

`tests/src/` — one file per feature area (`basic`, `select`, `filter`, `join`, `aggregates`, `sort`,
`group`, `ungroup`, `merge_tables`, `head_and_tail`, `lastby`, `input_table`, `new_table`,
`add_drop`, `view`, `attributes`, `script`, `on_close_cb`, `string_filter`, `validation`,
`ticking`, `update_by`, `types`, `date_time`, `time_unit`, `encoding`, `buffer_column_source`,
`cython_support`, `table_test`, `utility_test`), plus `main.cc` (Catch2 runner) and
`test_util.{h,cc}` (fixtures/comparers).

`examples/` — `hello_world`, `read_csv`, `create_table_with_table_maker`,
`create_table_with_arrow_flight`, `read_table_with_arrow_flight`, `concurrent_client`,
`table_cleanup`, `demos/` (`chapter1`–`chapter3`, `feedtimes` — the ticking demos). Each is a
standalone CMake project linking `deephaven::client`; they double as end-to-end smoke tests.
