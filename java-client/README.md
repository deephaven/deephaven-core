# Java client examples and integration tests

Notes for `java-client/*-examples`, `java-client/example-utilities`, and `java-client/integration-tests`,
in three parts:

1. **Learning from the examples**: a reading order, how to build and run them, and the vocabulary
   they use. For someone picking up the Java client.
2. **Reference tools**: the commands that are useful to run but were not written to be read, and
   the rule for what counts as one. Readers use it to know what to skip; maintainers use it to
   know where things go.
3. **Maintaining the examples**: the file shape, the duplication policy, the termination rule,
   the add-or-remove checklist, the smoke test, and known pitfalls. For whoever changes them
   next, person or LLM.

The rest of `java-client/` (the `session`, `flight`, `barrage`, `uri` libraries and their `*-dagger`
wiring) is not directly covered here.

## Learning from the examples

### Start here

Each example is one file: a small command-line tool you can run against a server, and a worked
example you can read top to bottom. They split by which client layer they use, and each project
also has a `tools` package of things that are useful to run but were not written to be read (see
[Reference tools](#reference-tools)).

A suggested reading order. Each line is a file in `src/main/java/io/deephaven/client/examples/`
under the named project, and the launcher script it builds.

**session-examples** (`java-client-session`: sessions, consoles, tickets, publishing)

1. `ConnectCheck` (`connect-check`): open a session and print it. The connection recipe every other
   example repeats.
2. `PrintConfigurationConstants` (`print-configuration-constants`): a first round trip that returns
   data.
3. `ExecuteCode` (`execute-code`): run a line of Python in the server console and see what
   variables changed. `ExecuteScript` (`execute-script`) does the same for a file.
4. `FilterTable` (`filter-table`): take a table that exists on the server by ticket, derive a
   filtered table from it, and publish the result under a name.
5. `Publish` (`publish`): bind an existing table to a second name.
6. `CreateSharedId` (`create-shared-id`): share a table with other sessions by id while this one
   stays open.
7. `SubscribeToFields` (`subscribe-fields`): watch the server's named objects come and go.
8. `SubscribeToLogs` (`subscribe-to-logs`): drop below the session wrapper to a raw gRPC stub.
9. `FetchObject` (`fetch-object`): fetch a plugin object's bytes and exports. Needs a plugin on the
   server.

**flight-examples** (`java-client-flight`: Arrow Flight, bulk data in and out)

1. `GetTsv` (`get-tsv`): describe a table, execute it, read the rows back with DoGet, print TSV. The
   Flight recipe every other example repeats.
2. `GetDirectTable` (`get-table`) and `GetDirectSchema` (`get-schema`): DoGet an existing table by
   ticket, or just its schema by path.
3. `ListTables` (`list-tables`): what the server exposes as flights.
4. `StructuredFilter` (`structured-filter`): build a filter as objects instead of a string.
5. `PollTsv` (`poll-tsv`): a ticking table, read repeatedly. Compare the barrage subscription below.
6. `DoPutTable` (`do-put-table`): build a table client-side and upload it with DoPut. `DoPutNew`
   (`do-put-new`) round-trips a server table through DoGet and DoPut.
7. `KeyValueInputTable` (`kv-input-table`) and `AddToBlinkTable` (`add-to-blink-table`): input tables
   the client appends to, keyed and blink.
8. `DoExchange` (`do-exchange`): a Barrage snapshot built by hand over a raw DoExchange, to see
   what `BarrageSession` does for you.
9. `ConvertToTable` (`convert-to-table`): read the table a plugin object exports. Needs a plugin.

**barrage-examples** (`java-client-barrage`: ticking data into a client-side engine)

1. `SubscribeTable` (`subscribe-table`): subscribe to a ticking table and print each update as it
   arrives. Shows the client-side engine setup the barrage layer needs.
2. `SnapshotTable` (`snapshot-table`): consistent snapshots of a table, whole or by rows and
   columns, including through a subscription.

### Running

```shell
# Build the launchers
./gradlew :java-client-session-examples:installDist   # or flight, barrage
java-client/session-examples/build/install/java-client-session-examples/bin/connect-check --help

# Against a local server with anonymous auth
./gradlew server-jetty-app:run -Panonymous
java-client/session-examples/build/install/java-client-session-examples/bin/connect-check

# Against a different host, with a pre-shared key
.../bin/connect-check --target dh+plain://host:10000 --psk deephaven
```

### Vocabulary

The words the examples use without introducing them. The comments in the examples use the same
phrasing.

- **Session**: one authenticated login on a connection. Everything it exports on the server is
  released when it closes. `Session.close()` blocks until that has happened.
- **Factory**: holds the connection (a gRPC channel) and a scheduler, and opens sessions on it.
  The examples make one factory, open one session, and shut both down in `finally`.
- **Scheduler**: a thread pool the client uses for background work, such as refreshing the
  session token.
- **TableSpec**: a description of a table and the operations applied to it. Building one does
  nothing; the server executes it. `TableSpec.empty(10).view("I=i")` is a spec.
- **TableHandle**: the server-side export of an executed spec. Closing the handle releases the
  export. Most examples get one from `manager.execute(spec)`.
- **Ticket**: a name for a table the server already holds. The examples take one as `--variable`
  (a scope variable), `--app-id` plus `--app-field` (an application field), `--shared-id-hex`, or
  `--ticket` (raw bytes). `ticket.ticketId().table()` wraps it as a spec to build on.
- **Path**: a table's location by scope variable or application field, which is also how Flight
  lists and names it.
- **Publish**: bind an export to a scope variable, so it outlives the session and other clients
  can find it by name. A **shared id** is the same idea for a name only the holder's session keeps
  alive.
- **Batch and serial**: how a `TableHandleManager` sends a multi-operation spec. Batch sends the
  whole thing as one request; serial sends one operation at a time. `--batch` and `--serial` pick
  one; the default is the session's own choice.
- **Console**: a script session in the server's global scope. Its language, Python or Groovy, must
  match the server's. It reports the displayable variables a script created, updated, or removed.
- **FlightSession**: a `Session` plus an Arrow Flight client. **DoGet** streams a table's rows out
  as Arrow record batches; **DoPut** uploads batches as a new table; **DoExchange** is the
  bidirectional stream Barrage runs over.
- **Allocator**: Arrow memory for the batches Flight reads and writes. The examples make one
  `RootAllocator` per process.
- **NewTable**: data built client-side, column headers plus rows, ready to upload.
- **Input table**: a server table the client appends to. **Append-only** keeps every row,
  **key-backed** upserts by key, **blink** holds only the rows added in the current update cycle.
- **BarrageSession**: a `FlightSession` plus Barrage snapshots and subscriptions. A **snapshot** is a
  consistent copy of a table at one moment; a **subscription** keeps a client-side copy updated as
  the server's table ticks. Both land in client-side engine tables, which is why the barrage
  examples set up an update graph and an execution context.
- **Liveness scope**: the engine's reference counting. The barrage examples open one around each
  client-side table so it is released when the scope closes.
- **Field**: a named object the server exposes, either a scope variable or an application field.
  `subscribe-fields` watches them.
- **Plugin object**: a server object of a type registered by a plugin, addressed by a typed ticket
  such as `Figure:s/my_figure`. Tables are the one type the client can read without a plugin.

## Reference tools

Each project has an `io.deephaven.client.examples.tools` package for commands that are worth
running but were not written to be read: benchmarks, load generators, and walkthroughs of a
protocol detail. They follow the same file shape as the examples and are covered by the same smoke
test, but their javadoc starts with what kind of tool they are rather than what they teach.

| tool | project | what it is |
|---|---|---|
| `table-manager` | session | protocol: how many messages batch versus serial mode sends for a staged query |
| `unreferenceable` | session | protocol: a stateful table service refusing a second child of a non-deterministic parent |
| `message-stream-send-receive` | session | plugin: drive a bidirectional object stream such as the echo plugin |
| `deep-query` | flight | stress: a chain of hundreds of head and tail operations |
| `sum-benchmark` | flight | benchmark: one aggregation over a large empty table |
| `agg-by`, `aggregate-all` | flight | load: a key-backed input table with every aggregation published, updated at random |
| `do-put-spray` | flight | operations: copy a table from one server to others |
| `add-to-input-table` | flight | walkthrough: input table validation metadata and structured errors, through a server test utility |

## Maintaining the examples

### The shape of an example

One file per example, no abstract base classes. Top to bottom:

1. A class javadoc saying what the example demonstrates, in a sentence or two.
2. `@Command` with the name the launcher script uses.
3. The shared option groups: `ConnectOptions`, `AuthenticationOptions`, and where relevant
   `BatchOrSerialOptions`, `ScriptTypeOptions`, `Ticket`, `Path`, `SharedField`.
4. The example's own options and parameters.
5. `call()`: build the factory, open the session in a try-with-resources, do the interesting thing,
   shut the channel and scheduler down in `finally`.
6. `main`: `System.exit(new CommandLine(new X()).execute(args))`.

The factory and session setup is about ten lines and is repeated in every file on purpose. It is
what a reader came to see, and hiding it behind a helper or base class is what made the previous
version hard to read. Keep it inline, and keep the short comments on it; they are the vocabulary
above, applied. The three variants are:

```java
// session
SessionFactoryConfig.builder()
        .clientConfig(ConnectOptions.options(connectOptions).config())
        .sessionConfig(AuthenticationOptions.sessionConfig(authenticationOptions))
        .scheduler(scheduler)
        .build().factory();
// flight: as above plus .allocator(allocator), then factory.newFlightSession()
// barrage: as flight, then factory.newBarrageSession(), inside an ExecutionContext with the
//          DEFAULT PeriodicUpdateGraph (see SnapshotTable or SubscribeTable)
```

There are no shutdown hooks; Ctrl-C drops the connection and the server expires the session itself.

### Duplication, and what goes where

Duplication between example files is accepted when the duplicated code is part of what the example
teaches. Duplication is not accepted for command-line plumbing. Concretely:

- **Repeated in every file, keep it that way**: the factory and session setup with its comments,
  the `finally` block, `main`.
- **Repeated in a few files, keep it that way**: the DoGet-and-print-TSV loop in `GetTsv`,
  `StructuredFilter`, `tools/DeepQuery`, `tools/SumBenchmark`; the input-table setup and
  random-update loop in `tools/AggByExample` and `tools/AggregateAllExample`; the client-side
  engine setup in the two barrage examples. When one of these changes, change its siblings in the
  same commit. `grep -l` for the distinctive call (`contentToTSVString`,
  `InMemoryKeyBackedInputTable`, `existingOrBuild`) finds them.
- **Shared through `example-utilities`, do not inline**: picocli option groups and the one-line
  helpers that turn a possibly absent group into a client value. These are CLI concerns, not client
  API usage, and a reader does not learn anything from seeing `--psk` parsed. The current set is
  `ConnectOptions`, `AuthenticationOptions`, `BatchOrSerialOptions`, `ScriptTypeOptions`, `Ticket`
  and its four field variants, `Path`, the converters, and `ChangesFormatter` for console output.
  Classes the `tools` package uses must be public.
- **Do not add**: abstract example bases, a shared "run with session" helper, or anything in
  `example-utilities` that calls into the client beyond building a config value. If two examples
  start sharing real logic, that is a sign one of them should be a different example, or that the
  logic belongs in the client library itself.

picocli leaves an absent `@ArgGroup` as `null`. The helpers in `example-utilities` take the possibly
null group and return the default (`ConnectOptions.options`, `AuthenticationOptions.sessionConfig`,
`BatchOrSerialOptions.manager`). Use them rather than null-checking in each file.

### Every command terminates

Every long-running example has a flag that bounds it, so it can run under the smoke test: `--cycles`,
`--rows`, `--updates`, `--count`, `--duration`, `--timeout`. The default is unlimited, so the tool
still works as a Ctrl-C tool. A new example that loops or blocks must have such a flag, and a smoke
row that uses it. Keep each bounded run to a few seconds.

### Adding or changing an example

1. Decide whether it teaches something or is a tool, and put it in the matching package.
2. Write the file following the shape above, with the vocabulary comments on the setup.
3. Register a launcher in the project's `build.gradle` with `createApplication('name', 'Class')`. The
   script name and the `@Command` name should match.
4. Add a row to `ExamplesSmokeTest` in `integration-tests`: the script, its arguments, and a regex
   the stdout must match. Prefer asserting on something the example computed, not on a banner.
5. If the example needs a server-side fixture, add it to the `execute-code` setup in the test's
   `@BeforeAll`, with a `smoke_` prefix so it cannot collide with what examples publish.
6. Add it to the reading order or the tools table above.
7. Run `./gradlew :java-client-integration-tests:test` and `spotlessApply` on the project.

Removing an example: delete the file, its `createApplication` line, its smoke row, and its line
above. Nothing else references it. A `createApplication` line whose class does not exist still
produces a script, and nothing but the smoke test will notice.

### The smoke test

`integration-tests` applies `io.deephaven.deephaven-in-docker`, which builds the jetty server image
(Python console, anonymous and pre-shared-key auth, key `deephaven`), starts it on a private Docker
network with its port published to a random host port, and removes it afterward. The test JVM runs on
the host under Gradle and spawns each launcher script as a child process with
`--target dh+plain://localhost:<port>`. The server container is the only thing in Docker.

- `./gradlew :java-client-integration-tests:test` runs it; it is also part of `check`, so CI runs it
  on every pull request. About 40 seconds when the image is cached, a few minutes when the server
  has to be rebuilt.
- Gradle considers the task up to date unless the example distributions, the server image, or the
  test sources changed. `-PforceTest=true` reruns it anyway. The container still starts and stops on
  an up-to-date run; that is how the docker extension works.
- Each child's stdout and stderr land in `integration-tests/build/example-output/<script>.out` and
  `.err`. On failure the assertion message includes both, and Gradle prints the server log and a
  server thread dump.
- The child JVM is the test JVM unless `-Ddh.examples.javaHome=<path>` is passed through to the
  test; the launcher scripts honor `JAVA_HOME`.

The setup also builds a Figure from the static table, a plugin object the image supports, so
`fetch-object` has a row. `do-put-spray` runs with the same server given twice. Not covered, and
why: `message-stream-send-receive` needs a bidirectional plugin such as the echo plugin, and
`convert-to-table` needs an object type whose fetch carries exports but no payload bytes (a
Figure's fetch carries its descriptor, which the example rejects); the server image has neither.

### The API-level tests

The same project holds JUnit tests that use the client libraries directly, over the same server and
the same real netty channel, with assertions on what comes back rather than on example output:

- `SessionApiTest`: configuration constants, the Python console reporting created tables and
  errors, executing a spec and checking its size, publishing and resolving from a second session,
  shared ids, the field subscription, and the log subscription seeing console output.
- `FlightApiTest`: DoGet values, schema by path, listing, DoPut round trip, and the append-only,
  key-backed, and blink input tables. Input table changes land on the next update cycle, so the
  assertions poll DoGet with a deadline.
- `BarrageApiTest`: snapshots of a whole table, a viewport, and a reverse viewport into client-side
  engine tables, and a subscription that keeps ticking. The client-side engine needs the DEFAULT
  update graph and an execution context, and the test task runs with `dh-defaults.prop` rather than
  the test conventions' `dh-tests.prop`, because that file forbids starting the refresh thread.
- `AuthenticationTest`: anonymous accepted, the pre-shared key accepted, a wrong key rejected.

`TestServer` is the one place that knows the server's port and key. Tests open their own sessions
and close them in try-with-resources; one factory per class is shut down in `@AfterAll`. Server
variables they publish use an `api_` prefix.

### The transport tests

Nothing else in the repo runs grpc-netty against a real server, so `TransportTest` exists to catch
what a netty or gRPC bump breaks rather than what the client API does. It is the test to run after
such a bump, with `-PforceTest=true`:

- A DoGet of about 80MB in many record batches, checked for completeness and order, and the same
  table DoPut back: HTTP/2 flow control and both allocators on both sides.
- `maxInboundMessageSize` set too small fails the stream with RESOURCE_EXHAUSTED; the default
  carries the same table.
- Thirty-two concurrent DoGets and three hundred concurrent unary calls over one channel while a
  log subscription stays open on it.
- A cancelled stream and a DEADLINE_EXCEEDED call, each followed by proof the channel still works.
- The SSL provider matches the classpath: BoringSSL loaded if and only if a platform-classified
  `netty-tcnative-boringssl-static` jar is present. Today none is (Arrow's flight-core brings the
  classifier-less one, which has no natives), so the client's TLS runs on the JDK provider; a bump
  that changes either side of that shows up here. The netty artifact versions in use are printed
  into the report.
- The test JVM runs with `io.netty.leakDetection.level=paranoid`; `logback-test.xml` routes netty's
  leak reports into `NettyLeakRecorder`, and the class fails in `@AfterAll` if any were recorded or
  if the Arrow allocator still holds memory.

### The TLS tests

`TlsTest` runs under its own task, `testTls`, against a second container started with the
development certificates from `server/dev-certs` bind-mounted in, serving TLS on its port with
client certificates wanted but not required. The `deephavenDocker` extension manages one container
per project, so `build.gradle` registers that container's tasks by hand, mirroring the extension,
with the image's plaintext health check replaced by a TLS one. The tests cover a trusted-CA
connection, mutual TLS with the client certificate, Flight data over TLS, and two failures: the
JDK's default trust rejecting the self-signed server, and plaintext against the TLS port. The
`test` task excludes `*TlsTest*`; `check` runs both tasks.

### Things that have bitten

- A repeated `@ArgGroup` list is filled last-first by picocli. `Publish` relies on this and says so.
- The console reports changes to displayable variables only. `x = 1` prints nothing; a table does.
- The log subscription replays the server's recent history, so `subscribe-to-logs -c 1` returns on
  old messages. Pair `--count` with `--timeout` when the point is to wait for something new.
- `SessionImpl.publish` rejects names that are not Java identifiers. Derive publish names from
  constants, not from `toString()` of anything.
- A method reference to a generic `create(TableCreator<T>)` is ambiguous between
  `executeLogic(TableCreationLogic)` and the labeled overload; cast it to `TableCreationLogic`.
- `add-to-input-table` builds its server-side validator through jpy from the Python console,
  because the validator is a server test utility with no public API. It is the one example tied to
  the console language.
