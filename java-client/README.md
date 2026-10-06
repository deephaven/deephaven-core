# Java client examples and integration tests

Notes for `java-client/*-examples`, `java-client/example-utilities`, and `java-client/integration-tests`,
for whoever changes them next, person or LLM. The rest of `java-client/` (the `session`, `flight`,
`barrage`, `uri` libraries and their `*-dagger` wiring) is not directly covered here.

## The examples

Three projects of command-line tools built on the Java client, split by which client layer they
use:

| project | client layer | what it shows |
|---|---|---|
| `session-examples` | `java-client-session` | sessions, consoles, tickets, publishing, fields, logs, plugin objects |
| `flight-examples` | `java-client-flight` | Arrow Flight DoGet, DoPut, DoExchange, input tables, aggregations |
| `barrage-examples` | `java-client-barrage` | Barrage snapshots and subscriptions into a client-side engine |

`example-utilities` holds the picocli option groups they share. Each project's `README.md` says how
to build and run its launchers; `integration-tests` runs every launcher against a server in Docker.

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

### Every command terminates

Every long-running example has a flag that bounds it, so it can run under the smoke test: `--cycles`,
`--rows`, `--updates`, `--count`, `--duration`, `--timeout`. The default is unlimited, so the tool
still works as a Ctrl-C tool. A new example that loops or blocks must have such a flag, and a smoke
row that uses it. Keep each bounded run to a few seconds.

### Adding or changing an example

1. Register a launcher in the project's `build.gradle` with `createApplication('name', 'Class')`. The
   script name and the `@Command` name should match.
2. Add a row to `ExamplesSmokeTest` in `integration-tests`: the script, its arguments, and a regex
   the stdout must match. Prefer asserting on something the example computed, not on a banner.
3. If the example needs a server-side fixture, add it to the `execute-code` setup in the test's
   `@BeforeAll`, with a `smoke_` prefix so it cannot collide with what examples publish.
4. Run `./gradlew :java-client-integration-tests:test` and `spotlessApply` on the project.

Removing an example: delete the file, its `createApplication` line, and its smoke row. Nothing else
references it. A `createApplication` line whose class does not exist still produces a script, and
nothing but the smoke test will notice.

## The integration tests

### The smoke test

`integration-tests` applies `io.deephaven.deephaven-in-docker`, which builds the jetty server image
(Python console, anonymous and pre-shared-key auth, key `deephaven`), starts it on a private Docker
network with its port published to a random host port, and removes it afterward. The test JVM runs on
the host under Gradle and spawns each launcher script as a child process with
`--target dh+plain://localhost:<port>`. The server container is the only thing in Docker.

- `./gradlew :java-client-integration-tests:test` runs it; it is also part of `check`, so CI runs it
  on every pull request. About 40 seconds when the image is cached, a few minutes when the server
  has to be rebuilt.
- Gradle considers the task up to date unless something it exercises changed: the example
  distributions, the server image (by content hash), the server's start options, the test classes,
  or the test runtime classpath, so a dependency bump reruns it on its own. The container still
  starts and stops on an up-to-date run; that is how the docker extension works.
- Each child's stdout and stderr land in `integration-tests/build/example-output/<script>.out` and
  `.err`. On failure the assertion message includes both, and Gradle prints the server log and a
  server thread dump.
- Each launcher runs on the test JVM's JDK, since the runner sets `JAVA_HOME` from it, so
  `-PtestRuntimeVersion` applies to the examples too.
- On JDK 25 the smoke test is skipped: the launchers' default JVM options include a flag JDK 25
  removed, so no example can start there (DH-23820). The other test classes still run on 25. The
  project applies `io.deephaven.java-netty-unsafe`, as the server and the `*-dagger` client tests
  do, because netty turns its Unsafe buffers off on 25 and Arrow's netty allocator fails on every
  Flight read without them; the example projects do not apply it yet, which is part of DH-23820.

The setup also builds a Figure from the static table, a plugin object the image supports, so
`fetch-object` has a row. `do-put-spray` runs with the same server given twice. Not covered, and
why: `message-stream-send-receive` needs a bidirectional plugin such as the echo plugin, and
`convert-to-table` needs an object type whose fetch carries exports but no payload bytes (a
Figure's fetch carries its descriptor, which the example rejects); the server image has neither.
`Example1..3`, `Sum`, and `SubscribeQST` have no launcher script.

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
- `PluginObjectTest`: the object service with a Figure built in the console. Fetching it by typed
  ticket returns its descriptor bytes, decoded as a `FigureDescriptor`, and the table it plots as
  an export, read back over Flight; fetching a plain table as a Figure fails with NOT_FOUND. This
  is the API under `fetch-object` and `convert-to-table`.
- `DoExchangeTest`: a Barrage snapshot request built by hand and sent over a raw DoExchange, to
  pin the wire format independently of the `BarrageSession` wrapper.

`TestServer` is the one place that knows the server's port and key. Tests open their own sessions
and close them in try-with-resources; one factory per class is shut down in `@AfterAll`. Server
variables they publish use an `api_` prefix.

### The transport tests

Nothing else in the repo runs grpc-netty against a real server, so `TransportTest` exists to catch
what a netty or gRPC bump breaks rather than what the client API does. A bump changes the test
classpath, so `check` reruns it without being asked:

- A DoGet of about 80MB in many record batches, checked for completeness and order, and the same
  table DoPut back: HTTP/2 flow control and both allocators on both sides.
- `maxInboundMessageSize` set too small fails the stream with RESOURCE_EXHAUSTED; the default
  carries the same table.
- Thirty-two DoGets and three hundred unary calls offered to one channel at once while a log
  subscription stays open on it. That is more streams than jetty allows at a time (128 by default),
  so the client's pending-stream queueing is exercised, not just multiplexing. If a CI runner
  cannot keep up, shrink the DoGet table before shrinking the counts.
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
- `add-to-input-table` builds its server-side validator through jpy from the Python console,
  because the validator is a server test utility with no public API. It is the one example tied to
  the console language.
