# Barrage shared writes for identical subscribers

How `BarrageMessageProducer` writes each propagated update once for every group of full
subscriptions that would be sent identical bytes, how it keeps their dictionaries consistent, what
runs in parallel, and the measurements behind it. Ticket: DH-23397.

Code: `server/src/main/java/io/deephaven/server/barrage/BarrageMessageProducer.java`,
`SharedMessageFanOut.java` beside it, and `BarrageMessageWriterImpl.java`.

## The problem

A producer writes each propagated update to each of its subscribers. A subscriber written alone
gets its own `SubView` of the message, and encoding happens when that view is drained:
`MessageViewAdapter.onNext` calls `SubView.forEachStream`, which builds each batch and hands it to
gRPC on the same thread. N subscribers cost N view constructions, N batch loops and N `onWrite`
calls, even when every subscriber would be sent the same bytes. Production metrics showed it as a
`PropagateMillis` count of 1 against a `WriteMillis` count equal to the number of identical
subscribers.

Through gRPC, a full subscriber's write of a 16 MB update costs about 25 ms of CPU, of which about
1–2 ms is copying the finished bytes into the transport's buffers. The rest is encoding, which is
identical for every subscriber with the same options, direction and columns.

## Terms

- **Full subscription:** a subscription to the whole table rather than a viewport. **Stable** once
  it has received the whole table; until then it is **growing** and is sent only the rows it has.
- **Propagation phase:** one coalesced `BarrageMessage` written to the producer's subscribers. One
  propagation job runs up to four in order: deltas before a snapshot, a blink-table flush, the
  snapshot itself, and deltas after it.
- **Unit:** what one phase writes as one piece of work: a **group** of two or more stable full
  subscriptions sharing one write, or a single subscription written alone.
- **Member:** a subscription in a group.

## Data flow

```text
Record: update-graph thread, every cycle (step 1)
  DeltaListener.onUpdate → enqueueUpdate → pendingDeltas

Coalesce: propagation job, every update interval (step 2)
  updateSubscriptionsSnapshotAndPropagate → aggregateUpdatesInRange → one BarrageMessage per phase

Fan out: propagation job, once per phase, units in parallel (steps 3–6)
  propagateToSubscribers
  ├─ single subscription → propagateSingle → SubView → gRPC
  └─ group → propagateGroup
             ├─ SharedMessageFanOut.synchronizeDictionaries → catch-ups → each member → gRPC
             └─ one SubView → SharedMessageFanOut → each member → gRPC

Deliver: gRPC transport threads, then the client (step 7)
  gRPC transport → socket → client reader → BarrageTable
```

1. **Record.** On the update-graph thread, under the producer's monitor,
   `enqueueUpdate` copies the cycle's added and modified rows of the producer's active columns (the
   union of every subscription's columns) into pooled chunks, and appends a `BarrageMessageDelta`
   to `pendingDeltas`. `schedulePropagation` arranges the next propagation job at the update
   interval, and compaction may coalesce the queue before then (see `delta-coalescing.md`).
2. **Coalesce.** The propagation job applies subscription changes, takes a snapshot
   for growing subscriptions, and coalesces the pending deltas with `aggregateUpdatesInRange` into
   one `BarrageMessage` per phase. Chunks move from the deltas into the message without copying.
3. **Prepare the phase.** `propagateToSubscribers`:
   - resets any shared dictionary that has outgrown the table (`maybeResetSharedDictionaries`),
     single-threaded, before any write starts;
   - splits the targets into units (`groupPropagationTargets`, see [Grouping](#grouping));
   - builds one `BarrageMessageWriterImpl` for the message. Its row-set writers and per-chunk column
     writer contexts are built once and shared by every view; string payloads, for example, are
     serialized here, once;
   - runs the units in parallel (`forEachInParallel`).
4. **A single subscription.** `propagateSingle` calls
   `listener.onNext(bmw.getSubView(...))`. `MessageViewAdapter`, holding the gRPC stream's lock,
   drains the view: `processBatches` and `getInputStream` build each batch's column data, then the
   `DictionaryBatch` messages for each dictionary with new values (several when the values exceed a
   size limit; see [Large dictionary batches](#large-dictionary-batches)), then the `RecordBatch`,
   and hand each message straight to gRPC, which copies each message into the transport's buffers.
5. **A group.** `propagateGroup` hands the group to `SharedMessageFanOut.propagateToSubscribers`, which:
   1. `synchronizeDictionaries` works out, one dictionary id at a time, the catch-ups the members
      are owed (see [Dictionaries](#dictionaries)). It sends each through `deliverDictionaryToSubscribers`, which builds
      it once with `getDictionaryBatchView` (only if one of its members is still receiving) and
      delivers each of its messages to those members as it is written. It returns the group
      registry, a registry of the producer's shared dictionaries whose state for each id starts at
      the members' highest offset;
   2. one `SubView` is made for the group, with the group's columns and registry;
      `deliverMessageToSubscribers` drains it with a visitor that captures each message into
      an exact-size array and delivers the array to every member still receiving, in parallel when
      the round is large enough (see [Parallel copies](#parallel-copies)), before the writer builds
      the next message;
   3. once the write succeeds, `advanceSubscriberDictionaries` moves each member's dictionary states to
      the group registry's.
6. **After the phase.** Each write has recorded its metrics (see
   [Metrics and configuration](#metrics-and-configuration)). When every unit has finished, the
   writer closes and its contexts return the chunks to the pool.
7. **Transport and client.** gRPC sends its queued buffers to each socket. The client reader
   handles each FlightData message separately: dictionary batches update its dictionaries in order,
   and record batches are assembled into one `BarrageMessage` that `BarrageTable` applies.

What a group member that was behind receives for one update, for a table with a dictionary column
`D`:

```text
1. DictionaryBatch D, delta, values [offset, H)     catch-up, only for a member behind
2. DictionaryBatch D, delta, values [H, e1)         shared: the values batch 1 adds
3. RecordBatch 1, with the update's metadata        shared
4. DictionaryBatch D, delta, values [e1, e2)        shared, if batch 2 adds values
5. RecordBatch 2 ...                                shared
```

Every member sees messages 2 onward identically.

## Grouping

Every stable full target of a phase is given a `SubViewKey`, and the targets with equal keys form
one unit. A stable full subscription (`viewport == null`, `snapshotViewport == null`,
`!isGrowingViewport`, and a dictionary registry, which every stable full subscription has, backed by
the producer's shared dictionaries) has a key that holds the arguments of `getSubView` that can
differ between such subscriptions. That is everything that decides the bytes of a full view of a
propagated message:

| Part | Why it decides the bytes |
| --- | --- |
| `effectiveOptions(options)` | Batch size, maximum message size, null encoding, `columnsAsList` and the list preview limit all change the encoding. `BarrageSubscriptionOptions` is an Immutables value, so `equals` covers them. It also covers `minUpdateIntervalMs`, which doesn't change the bytes; see [Decisions recorded](#decisions-recorded) |
| `reverseViewport` | Reported in every update's metadata |
| `subscribedColumns` | A view writes an empty field node and no modified rows for every column its subscription leaves out |

Any other subscription's view also depends on its own state: a viewport subscription is sent only
its viewport's rows, a growing one only the rows it has so far, and one with a snapshot viewport is
sent the deltas before this cycle's snapshot as its previous viewport, columns and direction see
them. Each of those is a unit by itself. When grouping is off, no keys are made and every
subscription is a unit by itself. A unit of one takes the per-subscriber path.

Groups are rebuilt on every phase from the phase's targets, so there is no group state to keep in
step with subscriptions being added, removed or finishing growth. Units are sorted largest first,
so the longest writes start first.

## Streaming delivery

The `MessageView` API offers only `forEachStream`. To hand message k to every member before
message k+1 is encoded, each member is given one small view per message, over the captured array,
rather than one per update. A member receives the same FlightData messages a subscriber written
alone does, because `MessageViewAdapter` forwards each stream as its own message either way. An
`onError` from another thread can arrive between two messages of one update; the stream ends
either way.

Each captured message gets a new array of exactly `available()` bytes. A reused buffer would rely on
every listener draining before `onNext` returns, which the `StreamObserver` contract doesn't
promise; gRPC does drain synchronously, so each array is garbage as soon as its delivery loop ends.

**Memory.** A group holds one captured message at a time, never a whole update:

- Without dedup, the large copies are already inside gRPC, one per subscriber, queued until each
  socket takes them: heap byte arrays on Jetty, pooled direct buffers on Netty. A thread encoding a
  view holds only bookkeeping, because the writer contexts are shared.
- A group keeps the same N gRPC copies and adds one staging copy while it is encoding. When sockets
  are behind, that adds about 1/N to what is already queued.
- A staging copy can be as large as a record batch, and batches grow to about 90% of
  `maxMessageSize`, so about 90 MB at the 100 MB default. See [Open questions](#open-questions).

### Parallel copies

A round of copies, one captured message handed to every member still receiving, runs in parallel
when it is large enough to pay for handing the copies out:

- **When.** A round goes parallel when the message's size, multiplied by one less than the number
  of members it goes to, reaches `BarrageMessageProducer.parallelCopyMinBytes`, 4 MiB by default.
  That is what the copies would move off the group's thread, which copies one share itself. A
  smaller round is copied one member after another on the group's thread. See
  [Parallel copy threshold](#parallel-copy-threshold) for how the default was chosen.
- **Where.** The copies run through `invokeParallel` on the phase's propagation scheduler, the same
  one its units run on. So they draw on the same `propagationThreads - 1` helpers, which every
  producer shares, and the group's thread takes part. When the pool is busy, a refused copy runs on
  the group's thread, so a busy pool means fewer copies at once, never a wait.
- **Subject to `propagationThreads`.** With `BarrageMessageProducer.propagationThreads` of 1 or
  less, the scheduler is an `ImmediateJobScheduler` and every copy runs on the group's thread. That
  one property turns off parallel writes to subscribers and parallel copies together; there is no
  separate switch.
- **Order and memory.** A round finishes before the next message is encoded, so each member still
  receives its messages in order, and a group still holds one captured message at a time.
- **Failures.** Members of one round can fail on different threads at once, so `failSubscriber` is
  synchronized. A member whose delivery fails is still failed alone.
- **Cost of a busy pool.** A refused hand-off builds the helper pool's `RejectedExecutionException`,
  with a formatted message and a stack trace, on the order of 10 µs each, up to one fewer than the
  helpers a round asks for. Next to a round of at least 4 MiB, about 0.5 ms of copying, that is
  small. See [Open questions](#open-questions).

## Dictionaries

Full subscriptions share one `SharedWriterDictionary` per dictionary id, so their RecordBatches use
the same indices. Each subscription's own `SharedDictionaryWriterState` records how many of the
dictionary's values it has been sent (`flushedOffset`), whether its next batch must replace the
dictionary (`needsFullBatch`), and the dictionary generation it last saw. It is sent other
subscribers' values only in a batch whose own rows reference the dictionary (`referenced`).
Members of one group subscribe to the same columns, so they reference the same dictionaries; what
can differ between them is only their offsets.

`SharedMessageFanOut.synchronizeDictionaries` brings the members level before the shared write. For each dictionary
id in the members' registries, let H be the highest offset any member has reached:

| Member's state | Catch-up sent before the shared write |
| --- | --- |
| At H | None |
| Below H, and has received a batch | Delta of values [offset, H) |
| Needs a full batch, and H > 0 | Replacement of values [0, H) |
| Needs a full batch, and H = 0 | None: the group's first batch replaces the dictionary |

The group's registry then starts at H, so the shared write carries only values beyond H, which are
the same for every member. After a successful write, `advanceSubscriberDictionaries` moves every member's state to the
group's position with `SharedDictionaryWriterState.advanceTo`, which also clears `referenced`, as an
accepted batch does.

Rules:

- **Never start below H.** A member already holding the values in [lowest offset, H) would append
  them again and every later index would refer to the wrong value.
- **Catch-ups come first.** Arrow requires a dictionary before any RecordBatch references it, so
  `propagateGroup` delivers every catch-up before the shared view is requested.
- **The range below H is stable while other units write.** No offset exceeds the dictionary's size
  within a generation, so every value below H exists; `synchronizeDictionaries` checks it. Resets happen only in
  the single-threaded step before a phase, and values are only appended, so [0, H) stays fixed while
  other groups and single subscriptions add to the same dictionary in parallel.

Catch-ups are rare. Members of a group advance together, so in steady state there are none. They
appear when a member joins a group behind or ahead of it, for example after its snapshot added
values to a dictionary that a reset had emptied, or when parallel writes in an earlier phase built
members' deltas at different moments. After a reset every offset is 0, so none are needed. Members
owed the same values share one catch-up, built once.

A catch-up is built by `BarrageMessageWriter.getDictionaryBatchView`, which frames a standalone
`DictionaryBatch` the way the writer frames its other messages: FlightData from `Factory`, IPC from
`ArrowFactory`. The view shares `buildDictionaryBatchStreams` with the regular path, so a catch-up is
byte for byte the batches the stream would have carried. The method is abstract, so an
implementation without the method fails to compile rather than failing group members at runtime.

### Large dictionary batches

A client is likely to reject a message over its inbound limit, so a dictionary's pending delta is
held to `maxMessageSize` as record batches are. A delta over the limit is split into several
batches under it: the first keeps the replacement or delta flag, and the rest are deltas. A batch
also carries no more values than the subscription's batch size, as a record batch carries no more
rows. That
covers both the regular path and the catch-ups, so a catch-up can be several messages, delivered in
order.

## What runs in parallel

The work around one propagated update runs as follows.

| Work | Runs on | Runs at the same time as | Serialized by |
| --- | --- | --- | --- |
| Recording a delta (`enqueueUpdate`) | The update-graph thread | The producer's propagation job while it writes | The producer's monitor, held by the job only while it applies subscription changes and coalesces |
| Compacting pending deltas | The server scheduler | The update-graph thread appending deltas | The job's run lock (`tryLock`, so it never waits for a propagation) and the monitor for the splice |
| A producer's propagation job | The server scheduler's concurrent pool (`scheduler.poolSize`) | Other producers' jobs, for other tables or update intervals | The job's run lock: one job per producer at a time |
| The phases of one job | The job's thread | Nothing else in the same job | Order: pre-snapshot deltas, blink flush, snapshot, post-snapshot deltas, each finished before the next starts |
| The units of one phase | The propagation scheduler: the job's thread plus up to `BarrageMessageProducer.propagationThreads - 1` helpers from a pool every producer shares | Each other: groups and single subscriptions | `forEachInParallel` returns only when every unit has finished. A refused helper job runs on the job's thread, so a busy pool means fewer units at once, never a wait |
| A snapshot phase's writes | The same propagation scheduler | Each other, one per growing subscription | As for units |
| Inside a group: catch-ups and the shared encode | The unit's one thread, in turn | Nothing else in the group | The group's thread. Each message is captured and copied to every member before the next is encoded |
| Inside a group: copying one message to its members | The group's thread, plus propagation helpers when the round reaches `parallelCopyMinBytes` | Each other; the group's thread waits for the round | `invokeParallel` returns only when every copy of the round has run. A refused helper job runs on the group's thread |
| Inside a single subscription: encoding and handing to gRPC | The unit's one thread | Nothing else for that subscription | The gRPC stream's lock in `MessageViewAdapter` |
| Filling a shared dictionary | Whichever unit's thread writes a batch referencing it | Other units filling or copying the same dictionary | `SharedWriterDictionary`'s monitor, held once per chunk filled and once per batch copied; its size is a volatile field read without the lock |
| A member's dictionary states | Only its own unit's thread | Nothing | A member belongs to exactly one unit per phase |
| Sending queued bytes to sockets | gRPC's transport threads: Netty's event loop, or Jetty's asynchronous writer | The producer's next messages and phases | The transport's own queues |

What this means in practice:

- **Across tables, everything can overlap.** Each producer's job is independent, and the
  propagation helpers are shared, so a busy table cannot monopolize them beyond its own phases.
- **Within a phase, parallelism is by unit, and within a group by copy.** N identical subscribers
  form one unit, whose encode runs on one thread while its copies can spread over the propagation
  helpers. N subscribers written alone would instead spread N encodes over up to
  `propagationThreads`. The group's work is about one encode plus N copies, instead of N encodes.
- **Each member's messages stay in order,** because a group finishes copying message k to every
  member, on whichever threads, before encoding message k+1, and each member gets one copy a
  round.
- **A split dictionary batch stays on its unit's thread.** Its messages are delivered in order,
  like the rest of the unit's.
- **Copies share the helpers with units.** A group's copies and the phase's other units draw on
  the same pool. A phase of many units leaves fewer helpers for copies, and a single large group can
  use all of them. `propagationThreads` bounds both.

## Failure handling

- **A failure writing the shared message** (in `synchronizeDictionaries`, `getSubView` or `forEachStream`, not in
  a member's `onNext`) fails every member still receiving: each gets `onError` and is removed. There
  is no retry: the members share byte-identical inputs, so it would fail again. Member dictionary
  states are not moved.
- **A failure delivering to one member,** whether a catch-up or a shared message, fails only that
  member. It gets nothing more; the others carry on.
- **A catch-up that fails to build** fails only the members that needed it.
- **Failures on different threads.** With parallel copies, members of one round can fail at the
  same time on different threads. `failSubscriber` is synchronized, so each is failed once and
  reported alone.
- **Once every member has failed,** the visitor throws a private sentinel to stop the encode, and
  `deliverMessageToSubscribers` catches it. `BarrageMessageWriterImpl.visitMessages` closes the messages of the
  batch it had not yet handed over, so stopping early leaks no pooled chunks. It closes them the
  same way on the per-subscriber path when a delivery fails part way through a batch.

## Metrics and configuration

Each view calls `onWrite` once, when its write ends, so a write records one `WriteNanos` sample:
the elapsed time of the write, including copying it to every member, in parallel or not. The same
call records one `WriteBytes` sample, the size of one copy, counting every message the write
sends, dictionary batches included. When a group's write ends, the fan-out records one more
`WriteBytes` sample of that size for each other member still receiving, through the producer's
`recordCopyBytes`. Each catch-up is a write of its own, recorded the same way, with its time
running from building it to delivering it to the members owed it. So the `WriteNanos` count is
the number of writes, and the `WriteBytes` count is the number of deliveries: one for each write
or catch-up a member was sent.

A member that fails during a write isn't counted for it, and a write abandoned because every
member failed records nothing. A write whose remaining members all fail during its last message
still records its own sample, since the view records it before the fan-out can see the failures.

| Property | Default | Effect |
| --- | --- | --- |
| `BarrageMessageProducer.dedupIdenticalSubscribers` | `true` | When `true`, identical stable full subscriptions share one write. When `false`, every subscription is written alone, on every table |
| `BarrageMessageProducer.parallelCopyMinBytes` | `4194304` (4 MiB) | A round of copies runs in parallel when the message's size, multiplied by one less than the number of its members, reaches this. A very large value copies every round serially |
| `BarrageMessageProducer.propagationThreads` | The number of available processors | Bounds parallel writes to subscribers and parallel copies within groups together. 1 or less turns both off |

The one property covers tables with dictionary encoded columns too: dedup applies to every
subscriber or to none.

## Client compatibility

A catch-up followed by the shared delta carries the same values as one delta, split across two
messages, so a member can receive two `DictionaryBatch` messages for one id back to back before a
RecordBatch. The Arrow format allows it, and the readers apply each batch on its own:

| Client | How it reads dictionary batches | Checked |
| --- | --- | --- |
| Java | `BarrageMessageReaderImpl` applies each through `DictionaryReaderRegistry.update`: a delta appends, a replacement clears and appends | By the server round-trip tests |
| JS | `WebBarrageMessageReader.parseFrom` does the same, through the same registry | By reading the code |
| C++ and R | Arrow's `FlightStreamReader` and IPC dictionary handling | With pyarrow 25.0.1, on spliced streams: two deltas back to back, and a replacement then a delta, both decode correctly. Through the C++ client's own Arrow build, `encoding_test.cc` decodes split replacements, split catch-ups and the deltas that follow them |
| Python ticking | pyarrow's `FlightStreamReader` | As for C++ |
| C# | Apache.Arrow.Flight 23.0.0 | Not checked, including whether it reads dictionary deltas at all |

## Measurements

JMH `AverageTime`, one fork per configuration, with nothing else loading the machine.

**Hardware**: Apple M1 Max, 10 cores, 64 GiB, macOS 26.6.2 (arm64). **JVM**: OpenJDK 21.0.1
(`21.0.1+12-LTS`), G1. JMH 1.37. **Code**: this change, parallel copies included, on main
`4bc6b7bb1`, which includes DH-24095. Measured 2026-10-22.

### Through gRPC

`BarrageGrpcPropagateBenchmark.tick`: 1M rows of 30 mixed columns, including a dictionary encoded
and a run-end encoded column, 100,000 rows modified per tick, about 16 MB per subscriber, written
through a real gRPC Netty transport to clients on the loopback interface. `-Xmx12g`, 2×5 s warmup,
3×5 s measurement. Each update is two record batches: the writer's first, of 4,096 rows and about
0.66 MB, then the rest. It ran three times: with dedup off; with dedup on and `parallelCopyMinBytes`
at `Long.MAX_VALUE`, so every round of copies is serial; and as shipped. The table reports the
producer's job time per tick, averaged over 6 to 54 invocations. The overall tick also includes the
clients receiving the bytes, on the same cores, and its confidence intervals are too wide to compare.
With one thread the scheduler runs every job inline, so both dedup-on runs copy serially there.

| Threads | Subscribers | Dedup off (ms) | Dedup on, serial copies (ms) | Dedup on (ms) | Speedup over dedup off | Parallel over serial copies |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 31.5 | 31.2 | 32.2 | 0.98x | 0.97x |
| 1 | 2 | 61.2 | 35.4 | 36.3 | 1.68x | 0.98x |
| 1 | 3 | 83.3 | 37.8 | 35.9 | 2.32x | 1.05x |
| 1 | 4 | 112.8 | 36.8 | 37.0 | 3.05x | 1.00x |
| 1 | 8 | 194.0 | 43.2 | 46.1 | 4.21x | 0.94x |
| 1 | 16 | 367.3 | 57.9 | 58.2 | 6.32x | 1.00x |
| 1 | 128 | 2642.1 | 297.5 | 261.6 | 10.10x | 1.14x |
| 8 | 1 | 31.8 | 31.3 | 31.8 | 1.00x | 0.99x |
| 8 | 2 | 32.3 | 37.0 | 32.2 | 1.00x | 1.15x |
| 8 | 3 | 37.7 | 37.5 | 33.4 | 1.13x | 1.12x |
| 8 | 4 | 33.2 | 36.5 | 32.2 | 1.03x | 1.13x |
| 8 | 8 | 39.1 | 43.2 | 33.9 | 1.15x | 1.28x |
| 8 | 16 | 77.5 | 57.8 | 38.4 | 2.02x | 1.51x |
| 8 | 128 | 584.7 | 303.4 | 123.5 | 4.74x | 2.46x |

### Parallel copy threshold

`BarrageGrpcPropagateBenchmark.tick` with dedup on and 8 threads, 1M rows, run once with
`parallelCopyMinBytes` at `Long.MAX_VALUE`, so every round of copies is serial, and once at `0`, so
every round is parallel. The table reports the producer's job time per tick, as in
[Through gRPC](#through-grpc). An update of more than 4,096 rows is two record batches, and each is
copied in a round of its own, so the table gives each round's size: the batch's size multiplied by
one less than the number of subscribers.

| Rows per tick | Subscribers | Copied off the group's thread, per round | Serial copies (ms) | Parallel copies (ms) | Speedup |
| --- | --- | --- | --- | --- | --- |
| 1,000 | 2 | 0.16 MB | 0.93 | 0.88 | 1.06x |
| 1,000 | 4 | 0.49 MB | 1.10 | 1.03 | 1.07x |
| 1,000 | 16 | 2.45 MB | 1.00 | 1.09 | 0.92x |
| 10,000 | 2 | 0.66 MB and 0.95 MB | 4.21 | 4.03 | 1.05x |
| 10,000 | 4 | 1.98 MB and 2.85 MB | 5.17 | 3.96 | 1.31x |
| 10,000 | 16 | 9.88 MB and 14.24 MB | 7.05 | 5.02 | 1.40x |
| 100,000 | 2 | 0.66 MB and 15.39 MB | 34.36 | 33.47 | 1.03x |
| 100,000 | 4 | 1.97 MB and 46.18 MB | 38.27 | 32.75 | 1.17x |
| 100,000 | 16 | 9.86 MB and 230.90 MB | 61.10 | 43.40 | 1.41x |

Above 4 MiB, every update with four or more subscribers gained, by 1.17x to 1.41x. With two
subscribers only one copy moves off the group's thread, and the gain was at most 6% at any size,
within the noise.

Below 4 MiB the results were small and mixed. The one loss was the 2.45 MB round of fifteen copies of
a 0.16 MB message, and the largest gain the 1.98 MB and 2.85 MB rounds of three copies each, so a
round's size alone does not separate them. Run again with three forks each, both came out within
the spread between forks:

| Rows per tick | Subscribers | Serial copies, per fork (ms) | Parallel copies, per fork (ms) | Speedup |
| --- | --- | --- | --- | --- |
| 1,000 | 16 | 0.96, 0.93, 1.08 | 1.07, 1.02, 1.01 | 0.95x |
| 10,000 | 4 | 4.26, 4.66, 4.75 | 4.65, 3.90, 3.95 | 1.10x |

So the default stays at 4 MiB: below it, parallel copies moved a round's time by about a tenth at
most, in either direction, and above it every update with four or more subscribers gained.

### What the numbers show

- **CPU per extra identical subscriber drops by about 90%.** On one thread, where job time is close
  to CPU time, each extra subscriber costs about 21–22 ms per update without dedup and about
  1.7–1.8 ms with it. The saving grows with the number of identical subscribers: at 128, a tick
  takes 2.6 s of the producer's time without dedup and 0.26 s with it.
- **One subscriber is not affected.** It takes the per-subscriber path in every run, so the
  differences in those rows are noise.
- **Parallel copies remove dedup's elapsed-time cost for small groups.** On 8 threads, writes
  without dedup overlap until subscribers outnumber threads, so dedup with serial copies was up to
  13% slower at 2–8 subscribers. With parallel copies dedup is never slower: 1.00–1.15x at 2–8
  subscribers, 2.02x at 16 and 4.74x at 128. Against serial copies, parallel copies are 1.12–1.15x
  faster at 2–4 subscribers, 1.28x at 8, 1.51x at 16 and 2.46x at 128.
- **Noise.** One fork per configuration is enough for effects of 2–10x, not for differences of
  10%. With one thread both dedup-on runs copy serially, so their ratio measures the noise: within
  6% up to 16 subscribers, and 14% at 128, where each run averaged only 6 to 26 invocations.

## Decisions recorded

| Decision | Why |
| --- | --- |
| Group by options, viewport direction and columns | Those are exactly what decide a full view's bytes; columns do because each view writes only its subscription's columns (DH-24095) |
| Compare whole options, `minUpdateIntervalMs` included | Comparing the options' `equals` covers any option added later that changes the bytes, where a hand-picked list that missed one would group subscribers needing different bytes. `minUpdateIntervalMs` is the one field that doesn't change them, and it rarely splits a group: producers are memoized per table and update interval, with an unset interval resolved to the default first, so subscriptions on one producer differ in it only when one leaves it unset and another sends the default explicitly |
| Rebuild groups every phase | No group state to keep in step with subscriptions; hashing the key is trivial next to an encode |
| Capture and deliver one message at a time | Bounds a group's extra memory to one message, never a whole update |
| A new array per message | `StreamObserver` doesn't promise a synchronous drain, so a reused buffer could be overwritten while a listener still holds it |
| Fail every member on a write failure | Members share byte-identical inputs, so a retry per member would fail the same way |
| Catch up to the highest offset, never down to the lowest | Starting lower would make members that hold those values append them again |
| Catch-ups before the shared write | Arrow needs a dictionary before a RecordBatch references it, and the range below H is stable then |
| `WriteNanos` once per write, `WriteBytes` once per delivery | The `WriteNanos` count falls from one per subscriber to one per group, which shows the dedup at work, while `WriteBytes` still reads as it would without sharing: one sample of each subscriber's bytes for each write it is sent |
| One switch, for every table | Dedup applies to every subscriber or to none; a separate mode for dictionary encoded tables would be one more configuration to test and support |
| `getDictionaryBatchView` abstract | A writer that lacks it fails to compile, rather than failing every member that needs a catch-up |
| Copy to members in parallel, above 4 MiB | Through gRPC on 8 threads, parallel copies make a group's write 1.12–1.15x faster than serial copies at 2–4 members, 1.51x at 16 and 2.46x at 128, and remove dedup's elapsed-time cost for small groups. Below 4 MiB a round gained or lost at most about 10%, within the noise of three forks |
| Measure a round as message size × (members − 1) | That is what moves off the group's thread, which copies one share itself. A size alone would keep a small message to many members serial, and would parallelize a large message to two members that gains only one copy |
| Copy on the phase's scheduler, passed unconditionally | `propagationThreads` of 1 then turns off parallel copies along with parallel writes, with no separate switch to configure |

## Open questions

- **Pipelining copies with the encode.** The group's thread could encode message k+1 while round k
  is still copying, helping with any copies of round k not yet claimed before it starts round k+1. It
  would hold two captured messages per group instead of one, and it helps only updates of many
  similar messages; the benchmark's updates are one or two messages, where parallel copies take
  nearly all the gain. Not planned.
- **Refused hand-offs.** `ExecutorJobScheduler.newHelperPool` keeps the `ThreadPoolExecutor`'s
  default `AbortPolicy`, so each job a busy pool refuses costs a `RejectedExecutionException` with a
  formatted message and a stack trace. A rejection without either would cut that cost for parallel
  copies and parallel writes to subscribers alike. It is engine code, not changed here.
- **The staging copy's size.** Capping the message size for grouped writes would bound it, and
  capturing into pooled 1 MB segments would avoid G1's humongous allocations. Neither is
  implemented.
- **Clients not tested.** The C++ client's `encoding_test.cc` subscribes through the client's own
  Arrow build to a table whose writes split their dictionary batches and send catch-ups. No Python
  ticking, C# or JS test exercises two dictionary batches for one id back to back, and the C#
  reader's support for dictionary deltas at all is unknown.

## Re-running

```bash
# through gRPC: run with dedup off; with dedup on and parallelCopyMinBytes=9223372036854775807, for
# serial copies; and with dedup on and the default threshold
./gradlew :extensions-barrage-benchmark:jmhRunBarrageGrpcPropagate --args="BarrageGrpcPropagateBenchmark.tick \
    -p subscribers=1,2,3,4,8,16,128 -p threads=1,8 -p rows=1000000 -p deltaRows=100000 -wi 2 -i 3 -w 5 -r 5 \
    -jvmArgsPrepend -DBarrageMessageProducer.dedupIdenticalSubscribers=false"

# the parallel copy threshold: run with parallelCopyMinBytes=9223372036854775807, then with 0;
# the three-fork check adds -f 3 and narrows the matrix to its two cases
./gradlew :extensions-barrage-benchmark:jmhRunBarrageGrpcPropagate --args="BarrageGrpcPropagateBenchmark.tick \
    -p subscribers=2,4,16 -p threads=8 -p rows=1000000 -p deltaRows=1000,10000,100000 -wi 2 -i 3 -w 5 -r 5 \
    -jvmArgsPrepend -DBarrageMessageProducer.parallelCopyMinBytes=9223372036854775807"
```

Pass each property with its own `-jvmArgsPrepend`: `-jvmArgsAppend` would replace the fork's own
arguments, including its heap size. Each run of the gRPC matrix takes about 9 minutes, and each run
of the threshold sweep about 5. Run nothing else on the machine meanwhile, and never run Gradle
against `:extensions-barrage-benchmark` while a JMH run of it is live: it rewrites the fork's
classpath.
