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
   drains the view: `processBatches` and `getInputStream` build each batch's column data, then one
   `DictionaryBatch` for each dictionary with new values, then the `RecordBatch`, and hand each
   message straight to gRPC, which copies it into its transport buffers.
5. **A group.** `propagateGroup` hands the group to `SharedMessageFanOut.propagateToSubscribers`, which:
   1. `synchronizeDictionaries` works out, one dictionary id at a time, the catch-ups the members
      are owed (see [Dictionaries](#dictionaries)). It sends each through `deliverDictionaryToSubscribers`, which builds
      it once with `getDictionaryBatchView` (only if one of its members is still receiving) and
      delivers each of its messages to those members as it is written. It returns the group
      registry, a registry of the producer's shared dictionaries whose state for each id starts at
      the members' highest offset;
   2. one `SubView` is made for the group, with the group's columns and registry;
      `deliverMessageToSubscribers` drains it with a visitor that captures each message into
      an exact-size array and delivers the array to every member still receiving, before the writer
      builds the next message;
   3. once the write succeeds, `advanceSubscriberDictionaries` moves each member's dictionary states to
      the group registry's.
6. **After the phase.** Each view records `onWrite` once. When every unit has finished, the writer
   closes and its contexts return the chunks to the pool.
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
`ArrowFactory`. It shares `buildDictionaryBatchStream` with the regular path, so a catch-up is byte
for byte the batch the stream would have carried. The method is abstract, so an implementation that
lacks it fails to compile rather than failing group members at runtime.

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
| Inside a group: catch-ups, the shared encode and the copies to members | The unit's one thread, in turn | Nothing else in the group | The group's thread. Each message is captured, then copied to member 1, member 2 and so on, before the next is encoded |
| Inside a single subscription: encoding and handing to gRPC | The unit's one thread | Nothing else for that subscription | The gRPC stream's lock in `MessageViewAdapter` |
| Filling a shared dictionary | Whichever unit's thread writes a batch referencing it | Other units filling or copying the same dictionary | `SharedWriterDictionary`'s monitor, held once per chunk filled and once per batch copied; its size is a volatile field read without the lock |
| A member's dictionary states | Only its own unit's thread | Nothing | A member belongs to exactly one unit per phase |
| Sending queued bytes to sockets | gRPC's transport threads: Netty's event loop, or Jetty's asynchronous writer | The producer's next messages and phases | The transport's own queues |

What this means in practice:

- **Across tables, everything can overlap.** Each producer's job is independent, and the
  propagation helpers are shared, so a busy table cannot monopolize them beyond its own phases.
- **Within a phase, parallelism is by unit.** N identical subscribers form one unit and use one
  thread, where N subscribers written alone would spread over up to `propagationThreads`. That
  trades elapsed time for CPU: the group's work is about one encode plus N copies, instead of N
  encodes.
- **Each member's messages stay in order,** because a member is written only by its unit's thread,
  and a group copies message k to every member before encoding message k+1.
- **A split dictionary batch stays on its unit's thread.** Its messages are delivered in order,
  like the rest of the unit's.
- **Not parallel: the copies to a group's members.** They run one after another on the group's
  thread. [Open questions](#open-questions) describes how they could run in parallel and why that is
  not planned.

## Failure handling

- **A failure writing the shared message** (in `synchronizeDictionaries`, `getSubView` or `forEachStream`, not in
  a member's `onNext`) fails every member still receiving: each gets `onError` and is removed. There
  is no retry: the members share byte-identical inputs, so it would fail again. Member dictionary
  states are not moved.
- **A failure delivering to one member,** whether a catch-up or a shared message, fails only that
  member. It gets nothing more; the others carry on.
- **A catch-up that fails to build** fails only the members that needed it.
- **Once every member has failed,** the visitor throws a private sentinel to stop the encode, and
  `deliverMessageToSubscribers` catches it. `BarrageMessageWriterImpl.visitMessages` closes the messages of the
  batch it had not yet handed over, so stopping early leaks no pooled chunks. It closes them the
  same way on the per-subscriber path when a delivery fails part way through a batch.

## Metrics and configuration

The shared view calls `onWrite` once, so `WriteNanos` and `WriteBytes` are recorded once per write:
the count is the number of writes, `WriteBytes` is the size of one copy, and `WriteNanos` includes
copying to every member. Each catch-up records a write of its own, from building it to delivering it
to the members owed it. Every write's `WriteBytes` counts all the messages it sends, dictionary
batches included; before this change, a write with rows counted only its record batches.

| Property | Default | Effect |
| --- | --- | --- |
| `BarrageMessageProducer.dedupIdenticalSubscribers` | `true` | When `false`, every subscription is written alone, on every table |

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
| C++ and R | Arrow's `FlightStreamReader` and IPC dictionary handling | With pyarrow 25.0.1, on spliced streams: two deltas back to back, and a replacement then a delta, both decode correctly. The C++ client's own Arrow version was not tested |
| Python ticking | pyarrow's `FlightStreamReader` | As for C++ |
| C# | Apache.Arrow.Flight 23.0.0 | Not checked, including whether it reads dictionary deltas at all |

## Measurements

JMH `AverageTime`, lower is better. One fork per configuration; `±` is the 99.9% confidence interval
over that fork's iterations. Each pair ran with the property on and off, back to back, with nothing
else loading the machine: no tests or other benchmarks ran during either.

**Hardware**: Apple M1 Max, 10 cores, 64 GiB, macOS 26.6.2 (arm64). **JVM**: OpenJDK 21.0.1
(`21.0.1+12-LTS`), G1. JMH 1.37. **Code**: this change at `435a7ee82`, on main `4bc6b7bb1`, which
includes DH-24095. Measured 2026-10-22; runs on 2026-10-18 and 2026-10-21 agreed to within the
noise, except as noted below.

### In process

`BarrageMessageProducerPropagateBenchmark.tick`: a real producer on a `TestControlledScheduler`,
200,000 rows by 50 columns, 20,000 rows modified in every column per tick, every subscriber full.
Each subscriber drains into a sink that copies into a small scratch array, standing in for gRPC's
copy. `-Xmx8g`, 5×3 s warmup, 5×3 s measurement, `-prof gc`. `dictString` columns hold strings from
a pool of 1,000 values, dictionary encoded. Milliseconds per tick:

| Columns | Threads | Subscribers | Dedup off | Dedup on | Speedup | Allocated per tick, off / on |
| --- | --- | --- | --- | --- | --- | --- |
| `long` | 1 | 1 | 5.8 ± 0.6 | 5.9 ± 0.7 | 1.0x | 420 / 420 MB |
| `long` | 1 | 4 | 21.5 ± 0.6 | 6.9 ± 0.2 | 3.1x | 421 / 428 MB |
| `long` | 1 | 16 | 86.4 ± 2.2 | 9.2 ± 0.2 | 9.4x | 427 / 428 MB |
| `long` | 4 | 1 | 5.7 ± 0.6 | 5.8 ± 0.8 | 1.0x | 420 / 420 MB |
| `long` | 4 | 4 | 5.9 ± 0.1 | 6.9 ± 0.1 | 0.9x | 421 / 428 MB |
| `long` | 4 | 16 | 22.5 ± 0.1 | 9.1 ± 1.2 | 2.5x | 427 / 428 MB |
| `dictString` | 1 | 1 | 21.4 ± 0.4 | 22.6 ± 2.0 | 0.9x | 315 / 315 MB |
| `dictString` | 1 | 4 | 83.9 ± 5.4 | 22.1 ± 0.6 | 3.8x | 317 / 319 MB |
| `dictString` | 1 | 16 | 320.0 ± 10.3 | 23.8 ± 4.7 | 13.4x | 323 / 319 MB |
| `dictString` | 4 | 1 | 22.2 ± 0.9 | 21.8 ± 2.2 | 1.0x | 315 / 315 MB |
| `dictString` | 4 | 4 | 22.9 ± 1.8 | 21.8 ± 0.2 | 1.0x | 317 / 319 MB |
| `dictString` | 4 | 16 | 84.1 ± 6.9 | 23.0 ± 0.2 | 3.7x | 322 / 319 MB |

Most of the allocation is the benchmark's own table update; the capture arrays add about one message
per write, about 7 MB per tick for `long`.

### Through gRPC

`BarrageGrpcPropagateBenchmark.tick`: 1M rows of 30 mixed columns, including a dictionary encoded
and a run-end encoded column, 100,000 rows modified per tick, about 16 MB per subscriber, written
through a real gRPC Netty transport to clients on the loopback interface. `-Xmx12g`, 2×5 s warmup,
3×5 s measurement. The table reports the producer's job time per tick, averaged over 24–53
invocations; the overall tick also includes the clients receiving the bytes, on the same cores, and
its confidence intervals are too wide to compare.

| Threads | Subscribers | Dedup off (ms) | Dedup on (ms) | Speedup |
| --- | --- | --- | --- | --- |
| 1 | 1 | 31.4 | 31.1 | 1.01x |
| 1 | 4 | 111.7 | 38.2 | 2.92x |
| 1 | 16 | 369.7 | 65.6 | 5.63x |
| 8 | 1 | 30.9 | 31.0 | 1.00x |
| 8 | 4 | 32.8 | 36.7 | 0.89x |
| 8 | 16 | 71.8 | 56.4 | 1.27x |

### What the numbers show

- **CPU per extra identical subscriber drops by about 90%.** On one thread, where job time is close
  to CPU time, each extra subscriber costs about 23–27 ms without dedup and about 2.3 ms with it
  through gRPC. In process it falls from 5.4 to 0.22 ms for `long` and from 19.9 to 0.08 ms for
  `dictString`. The saving grows with the number of identical subscribers; extending the gRPC line,
  128 of them would cost about 2.9 s of CPU per tick without dedup and about 0.3 s with it.
- **One subscriber is not affected.** It takes the per-subscriber path, so the differences in those
  rows are noise.
- **Elapsed time can be slightly worse when threads are plentiful.** With about as many threads as
  subscribers, the per-subscriber writes ran fully in parallel, while a group does one encode and
  then copies serially. That shows as 0.86x for `long` columns at 4 subscribers on 4 threads and
  0.89x through gRPC at 4 subscribers on 8 threads, while the group uses about a third of the CPU:
  38.2 ms against 111.7 ms on one thread.
- **Noise.** One fork per configuration is enough for effects of 3–14x but not for differences of
  10%. The 16-subscriber, 8-thread gRPC run without dedup varied most between runs: 118.1 ms on
  2026-10-18, 89.3 ms on 2026-10-21 and 71.8 ms here. With dedup, the 16-subscriber, 1-thread gRPC
  run moved from 59.0 to 65.6 ms between the last two runs, while the in-process runs, which share
  its code path but leave out the transport and the clients, did not move.

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
| Metrics once per write | The write count then shows the dedup, which is the signal the ticket used |
| One switch, for every table | Dedup applies to every subscriber or to none; a separate mode for dictionary encoded tables would be one more configuration to test and support |
| `getDictionaryBatchView` abstract | A writer that lacks it fails to compile, rather than failing every member that needs a catch-up |
| Copy to members serially | The encode saving dominates; parallel copies would add coordination cost to every update for a gain only on large updates to many subscribers |

## Open questions

- **Parallel copies within a group.** Through gRPC a copy costs about 1.7–2.3 ms per member, so at 16
  members on 8 threads the serial copies are about 25–35 ms of a 56 ms job. They could run in
  parallel: after capturing message k, helper jobs on the propagation scheduler claim members from a
  per-round counter and copy to them. With pipelining, the group's thread returns to encode message
  k+1 while round k is still copying, and before starting round k+1 it claims any members of round k
  not yet claimed and waits only for copies already running. That cannot deadlock, because the helper
  pool has no queue and a refused job runs on the submitting thread. It would hold two captured
  messages per group instead of one, need thread-safe failure bookkeeping, and help only when
  members × message size is large; small updates would get slower. Not planned.
- **The staging copy's size.** Capping the message size for grouped writes would bound it, and
  capturing into pooled 1 MB segments would avoid G1's humongous allocations. Neither is
  implemented.
- **Clients not tested.** No Python ticking, C++, C# or JS test exercises two dictionary batches for
  one id back to back; the C# reader's support for dictionary deltas at all is unknown.

## Re-running

```bash
# in process; add -p threads=... -p subscribers=... to narrow the matrix
./gradlew :extensions-barrage-benchmark:jmhRunBarrageProducerPropagate --args="BarrageMessageProducerPropagateBenchmark.tick \
    -p mix=full -p subscribers=1,4,16 -p threads=1,4 -p columnType=long,dictString -prof gc \
    -jvmArgsPrepend -DBarrageMessageProducer.dedupIdenticalSubscribers=false"

# through gRPC
./gradlew :extensions-barrage-benchmark:jmhRunBarrageGrpcPropagate --args="BarrageGrpcPropagateBenchmark.tick \
    -p subscribers=1,4,16 -p threads=1,8 -p rows=1000000 -p deltaRows=100000 -wi 2 -i 3 -w 5 -r 5 \
    -jvmArgsPrepend -DBarrageMessageProducer.dedupIdenticalSubscribers=false"
```

Run each with the property `true` and `false`. Pass it with `-jvmArgsPrepend`: `-jvmArgsAppend` would
replace the fork's own arguments, including its heap size and, for the in-process benchmark,
`subscriptionGrowthEnabled=false`. Each side of each benchmark takes about 7 minutes. Run nothing
else on the machine meanwhile, and never run Gradle against `:extensions-barrage-benchmark` while a
JMH run of it is live: it rewrites the fork's classpath.
