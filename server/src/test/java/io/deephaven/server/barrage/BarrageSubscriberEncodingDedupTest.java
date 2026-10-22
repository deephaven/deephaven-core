//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.generator.StringGenerator;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistry;
import io.deephaven.extensions.barrage.util.DefensiveDrainable;
import io.deephaven.server.session.SessionService;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.stringCol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Full subscriptions that would be sent identical bytes share one writing of each propagated message: that the group is
 * written once per update however many members it has, that subscriptions differing in what decides their bytes are
 * kept apart, that a failure writing the shared message fails every member while a failure delivering to one member
 * fails only it, and that members behind the group's dictionaries are caught up before the shared message.
 */
public class BarrageSubscriberEncodingDedupTest extends BarrageMessageRoundTripTestBase {

    private static final int PROPAGATION_THREADS = 4;

    @Override
    protected int propagationThreads() {
        return PROPAGATION_THREADS;
    }

    // ---- full subscriptions without dictionaries ----

    @Test
    public void testIdenticalFullSubscriptionsShareOneWrite() {
        final int size = 200;
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                new SetGenerator<>("a", "b", "c", "d"),
                new IntGenerator(0, 100),
                new SetGenerator<>(10.1, 20.1, 30.1));
        final QueryTable source = getTable(size, random, columnInfo);
        final RecordingWriterFactory factory = new RecordingWriterFactory();
        final BarrageMessageProducer producer = newProducer(source, factory);

        // small batches, so that an update is written as several messages
        final BarrageSubscriptionOptions options = optionsBuilder().batchSize(4).build();
        final List<RemoteClient> clients = new ArrayList<>();
        for (int ci = 0; ci < 8; ++ci) {
            clients.add(new RemoteClient(null, allColumns(source), producer, source, "full-" + ci, false, false,
                    options));
        }
        flushProducerTable();
        deliverAndValidate("initial", source, clients);
        factory.takeDeltaWriters();

        boolean sawSeveralBatches = false;
        for (int step = 0; step < 20; ++step) {
            final int[] before = observedCounts(clients);
            randomStep(source, columnInfo, size, random);

            final List<RecordingWriter> writers = factory.takeDeltaWriters();
            assertFalse("step " + step + " propagated an update", writers.isEmpty());
            for (final RecordingWriter writer : writers) {
                assertEquals("step " + step + ": one write for every member", 1, writer.views.size());
            }
            // every member was sent the same messages
            final List<Byte> sent = observedSince(clients.get(0), before[0]);
            for (int ci = 1; ci < clients.size(); ++ci) {
                assertEquals(clients.get(ci).name, sent, observedSince(clients.get(ci), before[ci]));
            }
            sawSeveralBatches |= sent.stream().filter(type -> type == MessageHeader.RecordBatch).count() > writers
                    .size();
            deliverAndValidate("step " + step, source, clients);
        }
        assertTrue("some update was written as several record batches", sawSeveralBatches);
    }

    @Test
    public void testSubscriptionsAreGroupedByOptionsViewportDirectionAndColumns() {
        final int size = 200;
        final Random random = new Random(1);
        final ColumnInfo<?, ?>[] columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                new SetGenerator<>("a", "b", "c", "d"),
                new IntGenerator(0, 100),
                new SetGenerator<>(10.1, 20.1, 30.1));
        final QueryTable source = getTable(size, random, columnInfo);
        final RecordingWriterFactory factory = new RecordingWriterFactory();
        final BarrageMessageProducer producer = newProducer(source, factory);

        final BarrageSubscriptionOptions defaults = defaultSubscriptionOptions();
        final BarrageSubscriptionOptions asList = optionsBuilder().columnsAsList(true).build();
        final BarrageSubscriptionOptions smallBatches = optionsBuilder().batchSize(7).build();
        final BitSet all = allColumns(source);
        final BitSet some = new BitSet();
        some.set(1, 3);

        final List<RemoteClient> clients = new ArrayList<>();
        for (int ci = 0; ci < 3; ++ci) {
            clients.add(new RemoteClient(null, all, producer, source, "full-" + ci, false, false, defaults));
        }
        // a full subscription to fewer columns is sent only those columns, so it is in a group of its own
        for (int ci = 0; ci < 2; ++ci) {
            clients.add(new RemoteClient(null, some, producer, source, "subset-" + ci, false, false, defaults));
        }
        for (int ci = 0; ci < 2; ++ci) {
            clients.add(new RemoteClient(RowSetFactory.fromRange(ci * 50L, ci * 50L + 39), all, producer, source,
                    "viewport-" + ci, false, false, defaults));
        }
        final List<DrainingObserver> drained = new ArrayList<>();
        for (int ci = 0; ci < 2; ++ci) {
            drained.add(subscribe(producer, defaults, true));
            drained.add(subscribe(producer, asList, false));
            drained.add(subscribe(producer, smallBatches, false));
        }
        flushProducerTable();
        deliverAndValidate("initial", source, clients);
        factory.takeDeltaWriters();

        final Set<GroupKey> expectedGroups = Set.of(
                new GroupKey(defaults, false, all),
                new GroupKey(defaults, false, some),
                new GroupKey(defaults, true, all),
                new GroupKey(asList, false, all),
                new GroupKey(smallBatches, false, all));
        for (int step = 0; step < 20; ++step) {
            if (step == 10) {
                // a subscription that joins later is in the default group once it has the whole table
                clients.add(new RemoteClient(null, all, producer, source, "late", false, false, defaults));
            }
            randomStep(source, columnInfo, size, random);
            for (final RecordingWriter writer : factory.takeDeltaWriters()) {
                final Map<GroupKey, Integer> groupWrites = new HashMap<>();
                int viewportWrites = 0;
                for (final ViewRequest view : writer.views) {
                    if (view.isStableFull()) {
                        groupWrites.merge(new GroupKey(view.options, view.reverseViewport, view.columns), 1,
                                Integer::sum);
                    } else {
                        ++viewportWrites;
                    }
                }
                assertEquals("step " + step, expectedGroups, groupWrites.keySet());
                for (final Map.Entry<GroupKey, Integer> entry : groupWrites.entrySet()) {
                    assertEquals("step " + step + ": writes for " + entry.getKey(), 1, (int) entry.getValue());
                }
                assertEquals("step " + step + ": each viewport is written alone", 2, viewportWrites);
            }
            deliverAndValidate("step " + step, source, clients);
        }
        for (final DrainingObserver observer : drained) {
            assertTrue(observer.errors.isEmpty());
            assertTrue(observer.messages > 0);
        }
    }

    @Test
    public void testFailureWritingTheMessageFailsEveryMember() {
        final QueryTable source = smallTable(20);
        final RecordingWriterFactory factory = new RecordingWriterFactory();
        final BarrageMessageProducer producer = newProducer(source, factory);
        final BarrageSubscriptionOptions options = optionsBuilder().batchSize(4).build();

        final List<RemoteClient> members = new ArrayList<>();
        for (int ci = 0; ci < 4; ++ci) {
            members.add(new RemoteClient(null, allColumns(source), producer, source, "full-" + ci, false, false,
                    options));
        }
        final RemoteClient viewport = new RemoteClient(RowSetFactory.fromRange(0, 9), allColumns(source), producer,
                source, "viewport", false, false, options);
        final List<RemoteClient> clients = new ArrayList<>(members);
        clients.add(viewport);
        flushProducerTable();
        deliverAndValidate("initial", source, clients);
        appendRows(source, 8);
        deliverAndValidate("before the failure", source, clients);

        // one member fails on its own before the shared write fails part way through
        final RemoteClient failsFirst = members.get(3);
        failsFirst.dummyObserver.failNextMessage = true;
        final int failAfter = 2;
        factory.failNextGroupViewAfter(failAfter);
        final int[] before = observedCounts(members);
        // forty rows in batches of four: many more messages than are written before the failure
        appendRows(source, 40);

        for (int mi = 0; mi < 3; ++mi) {
            final RemoteClient member = members.get(mi);
            assertEquals(member.name + " was sent the messages written before the failure, once",
                    before[mi] + failAfter, member.dummyObserver.observedHeaderTypes.size());
            assertEquals(member.name + " was failed", 1, member.dummyObserver.errors.size());
        }
        assertEquals(before[3], failsFirst.dummyObserver.observedHeaderTypes.size());
        assertEquals("a member that had already failed is not failed again", 1,
                failsFirst.dummyObserver.errors.size());

        // the viewport subscription, written alone, is unaffected
        deliverAndValidate("after the failure", source, List.of(viewport));
        appendRows(source, 4);
        deliverAndValidate("later", source, List.of(viewport));
        for (int mi = 0; mi < members.size(); ++mi) {
            final RemoteClient member = members.get(mi);
            assertEquals(member.name + " was sent nothing more", mi == 3 ? before[3] : before[mi] + failAfter,
                    member.dummyObserver.observedHeaderTypes.size());
            assertEquals(1, member.dummyObserver.errors.size());
            assertTrue(member.name + " was removed", member.dummyObserver.completed);
        }
        assertTrue(viewport.dummyObserver.errors.isEmpty());
    }

    @Test
    public void testDeliveryFailureFailsOnlyThatMember() {
        final QueryTable source = smallTable(20);
        final RecordingWriterFactory factory = new RecordingWriterFactory();
        final BarrageMessageProducer producer = newProducer(source, factory);
        final BarrageSubscriptionOptions options = optionsBuilder().batchSize(4).build();

        final List<RemoteClient> members = new ArrayList<>();
        for (int ci = 0; ci < 4; ++ci) {
            members.add(new RemoteClient(null, allColumns(source), producer, source, "full-" + ci, false, false,
                    options));
        }
        flushProducerTable();
        deliverAndValidate("initial", source, members);

        final RemoteClient failing = members.remove(1);
        failing.dummyObserver.failNextMessage = true;
        appendRows(source, 12);
        assertEquals(1, failing.dummyObserver.errors.size());
        deliverAndValidate("after the failure", source, members);

        final int failedCount = failing.dummyObserver.observedHeaderTypes.size();
        factory.takeDeltaWriters();
        appendRows(source, 12);
        for (final RecordingWriter writer : factory.takeDeltaWriters()) {
            assertEquals("the remaining members still share one write", 1, writer.views.size());
        }
        deliverAndValidate("later", source, members);
        assertEquals(failedCount, failing.dummyObserver.observedHeaderTypes.size());
        for (final RemoteClient member : members) {
            assertTrue(member.name, member.dummyObserver.errors.isEmpty());
        }
    }

    // ---- full subscriptions with dictionary encoded columns ----

    /**
     * Two members, and a third that joins after a reset left the dictionary holding only what changed since, so its
     * snapshot fills the dictionary with the rest of the table and puts it ahead of the other two.
     */
    private List<RemoteClient> membersWithOneAhead(final QueryTable source, final BarrageMessageProducer producer) {
        final List<RemoteClient> members = new ArrayList<>();
        for (int ci = 0; ci < 2; ++ci) {
            members.add(new RemoteClient(null, allColumns(source), producer, source, "full-" + ci, false, false,
                    defaultSubscriptionOptions()));
        }
        flushProducerTable();
        deliverAndValidate("initial", source, members);

        // The dictionary holds the table's ten values. One more outgrows the ten rows, so the next write first resets
        // the dictionary, leaving it holding only the value that write sends.
        setString(source, 0, "n1");
        deliverAndValidate("outgrown", source, members);
        setString(source, 1, "n2");
        deliverAndValidate("reset", source, members);

        members.add(new RemoteClient(null, allColumns(source), producer, source, "joined", false, false,
                defaultSubscriptionOptions()));
        flushProducerTable();
        deliverAndValidate("joined", source, members);
        return members;
    }

    @Test
    public void testMembersBehindTheGroupAreCaughtUp() {
        final QueryTable source = smallTable(10);
        source.setAttribute(Table.BARRAGE_SCHEMA_ATTRIBUTE, dictionaryEncodedSchema(source.getDefinition(), "Str"));
        final RecordingWriterFactory factory = new RecordingWriterFactory();
        final BarrageMessageProducer producer = newProducer(source, factory);
        final List<RemoteClient> members = membersWithOneAhead(source, producer);
        factory.takeDeltaWriters();

        setString(source, 2, "n3");
        final List<RecordingWriter> writers = factory.takeDeltaWriters();
        assertEquals(1, writers.size());
        assertEquals("one write for every member", 1, writers.get(0).views.size());
        assertEquals("the two members behind share one catch-up", 1, writers.get(0).dictionaryBatchViews.get());
        deliverAndValidate("caught up", source, members);

        // with the dictionary outgrown again it is reset, and no member is ahead of another
        setString(source, 3, "n4");
        assertEquals(0, factory.takeDeltaWriters().get(0).dictionaryBatchViews.get());
        deliverAndValidate("reset again", source, members);

        // members of a group advance together, so they stay level
        setString(source, 4, "n5");
        assertEquals(0, factory.takeDeltaWriters().get(0).dictionaryBatchViews.get());
        deliverAndValidate("level", source, members);
    }

    @Test
    public void testCatchUpDeliveryFailureFailsOnlyThatMember() {
        final QueryTable source = smallTable(10);
        source.setAttribute(Table.BARRAGE_SCHEMA_ATTRIBUTE, dictionaryEncodedSchema(source.getDefinition(), "Str"));
        final RecordingWriterFactory factory = new RecordingWriterFactory();
        final BarrageMessageProducer producer = newProducer(source, factory);
        final List<RemoteClient> members = membersWithOneAhead(source, producer);

        // the first member's first message of the next update is its catch-up
        final RemoteClient failing = members.remove(0);
        failing.dummyObserver.failNextMessage = true;
        setString(source, 2, "n3");
        assertEquals(1, failing.dummyObserver.errors.size());
        deliverAndValidate("after the failure", source, members);

        setString(source, 3, "n4");
        deliverAndValidate("reset", source, members);
        setString(source, 4, "n5");
        deliverAndValidate("later", source, members);
        for (final RemoteClient member : members) {
            assertTrue(member.name, member.dummyObserver.errors.isEmpty());
        }
    }

    @Test
    public void testDictionaryEncodedMembersJoiningLate() {
        final int size = 200;
        final Random random = new Random(2);
        final ColumnInfo<?, ?>[] columnInfo = initColumnInfos(new String[] {"Sym", "Str", "intCol"},
                new SetGenerator<>("a", "b", "c", "d", "e", "f", "g", "h"),
                new StringGenerator(500),
                new IntGenerator(0, 100));
        final QueryTable source = getTable(size / 4, random, columnInfo);
        source.setAttribute(Table.BARRAGE_SCHEMA_ATTRIBUTE,
                dictionaryEncodedSchema(source.getDefinition(), "Sym", "Str"));
        final RecordingWriterFactory factory = new RecordingWriterFactory();
        final BarrageMessageProducer producer = newProducer(source, factory);

        final BitSet all = allColumns(source);
        final BitSet symAndInt = new BitSet();
        symAndInt.set(0);
        symAndInt.set(2);
        final List<RemoteClient> clients = new ArrayList<>();
        for (int ci = 0; ci < 4; ++ci) {
            clients.add(new RemoteClient(null, ci % 2 == 0 ? all : symAndInt, producer, source, "full-" + ci, false,
                    false, defaultSubscriptionOptions()));
        }
        for (int ci = 0; ci < 2; ++ci) {
            clients.add(new RemoteClient(RowSetFactory.fromRange(ci * 10L, ci * 10L + 19), all, producer, source,
                    "viewport-" + ci, false, false, defaultSubscriptionOptions()));
        }
        flushProducerTable();
        deliverAndValidate("initial", source, clients);

        for (int step = 0; step < 40; ++step) {
            if (step % 10 == 5) {
                for (int ci = 0; ci < 2; ++ci) {
                    clients.add(new RemoteClient(null, all, producer, source, "late-" + step + "-" + ci, false,
                            false, defaultSubscriptionOptions()));
                }
            }
            randomStep(source, columnInfo, size, random);
            for (final RecordingWriter writer : factory.takeDeltaWriters()) {
                assertEquals("step " + step + ": one write for each column set of the full subscriptions", 2,
                        writer.views.stream().filter(ViewRequest::isStableFull).count());
            }
            deliverAndValidate("step " + step, source, clients);
        }
        for (final RemoteClient client : clients) {
            assertTrue(client.name, client.dummyObserver.errors.isEmpty());
        }
    }

    /**
     * With a small maximum message size, a dictionary batch too large for one message is split into several, and every
     * subscriber still decodes every value: one written alone, the members of a group, and a member that joins late and
     * is caught up.
     */
    @Test
    public void testDictionaryBatchesOverTheMessageLimitAreSplit() {
        final int size = 400;
        final Random random = new Random(3);
        final ColumnInfo<?, ?>[] columnInfo = initColumnInfos(new String[] {"Str", "intCol"},
                new StringGenerator(),
                new IntGenerator(0, 100));
        final QueryTable source = getTable(size, random, columnInfo);
        source.setAttribute(Table.BARRAGE_SCHEMA_ATTRIBUTE, dictionaryEncodedSchema(source.getDefinition(), "Str"));
        final BarrageMessageProducer producer = newProducer(source, new RecordingWriterFactory());

        final BarrageSubscriptionOptions small = optionsBuilder().maxMessageSize(2048).build();
        final List<RemoteClient> clients = new ArrayList<>();
        for (int ci = 0; ci < 3; ++ci) {
            clients.add(new RemoteClient(null, allColumns(source), producer, source, "full-" + ci, false, false,
                    small));
        }
        final RemoteClient viewport = new RemoteClient(RowSetFactory.fromRange(0, 199), allColumns(source), producer,
                source, "viewport", false, false, small);
        clients.add(viewport);
        flushProducerTable();
        deliverAndValidate("initial", source, clients);
        // A subscriber written alone is sent no catch-ups, so two dictionary batches in a row are one split batch; its
        // 200
        // rows hold about 200 distinct values of the only dictionary column, too many for one message under the limit.
        assertTrue("the viewport's first dictionary batch was split", hasConsecutiveDictionaryBatches(viewport));

        for (int step = 0; step < 20; ++step) {
            if (step == 10) {
                clients.add(new RemoteClient(null, allColumns(source), producer, source, "late", false, false,
                        small));
            }
            randomStep(source, columnInfo, size, random);
            deliverAndValidate("step " + step, source, clients);
        }
        for (final RemoteClient client : clients) {
            assertTrue(client.name, client.dummyObserver.errors.isEmpty());
        }
    }

    /** Whether {@code client} was sent two dictionary batches in a row. */
    private static boolean hasConsecutiveDictionaryBatches(final RemoteClient client) {
        final List<Byte> observed = client.dummyObserver.observedHeaderTypes;
        for (int ii = 1; ii < observed.size(); ++ii) {
            if (observed.get(ii - 1) == MessageHeader.DictionaryBatch
                    && observed.get(ii) == MessageHeader.DictionaryBatch) {
                return true;
            }
        }
        return false;
    }

    // ---- helpers ----

    private ControlledUpdateGraph updateGraph() {
        return ExecutionContext.getContext().getUpdateGraph().cast();
    }

    private BarrageSubscriptionOptions.Builder optionsBuilder() {
        return BarrageSubscriptionOptions.builder().useDeephavenNulls(useDeephavenNulls);
    }

    private BarrageMessageProducer newProducer(final QueryTable source, final BarrageMessageWriter.Factory factory) {
        return source.getResult(new BarrageMessageProducer.Operation(scheduler,
                new SessionService.ObfuscatingErrorTransformer(), factory, source, UPDATE_INTERVAL, null,
                propagationJobSchedulerFactory));
    }

    private static BitSet allColumns(final Table table) {
        final BitSet columns = new BitSet();
        columns.set(0, table.numColumns());
        return columns;
    }

    private static DrainingObserver subscribe(final BarrageMessageProducer producer,
            final BarrageSubscriptionOptions options, final boolean reverseViewport) {
        final DrainingObserver observer = new DrainingObserver();
        producer.addSubscription(observer, options, null, null, reverseViewport);
        return observer;
    }

    /** A refreshing table of {@code size} rows: column {@code I} holds the row key, {@code Str} holds "s" and it. */
    private static QueryTable smallTable(final int size) {
        final int[] ints = new int[size];
        final String[] strings = new String[size];
        for (int ii = 0; ii < size; ++ii) {
            ints[ii] = ii;
            strings[ii] = "s" + ii;
        }
        return testRefreshingTable(RowSetFactory.flat(size).toTracking(), intCol("I", ints),
                stringCol("Str", strings));
    }

    /** Appends {@code count} rows to a {@link #smallTable} in one update, and propagates it. */
    private void appendRows(final QueryTable table, final int count) {
        final long first = table.getRowSet().lastRowKey() + 1;
        final int[] ints = new int[count];
        final String[] strings = new String[count];
        for (int ii = 0; ii < count; ++ii) {
            ints[ii] = (int) (first + ii);
            strings[ii] = "a" + (first + ii);
        }
        updateGraph().runWithinUnitTestCycle(() -> {
            addToTable(table, RowSetFactory.fromRange(first, first + count - 1), intCol("I", ints),
                    stringCol("Str", strings));
            table.notifyListeners(RowSetFactory.fromRange(first, first + count - 1), i(), i());
        });
        flushProducerTable();
    }

    /** Sets row {@code key}'s {@code Str} in a {@link #smallTable} in one update, and propagates it. */
    private void setString(final QueryTable table, final long key, final String value) {
        updateGraph().runWithinUnitTestCycle(() -> {
            addToTable(table, i(key), intCol("I", (int) key), stringCol("Str", value));
            table.notifyListeners(i(), i(), i(key));
        });
        flushProducerTable();
    }

    private void randomStep(final QueryTable source, final ColumnInfo<?, ?>[] columnInfo, final int size,
            final Random random) {
        updateGraph().runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                GenerateTableUpdates.DEFAULT_PROFILE, size, random, source, columnInfo));
        flushProducerTable();
    }

    /** Applies what each client has been sent to its replicated table, and checks the table against the source. */
    private void deliverAndValidate(final String step, final QueryTable source, final List<RemoteClient> clients) {
        for (final RemoteClient client : clients) {
            client.flushEventsToReplicatedTable();
        }
        updateGraph().runWithinUnitTestCycle(updateSourceCombiner::run);
        for (final RemoteClient client : clients) {
            assertTrue(step + ": " + client.name + " was not failed", client.dummyObserver.errors.isEmpty());
            client.validate(step, source);
        }
    }

    private static int[] observedCounts(final List<RemoteClient> clients) {
        return clients.stream().mapToInt(client -> client.dummyObserver.observedHeaderTypes.size()).toArray();
    }

    private static List<Byte> observedSince(final RemoteClient client, final int from) {
        final List<Byte> observed = client.dummyObserver.observedHeaderTypes;
        return new ArrayList<>(observed.subList(from, observed.size()));
    }

    /** The options, viewport direction and columns of a group's writes. */
    private static final class GroupKey {
        private final BarrageSubscriptionOptions options;
        private final boolean reverseViewport;
        private final BitSet columns;

        private GroupKey(final BarrageSubscriptionOptions options, final boolean reverseViewport,
                final BitSet columns) {
            this.options = options;
            this.reverseViewport = reverseViewport;
            this.columns = (BitSet) columns.clone();
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof GroupKey)) {
                return false;
            }
            final GroupKey that = (GroupKey) o;
            return reverseViewport == that.reverseViewport && options.equals(that.options)
                    && columns.equals(that.columns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(options, reverseViewport, columns);
        }

        @Override
        public String toString() {
            return options + (reverseViewport ? " reversed" : "") + " columns " + columns;
        }
    }

    /** One view of a propagated message that the producer asked a writer for. */
    private static final class ViewRequest {
        private final BarrageSubscriptionOptions options;
        private final boolean fullSubscription;
        private final boolean hasViewport;
        private final boolean reverseViewport;
        private final BitSet columns;

        private ViewRequest(final BarrageSubscriptionOptions options, final boolean fullSubscription,
                final boolean hasViewport, final boolean reverseViewport, final BitSet columns) {
            this.options = options;
            this.fullSubscription = fullSubscription;
            this.hasViewport = hasViewport;
            this.reverseViewport = reverseViewport;
            this.columns = (BitSet) columns.clone();
        }

        /** Whether this is the view of a full subscription that has the whole table, which a group shares. */
        private boolean isStableFull() {
            return fullSubscription && !hasViewport;
        }
    }

    /**
     * Wraps the server's writer factory to record the views asked of each propagated message's writer, and to make the
     * next group's view fail part way through.
     */
    private final class RecordingWriterFactory implements BarrageMessageWriter.Factory {
        private final BarrageMessageWriter.Factory delegate = daggerRoot.getStreamGeneratorFactory();
        private final List<RecordingWriter> deltaWriters = Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger failGroupViewAfter = new AtomicInteger(-1);

        @Override
        public BarrageMessageWriter newMessageWriter(
                @NotNull final BarrageMessage message,
                @NotNull final ChunkWriter<Chunk<Values>>[] chunkWriters,
                @NotNull final BarrageMessageWriter.WriteMetricsConsumer metricsConsumer) {
            final RecordingWriter writer = new RecordingWriter(
                    delegate.newMessageWriter(message, chunkWriters, metricsConsumer), failGroupViewAfter);
            if (!message.isSnapshot) {
                deltaWriters.add(writer);
            }
            return writer;
        }

        @Override
        public BarrageMessageWriter.MessageView getSchemaView(
                @NotNull final ToIntFunction<FlatBufferBuilder> schemaPayloadWriter,
                @Nullable final Flight.FlightDescriptor flightDescriptor) {
            return delegate.getSchemaView(schemaPayloadWriter, flightDescriptor);
        }

        /** The writers made for propagated messages since the last call. */
        private List<RecordingWriter> takeDeltaWriters() {
            synchronized (deltaWriters) {
                final List<RecordingWriter> taken = new ArrayList<>(deltaWriters);
                deltaWriters.clear();
                return taken;
            }
        }

        /** Makes the next view of a stable full subscription fail after writing {@code streams} streams. */
        private void failNextGroupViewAfter(final int streams) {
            failGroupViewAfter.set(streams);
        }
    }

    private static final class RecordingWriter implements BarrageMessageWriter {
        private final BarrageMessageWriter delegate;
        private final AtomicInteger failGroupViewAfter;
        private final List<ViewRequest> views = Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger dictionaryBatchViews = new AtomicInteger();

        private RecordingWriter(final BarrageMessageWriter delegate, final AtomicInteger failGroupViewAfter) {
            this.delegate = delegate;
            this.failGroupViewAfter = failGroupViewAfter;
        }

        @Override
        public BarrageMessage getMessage() {
            return delegate.getMessage();
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public MessageView getSubView(final BarrageSubscriptionOptions options, final boolean isInitialSnapshot) {
            return delegate.getSubView(options, isInitialSnapshot);
        }

        @Override
        public MessageView getSubView(final BarrageSubscriptionOptions options, final boolean isInitialSnapshot,
                final boolean isFullSubscription, @Nullable final RowSet viewport, final boolean reverseViewport,
                @Nullable final RowSet keyspaceViewportPrev, @Nullable final RowSet keyspaceViewport,
                final BitSet subscribedColumns) {
            return getSubView(options, isInitialSnapshot, isFullSubscription, viewport, reverseViewport,
                    keyspaceViewportPrev, keyspaceViewport, subscribedColumns, null);
        }

        @Override
        public MessageView getSubView(final BarrageSubscriptionOptions options, final boolean isInitialSnapshot,
                final boolean isFullSubscription, @Nullable final RowSet viewport, final boolean reverseViewport,
                @Nullable final RowSet keyspaceViewportPrev, @Nullable final RowSet keyspaceViewport,
                final BitSet subscribedColumns, @Nullable final DictionaryWriterRegistry dictionaryRegistry) {
            final ViewRequest request =
                    new ViewRequest(options, isFullSubscription, viewport != null, reverseViewport, subscribedColumns);
            views.add(request);
            final MessageView view = delegate.getSubView(options, isInitialSnapshot, isFullSubscription, viewport,
                    reverseViewport, keyspaceViewportPrev, keyspaceViewport, subscribedColumns, dictionaryRegistry);
            if (request.isStableFull()) {
                final int failAfter = failGroupViewAfter.getAndSet(-1);
                if (failAfter >= 0) {
                    return new FailingView(view, failAfter);
                }
            }
            return view;
        }

        @Override
        public MessageView getSnapshotView(final BarrageSnapshotOptions options) {
            return delegate.getSnapshotView(options);
        }

        @Override
        public MessageView getSnapshotView(final BarrageSnapshotOptions options, @Nullable final RowSet viewport,
                final boolean reverseViewport, @Nullable final RowSet keyspaceViewport,
                final BitSet snapshotColumns) {
            return delegate.getSnapshotView(options, viewport, reverseViewport, keyspaceViewport, snapshotColumns);
        }

        @Override
        public MessageView getSnapshotView(final BarrageSnapshotOptions options, @Nullable final RowSet viewport,
                final boolean reverseViewport, @Nullable final RowSet keyspaceViewport,
                final BitSet snapshotColumns, @Nullable final DictionaryWriterRegistry dictionaryRegistry) {
            return delegate.getSnapshotView(options, viewport, reverseViewport, keyspaceViewport, snapshotColumns,
                    dictionaryRegistry);
        }

        @Override
        public MessageView getDictionaryBatchView(final BarrageOptions options, final long dictId,
                final ChunkWriter<Chunk<Values>> valuesWriter, final WritableChunk<Values> values,
                final boolean isDelta) {
            dictionaryBatchViews.incrementAndGet();
            return delegate.getDictionaryBatchView(options, dictId, valuesWriter, values, isDelta);
        }
    }

    /** A view that fails, as a failing writer would, once it has written {@code failAfter} streams. */
    private static final class FailingView implements BarrageMessageWriter.MessageView {
        private final BarrageMessageWriter.MessageView delegate;
        private final int failAfter;

        private FailingView(final BarrageMessageWriter.MessageView delegate, final int failAfter) {
            this.delegate = delegate;
            this.failAfter = failAfter;
        }

        @Override
        public void forEachStream(final Consumer<DefensiveDrainable> visitor) throws IOException {
            final AtomicInteger written = new AtomicInteger();
            delegate.forEachStream(stream -> {
                if (written.getAndIncrement() == failAfter) {
                    try {
                        stream.close();
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    throw new IllegalStateException("injected failure writing the shared message");
                }
                visitor.accept(stream);
            });
        }
    }

    /** Drains what it is sent, counting the messages. */
    private static final class DrainingObserver implements StreamObserver<BarrageMessageWriter.MessageView> {
        private int messages;
        private final List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onNext(final BarrageMessageWriter.MessageView view) {
            ++messages;
            try {
                view.forEachStream(stream -> {
                    try {
                        stream.drainTo(OutputStream.nullOutputStream());
                        stream.close();
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void onError(final Throwable t) {
            errors.add(t);
        }

        @Override
        public void onCompleted() {}
    }
}
