//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.util.ExecutorJobScheduler;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.BarrageTypeInfo;
import io.deephaven.extensions.barrage.DrainableByteArrayInputStream;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistry;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistryImpl;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterState;
import io.deephaven.extensions.barrage.chunk.SharedDictionaryWriterState;
import io.deephaven.extensions.barrage.chunk.SharedWriterDictionary;
import io.deephaven.extensions.barrage.util.DefensiveDrainable;
import io.deephaven.extensions.barrage.util.ExposedByteArrayOutputStream;
import io.grpc.stub.StreamObserver;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Delivering one writing of a message to several subscribers: each stream reaches every member still receiving before
 * the next is written, a member whose delivery fails is failed alone, and writing stops once no member is left. Before
 * the message, the members' dictionary states are brought level: the group starts at the highest offset any member has
 * reached, each member below it is owed exactly the values it lacks, and once the message is written every member ends
 * where the group does.
 */
public class SharedMessageFanOutTest {

    private static final BarrageSubscriptionOptions OPTS = BarrageSubscriptionOptions.builder().build();
    private static final long ID = 5;
    private static final ChunkWriter<Chunk<Values>> VALUES_WRITER = intWriter();

    private Long2ObjectOpenHashMap<SharedWriterDictionary> sharedDictionaries;
    private SharedWriterDictionary shared;

    @Before
    public void setUp() {
        sharedDictionaries = new Long2ObjectOpenHashMap<>();
        shared = new SharedWriterDictionary(ID, ChunkType.Object);
        sharedDictionaries.put(ID, shared);
    }

    /** A stream of one byte that records whether it was closed. */
    private static final class TrackedStream extends DrainableByteArrayInputStream {
        private boolean closed;

        private TrackedStream(final int value) {
            super(new byte[] {(byte) value}, 0, 1);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /**
     * Writes {@code numStreams} streams, recording each one it makes; it makes the next only once the visitor has
     * accepted the last, as a message writer does. After {@code failAfter} streams it fails instead.
     */
    private static final class GeneratingView implements BarrageMessageWriter.MessageView {
        private final int numStreams;
        private final int failAfter;
        private final List<TrackedStream> made = new ArrayList<>();

        private GeneratingView(final int numStreams) {
            this(numStreams, Integer.MAX_VALUE);
        }

        private GeneratingView(final int numStreams, final int failAfter) {
            this.numStreams = numStreams;
            this.failAfter = failAfter;
        }

        @Override
        public void forEachStream(final Consumer<DefensiveDrainable> visitor) throws IOException {
            for (int si = 0; si < numStreams; ++si) {
                if (si == failAfter) {
                    throw new IOException("injected failure writing stream " + si);
                }
                final TrackedStream stream = new TrackedStream(si);
                made.add(stream);
                visitor.accept(stream);
            }
        }
    }

    /** Records the bytes of every message it is sent; its {@code failOn}th message fails instead. */
    private static final class RecordingListener implements StreamObserver<BarrageMessageWriter.MessageView> {
        private final int failOn;
        private final List<byte[]> received = new ArrayList<>();
        private int calls;

        private RecordingListener() {
            this(-1);
        }

        private RecordingListener(final int failOn) {
            this.failOn = failOn;
        }

        @Override
        public void onNext(final BarrageMessageWriter.MessageView view) {
            if (calls++ == failOn) {
                throw new IllegalStateException("injected failure delivering message " + failOn);
            }
            try {
                view.forEachStream(stream -> {
                    try (final ExposedByteArrayOutputStream out = new ExposedByteArrayOutputStream()) {
                        stream.drainTo(out);
                        received.add(Arrays.copyOf(out.peekBuffer(), out.size()));
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void onError(final Throwable t) {}

        @Override
        public void onCompleted() {}
    }

    private static final class Failures implements SharedMessageFanOut.DeliveryFailureHandler {
        private final List<Integer> members = new ArrayList<>();

        @Override
        public void onFailure(final int member, final Exception failure) {
            members.add(member);
        }
    }

    /** Records each report of the copies a message's write did not record, as its bytes and copies. */
    private static final class Copies implements SharedMessageFanOut.CopyMetricsConsumer {
        private final List<String> reports = new ArrayList<>();

        @Override
        public void onCopies(final long bytes, final int copies) {
            reports.add("bytes=" + bytes + ", copies=" + copies);
        }
    }

    /** A fan-out to {@code listeners}, each member with a registry backed by the shared dictionaries. */
    private SharedMessageFanOut fanOut(
            final SharedMessageFanOut.DeliveryFailureHandler failures,
            final RecordingListener... listeners) {
        return fanOut(failures, new Copies(), listeners);
    }

    /**
     * A fan-out to {@code listeners} that reports copies to {@code copies}, each member with a registry backed by the
     * shared dictionaries.
     */
    private SharedMessageFanOut fanOut(
            final SharedMessageFanOut.DeliveryFailureHandler failures,
            final Copies copies,
            final RecordingListener... listeners) {
        final List<DictionaryWriterRegistryImpl> registries = new ArrayList<>(listeners.length);
        for (int ii = 0; ii < listeners.length; ++ii) {
            registries.add(member());
        }
        return new SharedMessageFanOut(List.of(listeners), registries, sharedDictionaries, failures, copies,
                new ImmediateJobScheduler(), Long.MAX_VALUE);
    }

    private static byte[] bytes(final int value) {
        return new byte[] {(byte) value};
    }

    @Test
    public void testEveryMemberReceivesEveryStreamInOrder() throws IOException {
        final RecordingListener first = new RecordingListener();
        final RecordingListener second = new RecordingListener();
        final Failures failures = new Failures();
        final SharedMessageFanOut fanOut = fanOut(failures, first, second);

        final GeneratingView view = new GeneratingView(3);
        fanOut.deliverMessageToSubscribers(view, IntList.of(0, 1));

        for (final RecordingListener listener : List.of(first, second)) {
            assertThat(listener.received).containsExactly(bytes(0), bytes(1), bytes(2));
        }
        assertThat(failures.members).isEmpty();
        assertThat(view.made).allMatch(stream -> stream.closed);
    }

    /**
     * Copied to the subscribers in parallel, every stream still reaches every subscriber in order, and a subscriber
     * whose delivery fails is failed alone.
     */
    @Test
    public void testParallelCopiesDeliverEveryStreamInOrder() throws IOException {
        final ThreadPoolExecutor helpers = ExecutorJobScheduler.newHelperPool(3, Executors.defaultThreadFactory());
        try {
            final List<RecordingListener> listeners = new ArrayList<>();
            final List<DictionaryWriterRegistryImpl> registries = new ArrayList<>();
            for (int si = 0; si < 8; ++si) {
                // the sixth subscriber's second delivery fails
                listeners.add(si == 5 ? new RecordingListener(1) : new RecordingListener());
                registries.add(member());
            }
            final Failures failures = new Failures();
            final Copies copies = new Copies();
            // a threshold of zero copies every round in parallel
            final SharedMessageFanOut fanOut = new SharedMessageFanOut(new ArrayList<>(listeners), registries,
                    sharedDictionaries, failures, copies, new ExecutorJobScheduler(helpers, 4), 0);

            final GeneratingView view = new GeneratingView(3);
            fanOut.deliverMessageToSubscribers(view, IntList.of(0, 1, 2, 3, 4, 5, 6, 7));

            assertThat(failures.members).containsExactly(5);
            for (int si = 0; si < listeners.size(); ++si) {
                if (si == 5) {
                    assertThat(listeners.get(si).received).containsExactly(bytes(0));
                } else {
                    assertThat(listeners.get(si).received).containsExactly(bytes(0), bytes(1), bytes(2));
                }
            }
            assertThat(view.made).allMatch(stream -> stream.closed);
            // the seven members sent the whole message are counted, once by the write and six times as copies
            assertThat(copies.reports).containsExactly("bytes=3, copies=6");
        } finally {
            helpers.shutdownNow();
        }
    }

    /**
     * A message's write records one copy, and the fan-out reports a copy for each other member still receiving once the
     * whole message is delivered, so every member sent all of it is counted once. A member that failed during the
     * message is not counted, and a message that is not delivered reports nothing.
     */
    @Test
    public void testCopiesBeyondTheWritesOwnAreReported() throws IOException {
        final Copies copies = new Copies();
        final SharedMessageFanOut fanOut = fanOut(new Failures(), copies, new RecordingListener(),
                new RecordingListener(1), new RecordingListener(), new RecordingListener());

        // the second member fails on the second stream, so three members are sent all three bytes
        fanOut.deliverMessageToSubscribers(new GeneratingView(3), IntList.of(0, 1, 2, 3));
        assertThat(copies.reports).containsExactly("bytes=3, copies=2");

        // a message sent to one member is counted by its write alone
        fanOut.deliverMessageToSubscribers(new GeneratingView(2), IntList.of(2));
        assertThat(copies.reports).hasSize(1);

        // a message whose write fails is not delivered
        assertThatThrownBy(() -> fanOut.deliverMessageToSubscribers(new GeneratingView(5, 2), IntList.of(0, 2, 3)))
                .isInstanceOf(IOException.class);
        assertThat(copies.reports).hasSize(1);

        // nor is one abandoned because every member failed
        final Copies abandonedCopies = new Copies();
        fanOut(new Failures(), abandonedCopies, new RecordingListener(0), new RecordingListener(0))
                .deliverMessageToSubscribers(new GeneratingView(10), IntList.of(0, 1));
        assertThat(abandonedCopies.reports).isEmpty();
    }

    @Test
    public void testDeliveryFailureFailsOnlyThatMember() throws IOException {
        final RecordingListener first = new RecordingListener();
        final RecordingListener failing = new RecordingListener(1);
        final RecordingListener third = new RecordingListener();
        final Failures failures = new Failures();
        final SharedMessageFanOut fanOut = fanOut(failures, first, failing, third);

        fanOut.deliverMessageToSubscribers(new GeneratingView(3), IntList.of(0, 1, 2));

        assertThat(failures.members).containsExactly(1);
        // the failed member is sent nothing after its failure
        assertThat(failing.received).containsExactly(bytes(0));
        assertThat(failing.calls).isEqualTo(2);
        for (final RecordingListener listener : List.of(first, third)) {
            assertThat(listener.received).containsExactly(bytes(0), bytes(1), bytes(2));
        }
    }

    @Test
    public void testStopsWritingOnceNoMemberIsReceiving() throws IOException {
        final Failures failures = new Failures();
        final SharedMessageFanOut fanOut = fanOut(failures, new RecordingListener(0), new RecordingListener(0));

        final GeneratingView view = new GeneratingView(10);
        fanOut.deliverMessageToSubscribers(view, IntList.of(0, 1));

        assertThat(failures.members).containsExactly(0, 1);
        // the stream written while both failed, and the one written after, which was abandoned; no more
        assertThat(view.made).hasSize(2);
        assertThat(view.made).allMatch(stream -> stream.closed);
    }

    @Test
    public void testWriteFailureIsThrownToTheCaller() {
        final RecordingListener first = new RecordingListener();
        final RecordingListener second = new RecordingListener();
        final Failures failures = new Failures();
        final SharedMessageFanOut fanOut = fanOut(failures, first, second);

        assertThatThrownBy(() -> fanOut.deliverMessageToSubscribers(new GeneratingView(5, 2), IntList.of(0, 1)))
                .isInstanceOf(IOException.class);

        // members are failed by the caller, which knows the failure would meet them all
        assertThat(failures.members).isEmpty();
        for (final RecordingListener listener : List.of(first, second)) {
            assertThat(listener.received).containsExactly(bytes(0), bytes(1));
        }
        fanOut.failAllSubscribers(new IllegalStateException("failed writing"));
        assertThat(failures.members).containsExactly(0, 1);
        // a member is failed only once
        fanOut.failSubscriber(0, new IllegalStateException("again"));
        assertThat(failures.members).containsExactly(0, 1);
    }

    @Test
    public void testDeliverMessageToSubscribersSendsOnlyToTheListedMembersStillReceiving() throws IOException {
        final RecordingListener first = new RecordingListener();
        final RecordingListener second = new RecordingListener();
        final RecordingListener third = new RecordingListener();
        final SharedMessageFanOut fanOut = fanOut(new Failures(), first, second, third);

        fanOut.failSubscriber(0, new IllegalStateException("failed"));
        // every stream of the view reaches the listed members still receiving, in order
        final GeneratingView view = new GeneratingView(2);
        fanOut.deliverMessageToSubscribers(view, IntList.of(0, 2));

        assertThat(first.received).isEmpty();
        assertThat(second.received).isEmpty();
        assertThat(third.received).containsExactly(bytes(0), bytes(1));
        assertThat(view.made).hasSize(2);
        assertThat(view.made).allMatch(stream -> stream.closed);
    }

    /** A stream that writes more or fewer bytes than it reports fails the write, and nothing of it is delivered. */
    @Test
    public void testDeliverMessageRejectsAStreamThatMisreportsItsSize() {
        final RecordingListener first = new RecordingListener();
        final RecordingListener second = new RecordingListener();
        final SharedMessageFanOut fanOut = fanOut(new Failures(), first, second);

        final DefensiveDrainable shortStream = new DefensiveDrainable() {
            @Override
            public int available() {
                return 4;
            }

            @Override
            public int drainTo(final OutputStream target) throws IOException {
                target.write(new byte[3]);
                return 3;
            }
        };
        assertThatThrownBy(() -> fanOut.deliverMessageToSubscribers(visitor -> visitor.accept(shortStream),
                IntList.of(0, 1))).isInstanceOf(IllegalStateException.class);

        final DefensiveDrainable longStream = new DefensiveDrainable() {
            @Override
            public int available() {
                return 2;
            }

            @Override
            public int drainTo(final OutputStream target) throws IOException {
                target.write(new byte[3]);
                return 3;
            }
        };
        assertThatThrownBy(() -> fanOut.deliverMessageToSubscribers(visitor -> visitor.accept(longStream),
                IntList.of(0, 1))).isInstanceOf(IllegalStateException.class);

        assertThat(first.received).isEmpty();
        assertThat(second.received).isEmpty();
    }

    /** A placeholder values writer; these tests never serialize values. */
    @SuppressWarnings("unchecked")
    private static ChunkWriter<Chunk<Values>> intWriter() {
        final org.apache.arrow.vector.types.pojo.Schema pojoSchema =
                new org.apache.arrow.vector.types.pojo.Schema(Collections.singletonList(
                        new Field("x", FieldType.notNullable(new ArrowType.Int(32, true)),
                                Collections.emptyList())));
        final org.apache.arrow.flatbuf.Schema fbSchema = io.deephaven.proto.flight.util.SchemaHelper
                .flatbufSchema(ByteBuffer.wrap(pojoSchema.serializeAsMessage()));
        return (ChunkWriter<Chunk<Values>>) (ChunkWriter<?>) DefaultChunkWriterFactory.INSTANCE
                .newWriter(BarrageTypeInfo.make(int.class, null, fbSchema.fields(0)));
    }

    /** A member's registry, backed by the shared dictionaries. */
    private DictionaryWriterRegistryImpl member() {
        return new DictionaryWriterRegistryImpl(sharedDictionaries);
    }

    /** Adds {@code values} to dictionary {@code dictionary}, as some subscriber's write would. */
    private static void add(final SharedWriterDictionary dictionary, final Object... values) {
        final DictionaryWriterState writer = new SharedDictionaryWriterState(dictionary);
        for (final Object value : values) {
            try (final WritableObjectChunk<Object, Values> src = WritableObjectChunk.makeWritableChunk(1);
                    final WritableIntChunk<Values> out = WritableIntChunk.makeWritableChunk(1)) {
                src.set(0, value);
                src.setSize(1);
                out.setSize(1);
                writer.fillIndexChunk(src, null, OPTS, out);
            }
        }
    }

    /** The state for {@link #ID} in {@code registry}, created if need be. */
    private static DictionaryWriterState state(final DictionaryWriterRegistry registry) {
        return registry.getOrCreate(ID, VALUES_WRITER, ChunkType.Object);
    }

    /**
     * Sends {@code registry}'s pending deltas as a batch whose rows reference every one of its dictionaries would, and
     * returns the values each id's batch carried.
     */
    private static List<Object> send(final DictionaryWriterRegistry registry) {
        for (final DictionaryWriterRegistry.Entry entry : registry.entries()) {
            reference(entry.state);
        }
        return sendUnreferenced(registry);
    }

    /** Sends {@code registry}'s pending deltas as a batch whose rows reference none of its dictionaries would. */
    private static List<Object> sendUnreferenced(final DictionaryWriterRegistry registry) {
        final List<Object> sent = new ArrayList<>();
        for (final DictionaryWriterRegistry.Entry entry : registry.entries()) {
            if (entry.state.hasDelta()) {
                sent.addAll(values(entry.state.buildDeltaChunk()));
            }
        }
        registry.resetDeltas();
        return sent;
    }

    /** Encodes no rows with {@code state}, which is enough for the batch being built to reference its dictionary. */
    private static void reference(final DictionaryWriterState state) {
        try (final WritableObjectChunk<Object, Values> src = WritableObjectChunk.makeWritableChunk(0);
                final WritableIntChunk<Values> out = WritableIntChunk.makeWritableChunk(0)) {
            state.fillIndexChunk(src, null, OPTS, out);
        }
    }

    /** The contents of {@code chunk}, which this closes. */
    private static List<Object> values(final WritableChunk<Values> chunk) {
        try (chunk) {
            final ObjectChunk<Object, Values> objects = chunk.asObjectChunk();
            final List<Object> result = new ArrayList<>(objects.size());
            for (int ii = 0; ii < objects.size(); ++ii) {
                result.add(objects.get(ii));
            }
            return result;
        }
    }

    /** A catch-up DictionaryBatch the fan-out asked the writer for. */
    private static final class CatchUp {
        private final long dictId;
        private final boolean isDelta;
        private final List<Object> values;

        private CatchUp(final long dictId, final boolean isDelta, final List<Object> values) {
            this.dictId = dictId;
            this.isDelta = isDelta;
            this.values = values;
        }

        private long getDictId() {
            return dictId;
        }

        private boolean isDelta() {
            return isDelta;
        }

        private List<Object> values() {
            return values;
        }
    }

    /**
     * Writes only standalone DictionaryBatches, recording each catch-up it is asked for. The one stream of the
     * {@code i}th is the single byte {@code i}, so the bytes a member receives name the catch-ups it was sent.
     */
    private static final class CatchUpWriter implements BarrageMessageWriter {
        private final List<CatchUp> written = new ArrayList<>();
        /** The index of the catch-up that fails to build, or -1 for none. */
        private int failBuilding = -1;
        /** The index of the catch-up whose view fails to write, or -1 for none. */
        private int failWriting = -1;

        @Override
        public MessageView getDictionaryBatchView(
                final BarrageOptions options,
                final long dictId,
                final ChunkWriter<Chunk<Values>> valuesWriter,
                final WritableChunk<Values> values,
                final boolean isDelta) {
            final int index = written.size();
            written.add(new CatchUp(dictId, isDelta, values(values)));
            if (index == failBuilding) {
                throw new IllegalStateException("injected failure building catch-up " + index);
            }
            if (index == failWriting) {
                return visitor -> {
                    throw new IOException("injected failure writing catch-up " + index);
                };
            }
            final byte[] bytes = bytes(index);
            return visitor -> visitor.accept(new DrainableByteArrayInputStream(bytes, 0, bytes.length));
        }

        @Override
        public BarrageMessage getMessage() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessageView getSubView(final BarrageSubscriptionOptions options, final boolean isInitialSnapshot) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessageView getSubView(
                final BarrageSubscriptionOptions options,
                final boolean isInitialSnapshot,
                final boolean isFullSubscription,
                final RowSet viewport,
                final boolean reverseViewport,
                final RowSet keyspaceViewportPrev,
                final RowSet keyspaceViewport,
                final BitSet subscribedColumns) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessageView getSnapshotView(final BarrageSnapshotOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessageView getSnapshotView(
                final BarrageSnapshotOptions options,
                final RowSet viewport,
                final boolean reverseViewport,
                final RowSet keyspaceViewport,
                final BitSet snapshotColumns) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessageView getSubView(
                final BarrageSubscriptionOptions options,
                final boolean isInitialSnapshot,
                final boolean isFullSubscription,
                final RowSet viewport,
                final boolean reverseViewport,
                final RowSet keyspaceViewportPrev,
                final RowSet keyspaceViewport,
                final BitSet subscribedColumns,
                final DictionaryWriterRegistry dictionaryRegistry) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessageView getSnapshotView(
                final BarrageSnapshotOptions options,
                final RowSet viewport,
                final boolean reverseViewport,
                final RowSet keyspaceViewport,
                final BitSet snapshotColumns,
                final DictionaryWriterRegistry dictionaryRegistry) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }

    /** A group whose dictionaries have been synchronized: the catch-ups it sent, and the registry it starts from. */
    private static final class SynchronizedGroup {
        private final SharedMessageFanOut fanOut;
        private final List<RecordingListener> listeners;
        private final CatchUpWriter writer;
        private final DictionaryWriterRegistry registry;
        private final Failures failures;
        private final Copies copies;

        private SynchronizedGroup(
                final SharedMessageFanOut fanOut,
                final List<RecordingListener> listeners,
                final CatchUpWriter writer,
                final DictionaryWriterRegistry registry,
                final Failures failures,
                final Copies copies) {
            this.fanOut = fanOut;
            this.listeners = listeners;
            this.writer = writer;
            this.registry = registry;
            this.failures = failures;
            this.copies = copies;
        }

        /** Every catch-up built, in the order sent. */
        private List<CatchUp> catchUps() {
            return writer.written;
        }

        /** The catch-ups member {@code member} was sent, in order. */
        private List<CatchUp> catchUps(final int member) {
            final List<CatchUp> sent = new ArrayList<>();
            for (final byte[] message : listeners.get(member).received) {
                sent.add(writer.written.get(message[0]));
            }
            return sent;
        }

        /** The registry the group's message is written with. */
        private DictionaryWriterRegistry registry() {
            return registry;
        }

        private void advance(final int member) {
            fanOut.advanceSubscriberDictionaries(member, registry);
        }
    }

    /** Synchronizes the dictionaries of a group of {@code members}. */
    private SynchronizedGroup synchronizeDictionaries(final DictionaryWriterRegistryImpl... members) {
        return synchronizeDictionaries(new CatchUpWriter(), IntList.of(), members);
    }

    /**
     * Synchronizes the dictionaries of a group of {@code members}, with catch-ups written by {@code writer}, once the
     * members {@code failedFirst} have been failed.
     */
    private SynchronizedGroup synchronizeDictionaries(
            final CatchUpWriter writer,
            final IntList failedFirst,
            final DictionaryWriterRegistryImpl... members) {
        final List<RecordingListener> listeners = new ArrayList<>(members.length);
        for (int ii = 0; ii < members.length; ++ii) {
            listeners.add(new RecordingListener());
        }
        final Failures failures = new Failures();
        final Copies copies = new Copies();
        final SharedMessageFanOut fanOut = new SharedMessageFanOut(new ArrayList<>(listeners), List.of(members),
                sharedDictionaries, failures, copies, new ImmediateJobScheduler(), Long.MAX_VALUE);
        for (int ii = 0; ii < failedFirst.size(); ++ii) {
            fanOut.failSubscriber(failedFirst.getInt(ii), new IllegalStateException("failed before synchronizing"));
        }
        final DictionaryWriterRegistry registry = fanOut.synchronizeDictionaries(writer, OPTS);
        return new SynchronizedGroup(fanOut, listeners, writer, registry, failures, copies);
    }

    @Test
    public void testGroupStartsAtTheHighestOffset() {
        final DictionaryWriterRegistryImpl behind = member();
        final DictionaryWriterRegistryImpl ahead = member();
        final DictionaryWriterRegistryImpl fresh = member();
        final DictionaryWriterRegistryImpl alsoFresh = member();

        add(shared, "a", "b", "c");
        state(behind);
        assertThat(send(behind)).containsExactly("a", "b", "c");
        add(shared, "d", "e");
        state(ahead);
        assertThat(send(ahead)).containsExactly("a", "b", "c", "d", "e");
        state(fresh);
        state(alsoFresh);

        final SynchronizedGroup group = synchronizeDictionaries(behind, ahead, fresh, alsoFresh);

        // the member behind is owed the values from its offset up to the highest; it is never sent them twice
        final List<CatchUp> behindCatchUps = group.catchUps(0);
        assertThat(behindCatchUps).hasSize(1);
        assertThat(behindCatchUps.get(0).isDelta()).isTrue();
        assertThat(behindCatchUps.get(0).values()).containsExactly("d", "e");

        // the member at the highest offset is owed nothing
        assertThat(group.catchUps(1)).isEmpty();

        // a member that needs a full batch is sent every value as a replacement, and the two of them share the one
        // catch-up
        final List<CatchUp> freshCatchUps = group.catchUps(2);
        assertThat(freshCatchUps).hasSize(1);
        assertThat(freshCatchUps.get(0).isDelta()).isFalse();
        assertThat(freshCatchUps.get(0).values()).containsExactly("a", "b", "c", "d", "e");
        assertThat(group.catchUps(3)).containsExactly(freshCatchUps.get(0));
        // built once for both
        assertThat(group.catchUps()).hasSize(2);
        // the catch-up the two fresh members share counts one copy beyond its write; the member behind's has none
        assertThat(group.copies.reports).containsExactly("bytes=1, copies=1");

        // the group's message carries only what is new beyond the highest offset
        final DictionaryWriterState groupState = state(group.registry());
        assertThat(groupState.needsFullBatch()).isFalse();
        assertThat(groupState.hasDelta()).isFalse();
        add(shared, "f");
        assertThat(send(group.registry())).containsExactly("f");
    }

    /**
     * A catch-up is built only if a member owed it is still receiving, and a failure building or writing one fails only
     * the members owed it.
     */
    @Test
    public void testCatchUpFailuresFailOnlyTheMembersOwedIt() {
        final DictionaryWriterRegistryImpl behind = member();
        final DictionaryWriterRegistryImpl ahead = member();
        final DictionaryWriterRegistryImpl fresh = member();
        add(shared, "a", "b", "c");
        state(behind);
        send(behind);
        add(shared, "d", "e");
        state(ahead);
        send(ahead);
        state(fresh);
        // the fresh member's replacement is built first, then the delta for the member behind

        // with the fresh member already failed, its replacement is never built
        final SynchronizedGroup freshFailed =
                synchronizeDictionaries(new CatchUpWriter(), IntList.of(2), behind, ahead, fresh);
        assertThat(freshFailed.catchUps()).extracting(CatchUp::isDelta).containsExactly(true);
        assertThat(freshFailed.catchUps(0)).containsExactlyElementsOf(freshFailed.catchUps());

        // a replacement that cannot be built fails only the fresh member; the member behind still gets its delta
        final CatchUpWriter failsBuilding = new CatchUpWriter();
        failsBuilding.failBuilding = 0;
        final SynchronizedGroup buildFailed =
                synchronizeDictionaries(failsBuilding, IntList.of(), behind, ahead, fresh);
        assertThat(buildFailed.failures.members).containsExactly(2);
        assertThat(buildFailed.catchUps(0)).extracting(CatchUp::values).containsExactly(List.of("d", "e"));

        // a delta that cannot be written fails only the member behind; the fresh member still gets its replacement
        final CatchUpWriter failsWriting = new CatchUpWriter();
        failsWriting.failWriting = 1;
        final SynchronizedGroup writeFailed = synchronizeDictionaries(failsWriting, IntList.of(), behind, ahead, fresh);
        assertThat(writeFailed.failures.members).containsExactly(0);
        assertThat(writeFailed.catchUps(2)).extracting(CatchUp::isDelta).containsExactly(false);
        assertThat(writeFailed.catchUps(1)).isEmpty();
    }

    @Test
    public void testAdvanceMovesEveryMemberToTheGroup() {
        final DictionaryWriterRegistryImpl behind = member();
        final DictionaryWriterRegistryImpl ahead = member();
        final DictionaryWriterRegistryImpl fresh = member();
        add(shared, "a", "b");
        state(behind);
        send(behind);
        add(shared, "c");
        state(ahead);
        send(ahead);
        state(fresh);

        final SynchronizedGroup group = synchronizeDictionaries(behind, ahead, fresh);
        add(shared, "d");
        assertThat(send(group.registry())).containsExactly("d");
        for (int mi = 0; mi < 3; ++mi) {
            group.advance(mi);
        }

        // each member, including the one that needed a full batch, now holds exactly what the group does
        for (final DictionaryWriterRegistryImpl member : List.of(behind, ahead, fresh)) {
            final DictionaryWriterState state = member.getEntry(ID).state;
            assertThat(state.needsFullBatch()).isFalse();
            assertThat(state.hasDelta()).isFalse();
        }
        add(shared, "e");
        for (final DictionaryWriterRegistryImpl member : List.of(behind, ahead, fresh)) {
            assertThat(send(member)).containsExactly("e");
        }
    }

    @Test
    public void testLevelMembersNeedNoCatchUps() {
        final DictionaryWriterRegistryImpl first = member();
        final DictionaryWriterRegistryImpl second = member();
        add(shared, "a", "b", "c");
        state(first);
        state(second);
        send(first);
        send(second);

        final SynchronizedGroup group = synchronizeDictionaries(first, second);
        assertThat(group.catchUps()).isEmpty();
        assertThat(state(group.registry()).hasDelta()).isFalse();
    }

    @Test
    public void testAfterAResetTheGroupReplacesEveryDictionary() {
        final DictionaryWriterRegistryImpl behind = member();
        final DictionaryWriterRegistryImpl ahead = member();
        add(shared, "a", "b");
        state(behind);
        send(behind);
        add(shared, "c");
        state(ahead);
        send(ahead);

        // the producer resets an outgrown dictionary before the phase's writes; every offset falls back to zero
        shared.reset();
        add(shared, "x");

        final SynchronizedGroup group = synchronizeDictionaries(behind, ahead);
        assertThat(group.catchUps()).isEmpty();
        final DictionaryWriterState groupState = state(group.registry());
        assertThat(groupState.needsFullBatch()).isTrue();
        assertThat(send(group.registry())).containsExactly("x");
    }

    @Test
    public void testMembersThatHoldNothingNeedNoCatchUps() {
        final DictionaryWriterRegistryImpl first = member();
        final DictionaryWriterRegistryImpl second = member();
        state(first);
        state(second);

        final SynchronizedGroup group = synchronizeDictionaries(first, second);
        assertThat(group.catchUps()).isEmpty();
        // the group's first batch defines the dictionary for every member, even with no values in it
        final DictionaryWriterState groupState = state(group.registry());
        assertThat(groupState.needsFullBatch()).isTrue();
        assertThat(groupState.hasDelta()).isTrue();
    }

    @Test
    public void testCatchUpsAreInDictionaryIdOrder() {
        final long otherId = 2;
        final SharedWriterDictionary other = new SharedWriterDictionary(otherId, ChunkType.Object);
        sharedDictionaries.put(otherId, other);

        final DictionaryWriterRegistryImpl behind = member();
        final DictionaryWriterRegistryImpl ahead = member();
        state(behind);
        behind.getOrCreate(otherId, VALUES_WRITER, ChunkType.Object);
        send(behind);
        add(shared, "a");
        add(other, "z");
        state(ahead);
        ahead.getOrCreate(otherId, VALUES_WRITER, ChunkType.Object);
        send(ahead);

        final SynchronizedGroup group = synchronizeDictionaries(behind, ahead);
        final List<CatchUp> catchUps = group.catchUps(0);
        assertThat(catchUps).extracting(CatchUp::getDictId).containsExactly(otherId, ID);
        assertThat(group.catchUps()).containsExactlyElementsOf(catchUps);
    }

    /**
     * Members subscribed to the same columns never reference a dictionary their columns leave out, so each holds it
     * empty; they need no catch-up for it, and the group's message sends none of its values.
     */
    @Test
    public void testAnUnreferencedDictionaryStaysEmpty() {
        final DictionaryWriterRegistryImpl first = member();
        final DictionaryWriterRegistryImpl second = member();
        state(first);
        state(second);
        // the first batch defines the dictionary, empty, for strict Arrow readers
        assertThat(sendUnreferenced(first)).isEmpty();
        add(shared, "a", "b");
        assertThat(sendUnreferenced(second)).isEmpty();
        add(shared, "c");

        final SynchronizedGroup group = synchronizeDictionaries(first, second);
        assertThat(group.catchUps()).isEmpty();
        assertThat(state(group.registry()).hasDelta()).isFalse();
        assertThat(sendUnreferenced(group.registry())).isEmpty();
        group.advance(0);
        group.advance(1);

        // once a batch does reference the dictionary, every value is sent, as a delta
        for (final DictionaryWriterRegistryImpl member : List.of(first, second)) {
            assertThat(member.getEntry(ID).state.needsFullBatch()).isFalse();
            assertThat(send(member)).containsExactly("a", "b", "c");
        }
    }

    /** Every member's snapshot registered every dictionary id, so a member without a state for one is a bug. */
    @Test
    public void testEveryMemberMustHaveAStateForEveryId() {
        final DictionaryWriterRegistryImpl first = member();
        final DictionaryWriterRegistryImpl missing = member();
        state(first);
        assertThatThrownBy(() -> synchronizeDictionaries(first, missing))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no state for dictionary id " + ID);
    }

    @Test
    public void testMembersMustShareTheProducersDictionaries() {
        final List<StreamObserver<BarrageMessageWriter.MessageView>> listeners =
                List.of(new RecordingListener(), new RecordingListener());
        final Failures failures = new Failures();
        assertThatThrownBy(() -> new SharedMessageFanOut(listeners,
                List.of(member(), new DictionaryWriterRegistryImpl()), sharedDictionaries, failures))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SharedMessageFanOut(listeners,
                List.of(member(), new DictionaryWriterRegistryImpl(new Long2ObjectOpenHashMap<>())),
                sharedDictionaries, failures))
                .isInstanceOf(IllegalArgumentException.class);
        // and there must be one for each member
        assertThatThrownBy(() -> new SharedMessageFanOut(listeners, List.of(member()), sharedDictionaries, failures))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
