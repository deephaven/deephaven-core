//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory;
import io.deephaven.extensions.barrage.chunk.DictionaryChunkWriter;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistryImpl;
import io.deephaven.extensions.barrage.util.ExposedByteArrayOutputStream;
import io.deephaven.proto.flight.util.SchemaHelper;
import org.apache.arrow.flatbuf.DictionaryBatch;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import io.deephaven.chunk.ChunkType;
import io.deephaven.extensions.barrage.util.BarrageMessageReaderImpl;
import java.io.ByteArrayInputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A producer's message carries data for every column that any of its subscriptions needs; each subscription's view of
 * it writes only that subscription's columns. A standalone DictionaryBatch, which a producer sends to bring a member of
 * a group of subscriptions level with the others, is the same message the writer builds for those values in a stream.
 */
public class BarrageMessageWriterImplTest {

    private static final BarrageSubscriptionOptions OPTIONS = BarrageSubscriptionOptions.builder().build();
    private static final long DICT_ID = 7;
    private static final Class<?>[] TYPES = {int.class, String.class, String.class};

    /** An int column, a string column, and a dictionary encoded string column. */
    private static final Schema SCHEMA = new Schema(List.of(
            new Field("I", FieldType.nullable(new ArrowType.Int(32, true)), List.of()),
            new Field("S", FieldType.nullable(ArrowType.Utf8.INSTANCE), List.of()),
            new Field("D", new FieldType(true, ArrowType.Utf8.INSTANCE,
                    new DictionaryEncoding(DICT_ID, false, new ArrowType.Int(32, true))), List.of())));

    /**
     * A full subscription's view of an update leaves out the columns the subscription did not subscribe to: it writes
     * exactly what it would write were the update to hold no data for them, an empty field node for each, no modified
     * rows, and an empty dictionary.
     */
    @Test
    public void testFullViewOfAnUpdateWritesOnlyItsColumns() throws IOException {
        final List<byte[]> intColumnOnly;
        final List<byte[]> everyColumn;
        try (final BarrageMessageWriter writer = new BarrageMessageWriterImpl(
                delta(columns(0, 1, 2)), chunkWriters(), BarrageMessageWriter.WriteMetricsConsumer.NO_OP)) {
            intColumnOnly = drain(writer.getSubView(OPTIONS, false, true, null, false, null, null, columns(0),
                    new DictionaryWriterRegistryImpl()));
            everyColumn = drain(writer.getSubView(OPTIONS, false, true, null, false, null, null, columns(0, 1, 2),
                    new DictionaryWriterRegistryImpl()));
        }

        final List<byte[]> updateOfIntColumn;
        try (final BarrageMessageWriter writer = new BarrageMessageWriterImpl(
                delta(columns(0)), chunkWriters(), BarrageMessageWriter.WriteMetricsConsumer.NO_OP)) {
            updateOfIntColumn = drain(writer.getSubView(OPTIONS, false, true, null, false, null, null,
                    columns(0, 1, 2), new DictionaryWriterRegistryImpl()));
        }

        assertThat(intColumnOnly).hasSameSizeAs(updateOfIntColumn);
        for (int si = 0; si < intColumnOnly.size(); ++si) {
            assertThat(intColumnOnly.get(si)).isEqualTo(updateOfIntColumn.get(si));
        }
        assertThat(totalBytes(intColumnOnly)).isLessThan(totalBytes(everyColumn));
    }

    /** A standalone DictionaryBatch is byte for byte the batch the writer sends before a RecordBatch needing it. */
    @Test
    public void testDictionaryBatchViewMatchesTheStreamsDictionaryBatch() throws IOException {
        final ChunkWriter<Chunk<Values>>[] writers = chunkWriters();
        final DictionaryChunkWriter dictionaryWriter = (DictionaryChunkWriter) (ChunkWriter<?>) writers[2];
        try (final BarrageMessageWriter writer = new BarrageMessageWriterImpl(
                delta(columns(0, 1, 2)), writers, BarrageMessageWriter.WriteMetricsConsumer.NO_OP)) {
            // a fresh registry sends every value its first batch refers to as a replacement, before that batch
            final List<byte[]> streams = drain(writer.getSubView(OPTIONS, false, true, null, false, null, null,
                    columns(0, 1, 2), new DictionaryWriterRegistryImpl()));
            assertThat(flightMessage(streams.get(0)).headerType()).isEqualTo(MessageHeader.DictionaryBatch);

            final List<byte[]> standalone = drain(writer.getDictionaryBatchView(OPTIONS, DICT_ID,
                    dictionaryWriter.getValuesWriter(), strings("x", "y", "z"), false));
            assertThat(standalone).hasSize(1);
            assertThat(standalone.get(0)).isEqualTo(streams.get(0));

            final DictionaryBatch delta = dictionaryBatch(drain(writer.getDictionaryBatchView(OPTIONS, DICT_ID,
                    dictionaryWriter.getValuesWriter(), strings("w"), true)).get(0));
            assertThat(delta.id()).isEqualTo(DICT_ID);
            assertThat(delta.isDelta()).isTrue();
            assertThat(delta.data().length()).isEqualTo(1);
        }
    }

    /** The Arrow IPC writer frames a standalone DictionaryBatch as an IPC message, like its other messages. */
    @Test
    public void testArrowFactoryFramesDictionaryBatchViewAsIpc() throws IOException {
        final ChunkWriter<Chunk<Values>>[] writers = chunkWriters();
        final DictionaryChunkWriter dictionaryWriter = (DictionaryChunkWriter) (ChunkWriter<?>) writers[2];
        try (final BarrageMessageWriter writer = new BarrageMessageWriterImpl.ArrowFactory()
                .newMessageWriter(delta(columns(0, 1, 2)), writers, BarrageMessageWriter.WriteMetricsConsumer.NO_OP)) {
            final List<byte[]> streams = drain(writer.getDictionaryBatchView(OPTIONS, DICT_ID,
                    dictionaryWriter.getValuesWriter(), strings("x", "y"), true));
            assertThat(streams).hasSize(1);
            final ByteBuffer ipc = ByteBuffer.wrap(streams.get(0)).order(ByteOrder.LITTLE_ENDIAN);
            assertThat(ipc.getInt()).isEqualTo(-1); // the continuation marker
            final int metadataLength = ipc.getInt();
            final ByteBuffer metadata = ipc.slice().order(ByteOrder.LITTLE_ENDIAN);
            metadata.limit(metadataLength);
            final Message message = Message.getRootAsMessage(metadata);
            assertThat(message.headerType()).isEqualTo(MessageHeader.DictionaryBatch);
            final DictionaryBatch batch = (DictionaryBatch) message.header(new DictionaryBatch());
            assertThat(batch.id()).isEqualTo(DICT_ID);
            assertThat(batch.isDelta()).isTrue();
            assertThat(batch.data().length()).isEqualTo(2);
        }
    }

    /** A view writes its message once; a second visit fails, rather than reading what the first released. */
    @Test
    public void testAViewMayBeVisitedOnlyOnce() throws IOException {
        final ChunkWriter<Chunk<Values>>[] writers = chunkWriters();
        final DictionaryChunkWriter dictionaryWriter = (DictionaryChunkWriter) (ChunkWriter<?>) writers[2];
        try (final BarrageMessageWriter writer = new BarrageMessageWriterImpl(
                delta(columns(0, 1, 2)), writers, BarrageMessageWriter.WriteMetricsConsumer.NO_OP)) {
            final BarrageMessageWriter.MessageView subView = writer.getSubView(OPTIONS, false, true, null, false, null,
                    null, columns(0, 1, 2), new DictionaryWriterRegistryImpl());
            assertThat(drain(subView)).isNotEmpty();
            assertThatThrownBy(() -> drain(subView)).isInstanceOf(IllegalStateException.class);

            final BarrageMessageWriter.MessageView dictionaryView = writer.getDictionaryBatchView(OPTIONS, DICT_ID,
                    dictionaryWriter.getValuesWriter(), strings("x"), true);
            assertThat(drain(dictionaryView)).hasSize(1);
            assertThatThrownBy(() -> drain(dictionaryView)).isInstanceOf(IllegalStateException.class);
        }
    }

    /**
     * A dictionary batch that would reach the maximum message size is split by position into several under it: the
     * first keeps the replacement flag, the rest append, and together they carry every value once.
     */
    @Test
    public void testALargeDictionaryBatchIsSplitUnderTheMessageLimit() throws IOException {
        final int limit = 2048;
        final BarrageSubscriptionOptions small = BarrageSubscriptionOptions.builder().maxMessageSize(limit).build();
        final String[] values = new String[1000];
        for (int ii = 0; ii < values.length; ++ii) {
            values[ii] = String.format("value-%05d", ii);
        }
        final ChunkWriter<Chunk<Values>>[] writers = chunkWriters();
        final DictionaryChunkWriter dictionaryWriter = (DictionaryChunkWriter) (ChunkWriter<?>) writers[2];
        try (final BarrageMessageWriter writer = new BarrageMessageWriterImpl(
                delta(columns(0, 1, 2)), writers, BarrageMessageWriter.WriteMetricsConsumer.NO_OP)) {
            final List<byte[]> messages = drain(writer.getDictionaryBatchView(small, DICT_ID,
                    dictionaryWriter.getValuesWriter(), strings(values), false));
            assertThat(messages.size()).isGreaterThan(1);
            int total = 0;
            for (int mi = 0; mi < messages.size(); ++mi) {
                assertThat(messages.get(mi).length).isLessThan(limit);
                final DictionaryBatch batch = dictionaryBatch(messages.get(mi));
                assertThat(batch.id()).isEqualTo(DICT_ID);
                assertThat(batch.isDelta()).isEqualTo(mi > 0);
                total += batch.data().length();
            }
            assertThat(total).isEqualTo(values.length);
        }
    }

    /**
     * A write records every byte it sends, its dictionary batches included, whether it carries rows or not, and a
     * standalone dictionary batch view records a write of its own.
     */
    @Test
    public void testAWriteRecordsEveryByteItSends() throws IOException {
        final List<Long> recorded = new ArrayList<>();
        final ChunkWriter<Chunk<Values>>[] writers = chunkWriters();
        final DictionaryChunkWriter dictionaryWriter = (DictionaryChunkWriter) (ChunkWriter<?>) writers[2];
        try (final BarrageMessageWriter writer = new BarrageMessageWriterImpl(
                delta(columns(0, 1, 2)), writers, (bytes, nanos) -> recorded.add(bytes))) {
            // a fresh registry sends a dictionary batch ahead of the first record batch
            final List<byte[]> withRows = drain(writer.getSubView(OPTIONS, false, true, null, false, null, null,
                    columns(0, 1, 2), new DictionaryWriterRegistryImpl()));
            assertThat(withRows.stream().map(BarrageMessageWriterImplTest::headerType))
                    .contains(MessageHeader.DictionaryBatch, MessageHeader.RecordBatch);
            assertThat(recorded).containsExactly(totalBytes(withRows));

            // an empty viewport includes no rows, so the write takes the path for a message with no data
            recorded.clear();
            final List<byte[]> withoutRows;
            try (final RowSet viewport = RowSetFactory.empty();
                    final RowSet keyspaceViewportPrev = RowSetFactory.empty();
                    final RowSet keyspaceViewport = RowSetFactory.empty()) {
                withoutRows = drain(writer.getSubView(OPTIONS, false, false, viewport, false,
                        keyspaceViewportPrev, keyspaceViewport, columns(0, 1, 2), new DictionaryWriterRegistryImpl()));
            }
            assertThat(withoutRows.stream().map(BarrageMessageWriterImplTest::headerType))
                    .contains(MessageHeader.DictionaryBatch);
            assertThat(recorded).containsExactly(totalBytes(withoutRows));

            recorded.clear();
            final List<byte[]> catchUp = drain(writer.getDictionaryBatchView(OPTIONS, DICT_ID,
                    dictionaryWriter.getValuesWriter(), strings("x", "y"), true));
            assertThat(recorded).containsExactly(totalBytes(catchUp));
        }
    }

    /** No dictionary batch carries more values than the batch size, however far under the message limit it is. */
    @Test
    public void testADictionaryBatchCarriesNoMoreValuesThanTheBatchSize() throws IOException {
        final BarrageSubscriptionOptions batchesOf100 = BarrageSubscriptionOptions.builder().batchSize(100).build();
        final String[] values = new String[1000];
        for (int ii = 0; ii < values.length; ++ii) {
            values[ii] = String.format("value-%05d", ii);
        }
        final ChunkWriter<Chunk<Values>>[] writers = chunkWriters();
        final DictionaryChunkWriter dictionaryWriter = (DictionaryChunkWriter) (ChunkWriter<?>) writers[2];
        try (final BarrageMessageWriter writer = new BarrageMessageWriterImpl(
                delta(columns(0, 1, 2)), writers, BarrageMessageWriter.WriteMetricsConsumer.NO_OP)) {
            final List<byte[]> messages = drain(writer.getDictionaryBatchView(batchesOf100, DICT_ID,
                    dictionaryWriter.getValuesWriter(), strings(values), false));
            assertThat(messages).hasSize(10);
            for (int mi = 0; mi < messages.size(); ++mi) {
                final DictionaryBatch batch = dictionaryBatch(messages.get(mi));
                assertThat(batch.data().length()).isEqualTo(100);
                assertThat(batch.isDelta()).isEqualTo(mi > 0);
            }
        }
    }

    /** A dictionary value too large for a message under the limit is sent in a message of its own. */
    @Test
    public void testADictionaryValueTooLargeForTheMessageLimitIsSentAlone() throws IOException {
        final int limit = 1024;
        final BarrageSubscriptionOptions small = BarrageSubscriptionOptions.builder().maxMessageSize(limit).build();
        final ChunkWriter<Chunk<Values>>[] writers = chunkWriters();
        final DictionaryChunkWriter dictionaryWriter = (DictionaryChunkWriter) (ChunkWriter<?>) writers[2];
        try (final BarrageMessageWriter writer = new BarrageMessageWriterImpl(
                delta(columns(0, 1, 2)), writers, BarrageMessageWriter.WriteMetricsConsumer.NO_OP)) {
            final List<byte[]> messages = drain(writer.getDictionaryBatchView(small, DICT_ID,
                    dictionaryWriter.getValuesWriter(), strings("a", "x".repeat(4 * limit), "b"), true));
            assertThat(messages).hasSize(3);
            for (int mi = 0; mi < messages.size(); ++mi) {
                final DictionaryBatch batch = dictionaryBatch(messages.get(mi));
                assertThat(batch.data().length()).isEqualTo(1);
                assertThat(batch.isDelta()).isTrue();
            }
            assertThat(messages.get(0).length).isLessThan(limit);
            assertThat(messages.get(1).length).isGreaterThan(limit);
            assertThat(messages.get(2).length).isLessThan(limit);
        }
    }

    /**
     * Closing a reader while it parses a message waits for that parse, and every parse after it returns nothing, as a
     * subscription cancelled from another thread needs: closing releases the message the reader had only partly read,
     * so it must not run while the parser still writes into that message.
     */
    @Test
    public void testClosingTheReaderWaitsForAParseInProgress() throws Exception {
        final List<byte[]> schema = drain(new BarrageMessageWriterImpl.Factory().getSchemaView(SCHEMA::getSchema));
        final List<byte[]> update;
        try (final BarrageMessageWriter writer = new BarrageMessageWriterImpl(
                delta(columns(0, 1, 2)), chunkWriters(), BarrageMessageWriter.WriteMetricsConsumer.NO_OP)) {
            // batches of four rows, so the update's six added and three modified rows take several record batches
            update = drain(writer.getSubView(BarrageSubscriptionOptions.builder().batchSize(4).build(), false, true,
                    null, false, null, null, columns(0, 1, 2), new DictionaryWriterRegistryImpl()));
        }
        int firstRecordBatch = 0;
        while (flightMessage(update.get(firstRecordBatch)).headerType() != MessageHeader.RecordBatch) {
            ++firstRecordBatch;
        }
        final int recordBatchIndex = firstRecordBatch;
        assertThat(update.stream().skip(recordBatchIndex + 1)
                .filter(bytes -> headerType(bytes) == MessageHeader.RecordBatch).count()).isPositive();

        // the reader reports each record batch's parse time before it returns; that report holds one parse in progress
        final AtomicBoolean holdNextParse = new AtomicBoolean();
        final CountDownLatch parsing = new CountDownLatch(1);
        final CountDownLatch finishParse = new CountDownLatch(1);
        final BarrageMessageReaderImpl reader = new BarrageMessageReaderImpl(nanos -> {
            if (holdNextParse.getAndSet(false)) {
                parsing.countDown();
                try {
                    finishParse.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        final ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            assertThat(parse(reader, schema.get(0))).isNull();
            for (int mi = 0; mi < recordBatchIndex; ++mi) {
                assertThat(parse(reader, update.get(mi))).isNull();
            }

            holdNextParse.set(true);
            final Future<BarrageMessage> parse = threads.submit(() -> parse(reader, update.get(recordBatchIndex)));
            assertThat(parsing.await(10, TimeUnit.SECONDS)).isTrue();
            final Future<?> closing = threads.submit(reader::close);
            assertThatThrownBy(() -> closing.get(200, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);

            finishParse.countDown();
            // the first of several record batches: the message is not complete yet
            assertThat(parse.get(10, TimeUnit.SECONDS)).isNull();
            closing.get(10, TimeUnit.SECONDS);

            // closed: the rest of the update, which would complete the message, parses to nothing
            for (int mi = recordBatchIndex + 1; mi < update.size(); ++mi) {
                assertThat(parse(reader, update.get(mi))).isNull();
            }
        } finally {
            finishParse.countDown();
            threads.shutdownNow();
        }
    }

    private static BarrageMessage parse(final BarrageMessageReaderImpl reader, final byte[] flightData) {
        return reader.safelyParseFrom(OPTIONS, new ChunkType[] {ChunkType.Int, ChunkType.Object, ChunkType.Object},
                TYPES, new Class<?>[TYPES.length], new ByteArrayInputStream(flightData));
    }

    private static byte headerType(final byte[] flightData) {
        try {
            return flightMessage(flightData).headerType();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Message flightMessage(final byte[] flightData) throws IOException {
        final Flight.FlightData data = Flight.FlightData.parseFrom(flightData);
        return Message.getRootAsMessage(data.getDataHeader().asReadOnlyByteBuffer());
    }

    private static DictionaryBatch dictionaryBatch(final byte[] flightData) throws IOException {
        final Message message = flightMessage(flightData);
        assertThat(message.headerType()).isEqualTo(MessageHeader.DictionaryBatch);
        return (DictionaryBatch) message.header(new DictionaryBatch());
    }

    @SuppressWarnings("unchecked")
    private static ChunkWriter<Chunk<Values>>[] chunkWriters() {
        final org.apache.arrow.flatbuf.Schema schema =
                SchemaHelper.flatbufSchema(ByteBuffer.wrap(SCHEMA.serializeAsMessage()));
        final ChunkWriter<Chunk<Values>>[] writers = new ChunkWriter[TYPES.length];
        for (int ci = 0; ci < TYPES.length; ++ci) {
            writers[ci] = (ChunkWriter<Chunk<Values>>) (ChunkWriter<?>) DefaultChunkWriterFactory.INSTANCE
                    .newWriter(BarrageTypeInfo.make(TYPES[ci], null, schema.fields(ci)));
        }
        return writers;
    }

    /**
     * A propagated update, holding data for {@code columnsWithData} alone: rows 20 through 25 added, rows 0 and 1
     * removed, and rows 5 through 7 modified.
     */
    private static BarrageMessage delta(final BitSet columnsWithData) {
        final BarrageMessage message = new BarrageMessage();
        message.firstSeq = message.lastSeq = 3;
        message.isSnapshot = false;
        message.tableSize = 24;
        message.rowsAdded = RowSetFactory.fromRange(20, 25);
        message.rowsIncluded = RowSetFactory.fromRange(20, 25);
        message.rowsRemoved = RowSetFactory.fromRange(0, 1);
        message.shifted = RowSetShiftData.EMPTY;
        final List<WritableChunk<Values>> added = List.of(
                ints(1, 2, 3, 4, 5, 6),
                strings("a", null, "c", "d", "e", "f"),
                strings("x", "y", "x", null, "y", "z"));
        final List<WritableChunk<Values>> modified = List.of(
                ints(7, 8, 9),
                strings("g", "h", null),
                strings("z", "x", "w"));
        message.addColumnData = new BarrageMessage.AddColumnData[TYPES.length];
        message.modColumnData = new BarrageMessage.ModColumnData[TYPES.length];
        for (int ci = 0; ci < TYPES.length; ++ci) {
            final boolean hasData = columnsWithData.get(ci);

            final BarrageMessage.AddColumnData addColumn = new BarrageMessage.AddColumnData();
            addColumn.type = TYPES[ci];
            addColumn.chunkType = added.get(ci).getChunkType();
            addColumn.data = new ArrayList<>();
            final BarrageMessage.ModColumnData modColumn = new BarrageMessage.ModColumnData();
            modColumn.type = TYPES[ci];
            modColumn.chunkType = modified.get(ci).getChunkType();
            modColumn.data = new ArrayList<>();
            if (hasData) {
                addColumn.data.add(added.get(ci));
                modColumn.rowsModified = RowSetFactory.fromRange(5, 7);
                modColumn.data.add(modified.get(ci));
            } else {
                added.get(ci).close();
                modColumn.rowsModified = RowSetFactory.empty();
                modified.get(ci).close();
            }
            message.addColumnData[ci] = addColumn;
            message.modColumnData[ci] = modColumn;
        }
        return message;
    }

    private static WritableChunk<Values> ints(final int... values) {
        final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(values.length);
        chunk.copyFromTypedArray(values, 0, 0, values.length);
        return chunk;
    }

    private static WritableChunk<Values> strings(final String... values) {
        final WritableObjectChunk<String, Values> chunk = WritableObjectChunk.makeWritableChunk(values.length);
        chunk.copyFromTypedArray(values, 0, 0, values.length);
        return chunk;
    }

    private static BitSet columns(final int... columns) {
        final BitSet set = new BitSet();
        for (final int column : columns) {
            set.set(column);
        }
        return set;
    }

    /** Every stream {@code view} writes, as bytes. */
    private static List<byte[]> drain(final BarrageMessageWriter.MessageView view) throws IOException {
        final List<byte[]> streams = new ArrayList<>();
        view.forEachStream(stream -> {
            try (final ExposedByteArrayOutputStream out = new ExposedByteArrayOutputStream()) {
                stream.drainTo(out);
                stream.close();
                streams.add(Arrays.copyOf(out.peekBuffer(), out.size()));
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        });
        return streams;
    }

    private static long totalBytes(final List<byte[]> streams) {
        return streams.stream().mapToLong(stream -> stream.length).sum();
    }
}
