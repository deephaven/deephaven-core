//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.barrage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageMessageWriterImpl;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.util.BarrageMessageReaderImpl;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.ExposedByteArrayOutputStream;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * The serialize, compress, decompress and deserialize steps the compression benchmarks time, shared with
 * {@link BarrageCompressionSizeReport}.
 * <p>
 * The uncompressed messages are exactly what {@code DoGet} sends for a static table: a schema message followed by the
 * record batches {@link BarrageUtil#createAndSendSnapshot} produces with {@link BarrageUtil#DEFAULT_SNAPSHOT_OPTIONS}.
 * Compression is applied to each whole {@code FlightData} message, as gRPC does.
 */
final class BarrageCompressionHarness {
    private final Table table;
    private final ChunkType[] wireChunkTypes;
    private final Class<?>[] wireTypes;
    private final Class<?>[] wireComponentTypes;

    BarrageCompressionHarness(final Table table) {
        this.table = table;
        final BarrageUtil.ConvertedArrowSchema schema =
                BarrageUtil.convertArrowSchema(BarrageUtil.schemaFromTable(table));
        wireChunkTypes = schema.computeWireChunkTypes();
        wireTypes = schema.computeWireTypes();
        wireComponentTypes = schema.computeWireComponentTypes();
    }

    /**
     * @return the uncompressed {@code FlightData} messages {@code DoGet} sends for the table
     */
    List<byte[]> serialize() {
        final List<byte[]> messages = new ArrayList<>();
        final BarrageMessageWriter.Factory factory = new BarrageMessageWriterImpl.Factory();
        final StreamObserver<BarrageMessageWriter.MessageView> listener = new MessageCollector(messages::add);

        listener.onNext(factory.getSchemaView(
                fbb -> BarrageUtil.makeTableSchemaPayload(fbb, BarrageUtil.DEFAULT_SNAPSHOT_OPTIONS, table)));
        BarrageUtil.createAndSendSnapshot(factory, (BaseTable<?>) table, null, null, false,
                BarrageUtil.DEFAULT_SNAPSHOT_OPTIONS, listener, new BarragePerformanceLog.SnapshotMetricsHelper());
        return messages;
    }

    static List<byte[]> compressAll(final List<byte[]> flightData, final MessageCodec codec) {
        final List<byte[]> wire = new ArrayList<>(flightData.size());
        for (final byte[] message : flightData) {
            wire.add(codec.compress(message));
        }
        return wire;
    }

    /**
     * Reads wire messages back into {@link BarrageMessage}s with the same reader the Java client uses.
     *
     * @param onMessage receives each completed message; the caller must close it
     */
    void deserialize(final List<byte[]> wire, final MessageCodec codec, final Consumer<BarrageMessage> onMessage) {
        final BarrageMessageReaderImpl reader = new BarrageMessageReaderImpl();
        for (final byte[] wireMessage : wire) {
            final BarrageMessage message = reader.safelyParseFrom(BarrageUtil.DEFAULT_SNAPSHOT_OPTIONS,
                    wireChunkTypes, wireTypes, wireComponentTypes,
                    new ByteArrayInputStream(codec.decompress(wireMessage)));
            if (message != null) {
                onMessage.accept(message);
            }
        }
    }

    /**
     * Checks that {@code wire} decodes back to {@code flightData} byte for byte and that the reader recovers every row.
     *
     * @throws IllegalStateException if the round trip is lossy
     */
    void verify(final List<byte[]> flightData, final List<byte[]> wire, final MessageCodec codec) {
        if (flightData.size() != wire.size()) {
            throw new IllegalStateException("message count changed: " + flightData.size() + " -> " + wire.size());
        }
        for (int mi = 0; mi < flightData.size(); ++mi) {
            if (!Arrays.equals(flightData.get(mi), codec.decompress(wire.get(mi)))) {
                throw new IllegalStateException("message " + mi + " did not round trip with " + codec);
            }
        }

        final long[] rowsRead = new long[1];
        deserialize(wire, codec, message -> {
            try (message) {
                rowsRead[0] += message.rowsIncluded.size();
            }
        });
        if (rowsRead[0] != table.size()) {
            throw new IllegalStateException("read " + rowsRead[0] + " rows; expected " + table.size());
        }
    }

    /**
     * Drains each {@link BarrageMessageWriter.MessageView} into the bytes the server would hand to gRPC.
     */
    private static final class MessageCollector implements StreamObserver<BarrageMessageWriter.MessageView> {
        private final Consumer<byte[]> onMessage;

        private MessageCollector(final Consumer<byte[]> onMessage) {
            this.onMessage = onMessage;
        }

        @Override
        public void onNext(final BarrageMessageWriter.MessageView view) {
            try {
                view.forEachStream(stream -> {
                    try (final ExposedByteArrayOutputStream out = new ExposedByteArrayOutputStream()) {
                        stream.drainTo(out);
                        stream.close();
                        onMessage.accept(Arrays.copyOf(out.peekBuffer(), out.size()));
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
            throw new IllegalStateException(t);
        }

        @Override
        public void onCompleted() {}
    }
}
