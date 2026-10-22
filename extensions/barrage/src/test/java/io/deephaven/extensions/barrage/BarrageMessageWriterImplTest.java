//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistryImpl;
import io.deephaven.extensions.barrage.util.ExposedByteArrayOutputStream;
import io.deephaven.proto.flight.util.SchemaHelper;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A producer's message carries data for every column that any of its subscriptions needs; each subscription's view of
 * it writes only that subscription's columns.
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
