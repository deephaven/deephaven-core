//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.BarrageTypeInfo;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class DictionaryWriterRegistryTest {

    /** Returns a real (non-null) Int32 ChunkWriter usable as a placeholder valuesWriter. */
    @SuppressWarnings("unchecked")
    private static ChunkWriter<Chunk<Values>> intWriter() {
        final org.apache.arrow.vector.types.pojo.Schema pojoSchema =
                new org.apache.arrow.vector.types.pojo.Schema(Collections.singletonList(
                        new Field("x", FieldType.notNullable(new ArrowType.Int(32, true)),
                                Collections.emptyList())));
        final byte[] schemaBytes = pojoSchema.serializeAsMessage();
        final org.apache.arrow.flatbuf.Schema fbSchema =
                io.deephaven.proto.flight.util.SchemaHelper.flatbufSchema(ByteBuffer.wrap(schemaBytes));
        final org.apache.arrow.flatbuf.Field fbField = fbSchema.fields(0);
        return (ChunkWriter<Chunk<Values>>) (ChunkWriter<?>) DefaultChunkWriterFactory.INSTANCE
                .newWriter(BarrageTypeInfo.make(int.class, null, fbField));
    }

    @Test
    public void testGetOrCreateReusesStateForSameId() {
        final DictionaryWriterRegistry manager = new DictionaryWriterRegistry();
        final ChunkWriter<Chunk<Values>> writer = intWriter();

        final DictionaryWriterState first = manager.getOrCreate(7L, writer, ChunkType.Int);
        first.resetDelta();
        final DictionaryWriterState second = manager.getOrCreate(7L, writer, ChunkType.Int);

        assertThat(second).isSameAs(first);
        assertThat(manager.entries()).hasSize(1);
    }

    @Test
    public void testHasAnyDeltaReflectsManagedStates() {
        final DictionaryWriterRegistry manager = new DictionaryWriterRegistry();
        final ChunkWriter<Chunk<Values>> writer = intWriter();

        assertThat(manager.hasAnyDelta()).isFalse();

        final DictionaryWriterState first = manager.getOrCreate(1L, writer, ChunkType.Int);
        assertThat(manager.hasAnyDelta()).isTrue();

        first.resetDelta();
        assertThat(manager.hasAnyDelta()).isFalse();

        final DictionaryWriterState second = manager.getOrCreate(2L, writer, ChunkType.Int);
        second.resetDelta();
        assertThat(manager.hasAnyDelta()).isFalse();

        second.indexFor("new-value");
        assertThat(manager.hasAnyDelta()).isTrue();
    }
}
