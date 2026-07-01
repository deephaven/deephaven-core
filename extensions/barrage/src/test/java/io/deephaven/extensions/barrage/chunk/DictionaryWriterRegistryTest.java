//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.BarrageTypeInfo;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
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

    @Test
    public void testResetDeltasAdvancesBoundaryForAllEntries() {
        final DictionaryWriterRegistry manager = new DictionaryWriterRegistry();
        final ChunkWriter<Chunk<Values>> writer = intWriter();

        final DictionaryWriterState s1 = manager.getOrCreate(1L, writer, ChunkType.Int);
        final DictionaryWriterState s2 = manager.getOrCreate(2L, writer, ChunkType.Int);

        // Both states start with needsFullBatch=true, so hasDelta is true
        assertThat(manager.hasAnyDelta()).isTrue();

        manager.resetDeltas();
        assertThat(manager.hasAnyDelta()).isFalse();
        assertThat(s1.needsFullBatch()).isFalse();
        assertThat(s2.needsFullBatch()).isFalse();

        // Only s1 gets a new value — only s1 should report a delta
        s1.indexFor(42);
        assertThat(manager.hasAnyDelta()).isTrue();

        manager.resetDeltas();
        assertThat(manager.hasAnyDelta()).isFalse();
    }

    @Test
    public void testResetOverflowedEntriesCompactsOverLimitStates() {
        final DictionaryWriterRegistry manager = new DictionaryWriterRegistry();
        final ChunkWriter<Chunk<Values>> writer = intWriter();

        final DictionaryWriterState s1 = manager.getOrCreate(1L, writer, ChunkType.Int);
        final DictionaryWriterState s2 = manager.getOrCreate(2L, writer, ChunkType.Int);

        s1.indexFor("a");
        s1.indexFor("b");
        s1.indexFor("c");
        s1.resetDelta();

        s2.indexFor("x");
        s2.resetDelta();

        assertThat(s1.totalSize()).isEqualTo(3);
        assertThat(s2.totalSize()).isEqualTo(1);

        // liveRowCount=2 means s1 (3 entries > 2) overflows; s2 (1 entry) does not
        manager.resetOverflowedEntries(2);

        assertThat(s1.needsFullBatch()).isTrue();
        assertThat(s1.totalSize()).isZero();
        assertThat(s2.needsFullBatch()).isFalse();
        assertThat(s2.totalSize()).isEqualTo(1);
    }

    @Test
    public void testSharedBackedRegistryCreatesFullSubscriptionStatesWithSharedIndexes() {
        final Long2ObjectOpenHashMap<SharedDictionaryWriterState> sharedStates = new Long2ObjectOpenHashMap<>();
        final ChunkWriter<Chunk<Values>> writer = intWriter();

        final DictionaryWriterRegistry reg1 =
                new DictionaryWriterRegistry(sharedStates);
        final DictionaryWriterRegistry reg2 =
                new DictionaryWriterRegistry(sharedStates);

        final DictionaryWriterState sub1 = reg1.getOrCreate(5L, writer, ChunkType.Int);
        final DictionaryWriterState sub2 = reg2.getOrCreate(5L, writer, ChunkType.Int);

        // Both registries wrap the same SharedDictionaryWriterState for id=5
        assertThat(sharedStates).containsKey(5L);

        // Index assignments are shared: sub1 adds "alpha", sub2 sees the same index
        assertThat(sub1.indexFor("alpha")).isZero();
        assertThat(sub2.indexFor("alpha")).isZero();
        assertThat(sub2.indexFor("beta")).isEqualTo(1);
        assertThat(sub1.indexFor("beta")).isEqualTo(1);
    }

    @Test
    public void testSharedBackedRegistryReusesExistingSharedState() {
        final Long2ObjectOpenHashMap<SharedDictionaryWriterState> sharedStates = new Long2ObjectOpenHashMap<>();
        final ChunkWriter<Chunk<Values>> writer = intWriter();

        // Pre-populate the shared map with one entry
        final SharedDictionaryWriterState preExisting = new SharedDictionaryWriterState(7L);
        preExisting.indexFor("pre");
        sharedStates.put(7L, preExisting);

        final DictionaryWriterRegistry reg = new DictionaryWriterRegistry(sharedStates);
        final DictionaryWriterState sub = reg.getOrCreate(7L, writer, ChunkType.Int);

        // The registry must have re-used the pre-existing shared state
        assertThat(sharedStates.get(7L)).isSameAs(preExisting);

        // The new subscriber's first getDeltaValues() returns the pre-existing value
        assertThat(sub.needsFullBatch()).isTrue();
        assertThat(sub.getDeltaValues()).containsExactly("pre");
    }
}
