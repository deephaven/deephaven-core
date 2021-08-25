package io.deephaven.db.v2.sources.chunk;

import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.v2.sources.chunk.page.ChunkPage;
import io.deephaven.db.v2.sources.chunk.util.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.util.factories.*;

import org.jetbrains.annotations.NotNull;
import java.util.function.IntFunction;

public enum ChunkType implements ChunkFactory {
    Boolean(new BooleanChunkFactory()), Char(new CharChunkFactory()), Byte(new ByteChunkFactory()), Short(
            new ShortChunkFactory()), Int(new IntChunkFactory()), Long(new LongChunkFactory()), Float(
                    new FloatChunkFactory()), Double(new DoubleChunkFactory()), Object(new ObjectChunkFactory());

    private static final SimpleTypeMap<ChunkType> fromElementTypeMap = SimpleTypeMap.create(
            ChunkType.Boolean, ChunkType.Char, ChunkType.Byte, ChunkType.Short, ChunkType.Int,
            ChunkType.Long, ChunkType.Float, ChunkType.Double, ChunkType.Object);

    public static ChunkType fromElementType(Class elementType) {
        return fromElementTypeMap.get(elementType);
    }

    ChunkType(ChunkFactory factory) {
        this.factory = factory;
    }

    final ChunkFactory factory;

    // Convenience methods
    @NotNull
    @Override
    public final Object makeArray(int capacity) {
        return factory.makeArray(capacity);
    }

    @NotNull
    @Override
    public <ATTR extends Any> Chunk<ATTR>[] makeChunkArray(int capacity) {
        return factory.makeChunkArray(capacity);
    }

    @NotNull
    @Override
    public <ATTR extends Any> Chunk<ATTR> getEmptyChunk() {
        return factory.getEmptyChunk();
    }

    @NotNull
    @Override
    public <ATTR extends Any> ChunkChunk<ATTR> getEmptyChunkChunk() {
        return factory.getEmptyChunkChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> Chunk<ATTR> chunkWrap(Object array) {
        return factory.chunkWrap(array);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> Chunk<ATTR> chunkWrap(Object array, int offset, int capacity) {
        return factory.chunkWrap(array, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array) {
        return factory.chunkChunkWrap(array);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array, int offset, int capacity) {
        return factory.chunkChunkWrap(array, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ChunkPage<ATTR> pageWrap(long beginRow, Object array, long mask) {
        return factory.pageWrap(beginRow, array, mask);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ChunkPage<ATTR> pageWrap(long beginRow, Object array, int offset, int capacity,
            long mask) {
        return factory.pageWrap(beginRow, array, offset, capacity, mask);
    }

    @NotNull
    @Override
    public DbArrayBase dbArrayWrap(Object array) {
        return factory.dbArrayWrap(array);
    }

    @NotNull
    @Override
    public DbArrayBase dbArrayWrap(Object array, int offset, int capacity) {
        return factory.dbArrayWrap(array, offset, capacity);
    }

    @NotNull
    @Override
    public <ATTR extends Any> ResettableReadOnlyChunk<ATTR> makeResettableReadOnlyChunk() {
        return factory.makeResettableReadOnlyChunk();
    }

    @NotNull
    @Override
    public <ATTR extends Any> ResettableChunkChunk<ATTR> makeResettableChunkChunk() {
        return factory.makeResettableChunkChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableChunk<ATTR> makeWritableChunk(int capacity) {
        return factory.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableChunkChunk<ATTR> makeWritableChunkChunk(int capacity) {
        return factory.makeWritableChunkChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableChunk<ATTR> writableChunkWrap(Object array, int offset, int capacity) {
        return factory.writableChunkWrap(array, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableChunkChunk<ATTR> writableChunkChunkWrap(WritableChunk<ATTR>[] array,
            int offset, int capacity) {
        return factory.writableChunkChunkWrap(array, offset, capacity);
    }

    @NotNull
    @Override
    public <ATTR extends Any> ResettableWritableChunk<ATTR> makeResettableWritableChunk() {
        return factory.makeResettableWritableChunk();
    }

    @NotNull
    @Override
    public <ATTR extends Any> ResettableWritableChunkChunk<ATTR> makeResettableWritableChunkChunk() {
        return factory.makeResettableWritableChunkChunk();
    }

    @NotNull
    @Override
    public IntFunction<Chunk[]> chunkArrayBuilder() {
        return factory.chunkArrayBuilder();
    }

    @NotNull
    @Override
    public IntFunction<WritableChunk[]> writableChunkArrayBuilder() {
        return factory.writableChunkArrayBuilder();
    }
}
