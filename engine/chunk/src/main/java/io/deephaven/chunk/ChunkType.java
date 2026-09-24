//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.factories.*;

import io.deephaven.util.SimpleTypeMap;
import org.jetbrains.annotations.NotNull;
import java.util.function.IntFunction;

public enum ChunkType implements ChunkFactory {

    // @formatter:off

    Boolean(new BooleanChunkFactory(), java.lang.Byte.BYTES),
    Char(new CharChunkFactory(), java.lang.Character.BYTES),
    Byte(new ByteChunkFactory(), java.lang.Byte.BYTES),
    Short(new ShortChunkFactory(), java.lang.Short.BYTES),
    Int(new IntChunkFactory(), java.lang.Integer.BYTES),
    Long(new LongChunkFactory(), java.lang.Long.BYTES),
    Float(new FloatChunkFactory(), java.lang.Float.BYTES),
    Double(new DoubleChunkFactory(), java.lang.Double.BYTES),
    Object(new ObjectChunkFactory(), java.lang.Long.BYTES);

    // @formatter:on

    private static final SimpleTypeMap<ChunkType> fromElementTypeMap = SimpleTypeMap.create(
            ChunkType.Boolean, ChunkType.Char, ChunkType.Byte, ChunkType.Short, ChunkType.Int,
            ChunkType.Long, ChunkType.Float, ChunkType.Double, ChunkType.Object);

    public static ChunkType fromElementType(Class elementType) {
        return fromElementTypeMap.get(elementType);
    }

    ChunkType(ChunkFactory factory, int elementBytes) {
        this.factory = factory;
        this.elementBytes = elementBytes;
    }

    final ChunkFactory factory;

    private final int elementBytes;

    /**
     * The width in bytes of one slot of a chunk's backing array. {@link #Object} reports the widest a reference can be,
     * so it over-estimates where the JVM uses compressed ordinary object pointers, and it never includes referents.
     */
    public final int elementBytes() {
        return elementBytes;
    }

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
