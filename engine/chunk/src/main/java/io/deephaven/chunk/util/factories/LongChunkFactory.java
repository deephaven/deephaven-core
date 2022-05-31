/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkFactory and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk.util.factories;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;

import org.jetbrains.annotations.NotNull;
import java.util.function.IntFunction;

public class LongChunkFactory implements ChunkFactory {
    @NotNull
    @Override
    public final long[] makeArray(int capacity) {
        return LongChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> LongChunk<ATTR>[] makeChunkArray(int capacity) {
        return LongChunkChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> LongChunk<ATTR> getEmptyChunk() {
        return LongChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> LongChunkChunk<ATTR> getEmptyChunkChunk() {
        return LongChunkChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> LongChunk<ATTR> chunkWrap(Object array) {
        final long[] typedArray = (long[])array;
        return LongChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> LongChunk<ATTR> chunkWrap(Object array, int offset, int capacity) {
        final long[] typedArray = (long[])array;
        return LongChunk.chunkWrap(typedArray, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> LongChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array) {
        LongChunk<ATTR>[] typedArray = (LongChunk<ATTR>[])array;
        return LongChunkChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> LongChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array, int offset, int capacity) {
        LongChunk<ATTR>[] typedArray = (LongChunk<ATTR>[])array;
        return LongChunkChunk.chunkWrap(typedArray, offset, capacity);
    }


    @NotNull
    @Override
    public final <ATTR extends Any> ResettableReadOnlyChunk<ATTR> makeResettableReadOnlyChunk() {
        return ResettableLongChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableChunkChunk<ATTR> makeResettableChunkChunk() {
        return ResettableLongChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableLongChunk<ATTR> makeWritableChunk(int capacity) {
        return WritableLongChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableLongChunkChunk<ATTR> makeWritableChunkChunk(int capacity) {
        return WritableLongChunkChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableLongChunk<ATTR> writableChunkWrap(Object array, int offset, int capacity) {
        final long[] realType = (long[])array;
        return WritableLongChunk.writableChunkWrap(realType, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableChunkChunk<ATTR> writableChunkChunkWrap(WritableChunk<ATTR>[] array, int offset, int capacity) {
        WritableLongChunk<ATTR>[] actual = (WritableLongChunk<ATTR>[])array;
        return WritableLongChunkChunk.writableChunkWrap(actual, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunk<ATTR> makeResettableWritableChunk() {
        return ResettableWritableLongChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunkChunk<ATTR> makeResettableWritableChunkChunk() {
        return ResettableWritableLongChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final IntFunction<Chunk[]> chunkArrayBuilder() {
        return LongChunk[]::new;
    }


    @NotNull
    public final IntFunction<WritableChunk[]> writableChunkArrayBuilder() {
        return WritableLongChunk[]::new;
    }
}
