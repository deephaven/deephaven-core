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

public class ShortChunkFactory implements ChunkFactory {
    @NotNull
    @Override
    public final short[] makeArray(int capacity) {
        return ShortChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ShortChunk<ATTR>[] makeChunkArray(int capacity) {
        return ShortChunkChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ShortChunk<ATTR> getEmptyChunk() {
        return ShortChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ShortChunkChunk<ATTR> getEmptyChunkChunk() {
        return ShortChunkChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ShortChunk<ATTR> chunkWrap(Object array) {
        final short[] typedArray = (short[])array;
        return ShortChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ShortChunk<ATTR> chunkWrap(Object array, int offset, int capacity) {
        final short[] typedArray = (short[])array;
        return ShortChunk.chunkWrap(typedArray, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ShortChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array) {
        ShortChunk<ATTR>[] typedArray = (ShortChunk<ATTR>[])array;
        return ShortChunkChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ShortChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array, int offset, int capacity) {
        ShortChunk<ATTR>[] typedArray = (ShortChunk<ATTR>[])array;
        return ShortChunkChunk.chunkWrap(typedArray, offset, capacity);
    }


    @NotNull
    @Override
    public final <ATTR extends Any> ResettableReadOnlyChunk<ATTR> makeResettableReadOnlyChunk() {
        return ResettableShortChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableChunkChunk<ATTR> makeResettableChunkChunk() {
        return ResettableShortChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableShortChunk<ATTR> makeWritableChunk(int capacity) {
        return WritableShortChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableShortChunkChunk<ATTR> makeWritableChunkChunk(int capacity) {
        return WritableShortChunkChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableShortChunk<ATTR> writableChunkWrap(Object array, int offset, int capacity) {
        final short[] realType = (short[])array;
        return WritableShortChunk.writableChunkWrap(realType, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableChunkChunk<ATTR> writableChunkChunkWrap(WritableChunk<ATTR>[] array, int offset, int capacity) {
        WritableShortChunk<ATTR>[] actual = (WritableShortChunk<ATTR>[])array;
        return WritableShortChunkChunk.writableChunkWrap(actual, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunk<ATTR> makeResettableWritableChunk() {
        return ResettableWritableShortChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunkChunk<ATTR> makeResettableWritableChunkChunk() {
        return ResettableWritableShortChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final IntFunction<Chunk[]> chunkArrayBuilder() {
        return ShortChunk[]::new;
    }


    @NotNull
    public final IntFunction<WritableChunk[]> writableChunkArrayBuilder() {
        return WritableShortChunk[]::new;
    }
}
