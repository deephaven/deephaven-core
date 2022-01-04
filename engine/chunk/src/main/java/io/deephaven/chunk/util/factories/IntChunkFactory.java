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

public class IntChunkFactory implements ChunkFactory {
    @NotNull
    @Override
    public final int[] makeArray(int capacity) {
        return IntChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> IntChunk<ATTR>[] makeChunkArray(int capacity) {
        return IntChunkChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> IntChunk<ATTR> getEmptyChunk() {
        return IntChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> IntChunkChunk<ATTR> getEmptyChunkChunk() {
        return IntChunkChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> IntChunk<ATTR> chunkWrap(Object array) {
        final int[] typedArray = (int[])array;
        return IntChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> IntChunk<ATTR> chunkWrap(Object array, int offset, int capacity) {
        final int[] typedArray = (int[])array;
        return IntChunk.chunkWrap(typedArray, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> IntChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array) {
        IntChunk<ATTR>[] typedArray = (IntChunk<ATTR>[])array;
        return IntChunkChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> IntChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array, int offset, int capacity) {
        IntChunk<ATTR>[] typedArray = (IntChunk<ATTR>[])array;
        return IntChunkChunk.chunkWrap(typedArray, offset, capacity);
    }


    @NotNull
    @Override
    public final <ATTR extends Any> ResettableReadOnlyChunk<ATTR> makeResettableReadOnlyChunk() {
        return ResettableIntChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableChunkChunk<ATTR> makeResettableChunkChunk() {
        return ResettableIntChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableIntChunk<ATTR> makeWritableChunk(int capacity) {
        return WritableIntChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableIntChunkChunk<ATTR> makeWritableChunkChunk(int capacity) {
        return WritableIntChunkChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableIntChunk<ATTR> writableChunkWrap(Object array, int offset, int capacity) {
        final int[] realType = (int[])array;
        return WritableIntChunk.writableChunkWrap(realType, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableChunkChunk<ATTR> writableChunkChunkWrap(WritableChunk<ATTR>[] array, int offset, int capacity) {
        WritableIntChunk<ATTR>[] actual = (WritableIntChunk<ATTR>[])array;
        return WritableIntChunkChunk.writableChunkWrap(actual, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunk<ATTR> makeResettableWritableChunk() {
        return ResettableWritableIntChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunkChunk<ATTR> makeResettableWritableChunkChunk() {
        return ResettableWritableIntChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final IntFunction<Chunk[]> chunkArrayBuilder() {
        return IntChunk[]::new;
    }


    @NotNull
    public final IntFunction<WritableChunk[]> writableChunkArrayBuilder() {
        return WritableIntChunk[]::new;
    }
}
