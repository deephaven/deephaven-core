package io.deephaven.chunk.util.factories;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;

import org.jetbrains.annotations.NotNull;
import java.util.function.IntFunction;

public class CharChunkFactory implements ChunkFactory {
    @NotNull
    @Override
    public final char[] makeArray(int capacity) {
        return CharChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> CharChunk<ATTR>[] makeChunkArray(int capacity) {
        return CharChunkChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> CharChunk<ATTR> getEmptyChunk() {
        return CharChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> CharChunkChunk<ATTR> getEmptyChunkChunk() {
        return CharChunkChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> CharChunk<ATTR> chunkWrap(Object array) {
        final char[] typedArray = (char[])array;
        return CharChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> CharChunk<ATTR> chunkWrap(Object array, int offset, int capacity) {
        final char[] typedArray = (char[])array;
        return CharChunk.chunkWrap(typedArray, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> CharChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array) {
        CharChunk<ATTR>[] typedArray = (CharChunk<ATTR>[])array;
        return CharChunkChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> CharChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array, int offset, int capacity) {
        CharChunk<ATTR>[] typedArray = (CharChunk<ATTR>[])array;
        return CharChunkChunk.chunkWrap(typedArray, offset, capacity);
    }


    @NotNull
    @Override
    public final <ATTR extends Any> ResettableReadOnlyChunk<ATTR> makeResettableReadOnlyChunk() {
        return ResettableCharChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableChunkChunk<ATTR> makeResettableChunkChunk() {
        return ResettableCharChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableCharChunk<ATTR> makeWritableChunk(int capacity) {
        return WritableCharChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableCharChunkChunk<ATTR> makeWritableChunkChunk(int capacity) {
        return WritableCharChunkChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableCharChunk<ATTR> writableChunkWrap(Object array, int offset, int capacity) {
        final char[] realType = (char[])array;
        return WritableCharChunk.writableChunkWrap(realType, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableChunkChunk<ATTR> writableChunkChunkWrap(WritableChunk<ATTR>[] array, int offset, int capacity) {
        WritableCharChunk<ATTR>[] actual = (WritableCharChunk<ATTR>[])array;
        return WritableCharChunkChunk.writableChunkWrap(actual, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunk<ATTR> makeResettableWritableChunk() {
        return ResettableWritableCharChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunkChunk<ATTR> makeResettableWritableChunkChunk() {
        return ResettableWritableCharChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final IntFunction<Chunk[]> chunkArrayBuilder() {
        return CharChunk[]::new;
    }


    @NotNull
    public final IntFunction<WritableChunk[]> writableChunkArrayBuilder() {
        return WritableCharChunk[]::new;
    }
}
