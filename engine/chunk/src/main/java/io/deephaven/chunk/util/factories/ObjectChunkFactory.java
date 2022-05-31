package io.deephaven.chunk.util.factories;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;

import org.jetbrains.annotations.NotNull;
import java.util.function.IntFunction;

public final class ObjectChunkFactory<T> implements ChunkFactory {
    @NotNull
    @Override
    public final T[] makeArray(int capacity) {
        return WritableObjectChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ObjectChunk<T, ATTR>[] makeChunkArray(int capacity) {
        return WritableObjectChunkChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ObjectChunk<T, ATTR> getEmptyChunk() {
        return ObjectChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ObjectChunkChunk<T, ATTR> getEmptyChunkChunk() {
        return ObjectChunkChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ObjectChunk<T, ATTR> chunkWrap(Object array) {
        //noinspection unchecked
        final T[] typedArray = (T[])array;
        return ObjectChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ObjectChunk<T, ATTR> chunkWrap(Object array, int offset, int capacity) {
        //noinspection unchecked
        final T[] typedArray = (T[])array;
        return ObjectChunk.chunkWrap(typedArray, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ObjectChunkChunk<T, ATTR> chunkChunkWrap(Chunk<ATTR>[] array) {
        ObjectChunk<T, ATTR>[] typedArray = (ObjectChunk<T, ATTR>[])array;
        return ObjectChunkChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ObjectChunkChunk<T, ATTR> chunkChunkWrap(Chunk<ATTR>[] array, int offset, int capacity) {
         ObjectChunk<T, ATTR>[] typedArray = (ObjectChunk<T, ATTR>[])array;
        return ObjectChunkChunk.chunkWrap(typedArray, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableReadOnlyChunk<ATTR> makeResettableReadOnlyChunk() {
        return ResettableObjectChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableChunkChunk<ATTR> makeResettableChunkChunk() {
        return ResettableObjectChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableObjectChunk<T, ATTR> makeWritableChunk(int capacity) {
        return WritableObjectChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableObjectChunkChunk<T, ATTR> makeWritableChunkChunk(int capacity) {
        return WritableObjectChunkChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableObjectChunk<T, ATTR> writableChunkWrap(Object array, int offset, int capacity) {
        //noinspection unchecked
        final T[] realType = (T[])array;
        return WritableObjectChunk.writableChunkWrap(realType, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableChunkChunk<ATTR> writableChunkChunkWrap(WritableChunk<ATTR>[] array, int offset, int capacity) {
        WritableObjectChunk<T, ATTR>[] actual = (WritableObjectChunk<T, ATTR>[])array;
        return WritableObjectChunkChunk.writableChunkWrap(actual, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunk<ATTR> makeResettableWritableChunk() {
        return ResettableWritableObjectChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunkChunk<ATTR> makeResettableWritableChunkChunk() {
        return ResettableWritableObjectChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final IntFunction<Chunk[]> chunkArrayBuilder() {
        return ObjectChunk[]::new;
    }

    @NotNull
    @Override
    public final IntFunction<WritableChunk[]> writableChunkArrayBuilder() {
        return WritableObjectChunk[]::new;
    }
}
