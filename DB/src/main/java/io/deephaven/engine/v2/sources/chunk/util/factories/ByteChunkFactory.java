/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkFactory and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources.chunk.util.factories;

import io.deephaven.engine.tables.dbarrays.*;
import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.Any;
import io.deephaven.engine.v2.sources.chunk.page.ByteChunkPage;

import org.jetbrains.annotations.NotNull;
import java.util.function.IntFunction;

public class ByteChunkFactory implements ChunkFactory {
    @NotNull
    @Override
    public final byte[] makeArray(int capacity) {
        return ByteChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ByteChunk<ATTR>[] makeChunkArray(int capacity) {
        return ByteChunkChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ByteChunk<ATTR> getEmptyChunk() {
        return ByteChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ByteChunkChunk<ATTR> getEmptyChunkChunk() {
        return ByteChunkChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ByteChunk<ATTR> chunkWrap(Object array) {
        final byte[] typedArray = (byte[])array;
        return ByteChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ByteChunk<ATTR> chunkWrap(Object array, int offset, int capacity) {
        final byte[] typedArray = (byte[])array;
        return ByteChunk.chunkWrap(typedArray, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ByteChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array) {
        ByteChunk<ATTR>[] typedArray = (ByteChunk<ATTR>[])array;
        return ByteChunkChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ByteChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array, int offset, int capacity) {
        ByteChunk<ATTR>[] typedArray = (ByteChunk<ATTR>[])array;
        return ByteChunkChunk.chunkWrap(typedArray, offset, capacity);
    }

    // region dbArrayWrap
    @NotNull
    @Override
    public final DbByteArrayDirect dbArrayWrap(Object array) {
        final byte[] typedArray = (byte[])array;
        return new DbByteArrayDirect(typedArray);
    }

    @NotNull
    @Override
    public DbByteArraySlice dbArrayWrap(Object array, int offset, int capacity) {
        DbByteArrayDirect dbByteArrayDirect = dbArrayWrap(array);
        return new DbByteArraySlice(dbByteArrayDirect, offset, capacity);
    }
    // endregion dbArrayWrap

    @NotNull
    @Override
    public final <ATTR extends Any> ByteChunkPage<ATTR> pageWrap(long beginRow, Object array, long mask) {
        byte[] typedArray = (byte[]) array;
        return ByteChunkPage.pageWrap(beginRow, typedArray, mask);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ByteChunkPage<ATTR> pageWrap(long beginRow, Object array, int offset, int capacity, long mask) {
        byte[] typedArray = (byte[]) array;
        return ByteChunkPage.pageWrap(beginRow, typedArray, offset, capacity, mask);
    }


    @NotNull
    @Override
    public final <ATTR extends Any> ResettableReadOnlyChunk<ATTR> makeResettableReadOnlyChunk() {
        return ResettableByteChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableChunkChunk<ATTR> makeResettableChunkChunk() {
        return ResettableByteChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableByteChunk<ATTR> makeWritableChunk(int capacity) {
        return WritableByteChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableByteChunkChunk<ATTR> makeWritableChunkChunk(int capacity) {
        return WritableByteChunkChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableByteChunk<ATTR> writableChunkWrap(Object array, int offset, int capacity) {
        final byte[] realType = (byte[])array;
        return WritableByteChunk.writableChunkWrap(realType, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableChunkChunk<ATTR> writableChunkChunkWrap(WritableChunk<ATTR>[] array, int offset, int capacity) {
        WritableByteChunk<ATTR>[] actual = (WritableByteChunk<ATTR>[])array;
        return WritableByteChunkChunk.writableChunkWrap(actual, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunk<ATTR> makeResettableWritableChunk() {
        return ResettableWritableByteChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunkChunk<ATTR> makeResettableWritableChunkChunk() {
        return ResettableWritableByteChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final IntFunction<Chunk[]> chunkArrayBuilder() {
        return ByteChunk[]::new;
    }


    @NotNull
    public final IntFunction<WritableChunk[]> writableChunkArrayBuilder() {
        return WritableByteChunk[]::new;
    }
}
