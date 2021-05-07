/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkFactory and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.chunk.util.factories;

import io.deephaven.db.tables.dbarrays.*;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.page.ShortChunkPage;

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

    // region dbArrayWrap
    @NotNull
    @Override
    public final DbShortArrayDirect dbArrayWrap(Object array) {
        final short[] typedArray = (short[])array;
        return new DbShortArrayDirect(typedArray);
    }

    @NotNull
    @Override
    public DbShortArraySlice dbArrayWrap(Object array, int offset, int capacity) {
        DbShortArrayDirect dbShortArrayDirect = dbArrayWrap(array);
        return new DbShortArraySlice(dbShortArrayDirect, offset, capacity);
    }
    // endregion dbArrayWrap

    @NotNull
    @Override
    public final <ATTR extends Any> ShortChunkPage<ATTR> pageWrap(long beginRow, Object array, long mask) {
        short[] typedArray = (short[]) array;
        return ShortChunkPage.pageWrap(beginRow, typedArray, mask);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ShortChunkPage<ATTR> pageWrap(long beginRow, Object array, int offset, int capacity, long mask) {
        short[] typedArray = (short[]) array;
        return ShortChunkPage.pageWrap(beginRow, typedArray, offset, capacity, mask);
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
