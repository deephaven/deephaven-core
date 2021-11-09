/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkFactory and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources.chunk.util.factories;

import io.deephaven.engine.tables.dbarrays.*;
import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.Any;
import io.deephaven.engine.v2.sources.chunk.page.LongChunkPage;

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

    // region vectorWrap
    @NotNull
    @Override
    public final LongVectorDirect vectorWrap(Object array) {
        final long[] typedArray = (long[])array;
        return new LongVectorDirect(typedArray);
    }

    @NotNull
    @Override
    public LongVectorSlice vectorWrap(Object array, int offset, int capacity) {
        LongVectorDirect dbLongArrayDirect = vectorWrap(array);
        return new LongVectorSlice(dbLongArrayDirect, offset, capacity);
    }
    // endregion vectorWrap

    @NotNull
    @Override
    public final <ATTR extends Any> LongChunkPage<ATTR> pageWrap(long beginRow, Object array, long mask) {
        long[] typedArray = (long[]) array;
        return LongChunkPage.pageWrap(beginRow, typedArray, mask);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> LongChunkPage<ATTR> pageWrap(long beginRow, Object array, int offset, int capacity, long mask) {
        long[] typedArray = (long[]) array;
        return LongChunkPage.pageWrap(beginRow, typedArray, offset, capacity, mask);
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
