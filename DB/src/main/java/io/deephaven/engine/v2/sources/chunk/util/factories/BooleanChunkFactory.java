/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkFactory and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources.chunk.util.factories;

import io.deephaven.engine.tables.dbarrays.*;
import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.Any;
import io.deephaven.engine.v2.sources.chunk.page.BooleanChunkPage;

import org.jetbrains.annotations.NotNull;
import java.util.function.IntFunction;

public class BooleanChunkFactory implements ChunkFactory {
    @NotNull
    @Override
    public final boolean[] makeArray(int capacity) {
        return BooleanChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> BooleanChunk<ATTR>[] makeChunkArray(int capacity) {
        return BooleanChunkChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> BooleanChunk<ATTR> getEmptyChunk() {
        return BooleanChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> BooleanChunkChunk<ATTR> getEmptyChunkChunk() {
        return BooleanChunkChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> BooleanChunk<ATTR> chunkWrap(Object array) {
        final boolean[] typedArray = (boolean[])array;
        return BooleanChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> BooleanChunk<ATTR> chunkWrap(Object array, int offset, int capacity) {
        final boolean[] typedArray = (boolean[])array;
        return BooleanChunk.chunkWrap(typedArray, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> BooleanChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array) {
        BooleanChunk<ATTR>[] typedArray = (BooleanChunk<ATTR>[])array;
        return BooleanChunkChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> BooleanChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array, int offset, int capacity) {
        BooleanChunk<ATTR>[] typedArray = (BooleanChunk<ATTR>[])array;
        return BooleanChunkChunk.chunkWrap(typedArray, offset, capacity);
    }

    // region vectorWrap
    @NotNull
    @Override
    public final BooleanVectorDirect vectorWrap(Object array) {
        throw new UnsupportedOperationException("No implementation for boolean primitive Vector exists");
    }

    @NotNull
    @Override
    public BooleanVectorSlice vectorWrap(Object array, int offset, int capacity) {
        throw new UnsupportedOperationException("No implementation for boolean primitive Vector exists");
    }
    // endregion vectorWrap

    @NotNull
    @Override
    public final <ATTR extends Any> BooleanChunkPage<ATTR> pageWrap(long beginRow, Object array, long mask) {
        boolean[] typedArray = (boolean[]) array;
        return BooleanChunkPage.pageWrap(beginRow, typedArray, mask);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> BooleanChunkPage<ATTR> pageWrap(long beginRow, Object array, int offset, int capacity, long mask) {
        boolean[] typedArray = (boolean[]) array;
        return BooleanChunkPage.pageWrap(beginRow, typedArray, offset, capacity, mask);
    }


    @NotNull
    @Override
    public final <ATTR extends Any> ResettableReadOnlyChunk<ATTR> makeResettableReadOnlyChunk() {
        return ResettableBooleanChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableChunkChunk<ATTR> makeResettableChunkChunk() {
        return ResettableBooleanChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableBooleanChunk<ATTR> makeWritableChunk(int capacity) {
        return WritableBooleanChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableBooleanChunkChunk<ATTR> makeWritableChunkChunk(int capacity) {
        return WritableBooleanChunkChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableBooleanChunk<ATTR> writableChunkWrap(Object array, int offset, int capacity) {
        final boolean[] realType = (boolean[])array;
        return WritableBooleanChunk.writableChunkWrap(realType, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableChunkChunk<ATTR> writableChunkChunkWrap(WritableChunk<ATTR>[] array, int offset, int capacity) {
        WritableBooleanChunk<ATTR>[] actual = (WritableBooleanChunk<ATTR>[])array;
        return WritableBooleanChunkChunk.writableChunkWrap(actual, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunk<ATTR> makeResettableWritableChunk() {
        return ResettableWritableBooleanChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunkChunk<ATTR> makeResettableWritableChunkChunk() {
        return ResettableWritableBooleanChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final IntFunction<Chunk[]> chunkArrayBuilder() {
        return BooleanChunk[]::new;
    }


    @NotNull
    public final IntFunction<WritableChunk[]> writableChunkArrayBuilder() {
        return WritableBooleanChunk[]::new;
    }
}
