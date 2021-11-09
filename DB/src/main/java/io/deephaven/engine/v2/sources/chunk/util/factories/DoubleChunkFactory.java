/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkFactory and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources.chunk.util.factories;

import io.deephaven.engine.tables.dbarrays.*;
import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.Any;
import io.deephaven.engine.v2.sources.chunk.page.DoubleChunkPage;

import org.jetbrains.annotations.NotNull;
import java.util.function.IntFunction;

public class DoubleChunkFactory implements ChunkFactory {
    @NotNull
    @Override
    public final double[] makeArray(int capacity) {
        return DoubleChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> DoubleChunk<ATTR>[] makeChunkArray(int capacity) {
        return DoubleChunkChunk.makeArray(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> DoubleChunk<ATTR> getEmptyChunk() {
        return DoubleChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> DoubleChunkChunk<ATTR> getEmptyChunkChunk() {
        return DoubleChunkChunk.getEmptyChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> DoubleChunk<ATTR> chunkWrap(Object array) {
        final double[] typedArray = (double[])array;
        return DoubleChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> DoubleChunk<ATTR> chunkWrap(Object array, int offset, int capacity) {
        final double[] typedArray = (double[])array;
        return DoubleChunk.chunkWrap(typedArray, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> DoubleChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array) {
        DoubleChunk<ATTR>[] typedArray = (DoubleChunk<ATTR>[])array;
        return DoubleChunkChunk.chunkWrap(typedArray);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> DoubleChunkChunk<ATTR> chunkChunkWrap(Chunk<ATTR>[] array, int offset, int capacity) {
        DoubleChunk<ATTR>[] typedArray = (DoubleChunk<ATTR>[])array;
        return DoubleChunkChunk.chunkWrap(typedArray, offset, capacity);
    }

    // region vectorWrap
    @NotNull
    @Override
    public final DoubleVectorDirect vectorWrap(Object array) {
        final double[] typedArray = (double[])array;
        return new DoubleVectorDirect(typedArray);
    }

    @NotNull
    @Override
    public DoubleVectorSlice vectorWrap(Object array, int offset, int capacity) {
        DoubleVectorDirect vectorDirect = vectorWrap(array);
        return new DoubleVectorSlice(vectorDirect, offset, capacity);
    }
    // endregion vectorWrap

    @NotNull
    @Override
    public final <ATTR extends Any> DoubleChunkPage<ATTR> pageWrap(long beginRow, Object array, long mask) {
        double[] typedArray = (double[]) array;
        return DoubleChunkPage.pageWrap(beginRow, typedArray, mask);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> DoubleChunkPage<ATTR> pageWrap(long beginRow, Object array, int offset, int capacity, long mask) {
        double[] typedArray = (double[]) array;
        return DoubleChunkPage.pageWrap(beginRow, typedArray, offset, capacity, mask);
    }


    @NotNull
    @Override
    public final <ATTR extends Any> ResettableReadOnlyChunk<ATTR> makeResettableReadOnlyChunk() {
        return ResettableDoubleChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableChunkChunk<ATTR> makeResettableChunkChunk() {
        return ResettableDoubleChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableDoubleChunk<ATTR> makeWritableChunk(int capacity) {
        return WritableDoubleChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableDoubleChunkChunk<ATTR> makeWritableChunkChunk(int capacity) {
        return WritableDoubleChunkChunk.makeWritableChunk(capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableDoubleChunk<ATTR> writableChunkWrap(Object array, int offset, int capacity) {
        final double[] realType = (double[])array;
        return WritableDoubleChunk.writableChunkWrap(realType, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> WritableChunkChunk<ATTR> writableChunkChunkWrap(WritableChunk<ATTR>[] array, int offset, int capacity) {
        WritableDoubleChunk<ATTR>[] actual = (WritableDoubleChunk<ATTR>[])array;
        return WritableDoubleChunkChunk.writableChunkWrap(actual, offset, capacity);
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunk<ATTR> makeResettableWritableChunk() {
        return ResettableWritableDoubleChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final <ATTR extends Any> ResettableWritableChunkChunk<ATTR> makeResettableWritableChunkChunk() {
        return ResettableWritableDoubleChunkChunk.makeResettableChunk();
    }

    @NotNull
    @Override
    public final IntFunction<Chunk[]> chunkArrayBuilder() {
        return DoubleChunk[]::new;
    }


    @NotNull
    public final IntFunction<WritableChunk[]> writableChunkArrayBuilder() {
        return WritableDoubleChunk[]::new;
    }
}
