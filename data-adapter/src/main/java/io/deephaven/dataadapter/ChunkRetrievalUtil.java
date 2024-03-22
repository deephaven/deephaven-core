//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

class ChunkRetrievalUtil {

    @NotNull
    static <T> ObjectChunk<T, ?> getObjChunkForKeys(
            final ColumnSource<T> columnSource,
            final RowSequence rowSequence,
            final ChunkSource.GetContext context,
            final boolean usePrev) {
        return getChunkForKeys(
                columnSource,
                rowSequence,
                context,
                usePrev).asObjectChunk();
    }

    @NotNull
    static CharChunk<? extends Values> getCharChunkForKeys(
            final ColumnSource<Character> columnSource,
            final RowSequence rowSequence,
            final ChunkSource.GetContext context,
            final boolean usePrev) {
        return getChunkForKeys(
                columnSource,
                rowSequence,
                context,
                usePrev).asCharChunk();
    }

    @NotNull
    static ByteChunk<? extends Values> getByteChunkForKeys(
            final ColumnSource<Byte> columnSource,
            final RowSequence rowSequence,
            final ChunkSource.GetContext context,
            final boolean usePrev) {
        return getChunkForKeys(
                columnSource,
                rowSequence,
                context,
                usePrev).asByteChunk();
    }

    @NotNull
    static ShortChunk<? extends Values> getShortChunkForKeys(
            final ColumnSource<Short> columnSource,
            final RowSequence rowSequence,
            final ChunkSource.GetContext context,
            final boolean usePrev) {
        return getChunkForKeys(
                columnSource,
                rowSequence,
                context,
                usePrev).asShortChunk();
    }

    @NotNull
    static IntChunk<? extends Values> getIntChunkForKeys(
            final ColumnSource<Integer> columnSource,
            final RowSequence rowSequence,
            final ChunkSource.GetContext context,
            final boolean usePrev) {
        return getChunkForKeys(
                columnSource,
                rowSequence,
                context,
                usePrev).asIntChunk();
    }

    @NotNull
    static FloatChunk<? extends Values> getFloatChunkForKeys(
            final ColumnSource<Float> columnSource,
            final RowSequence rowSequence,
            final ChunkSource.GetContext context,
            final boolean usePrev) {
        return getChunkForKeys(
                columnSource,
                rowSequence,
                context,
                usePrev).asFloatChunk();
    }

    @NotNull
    static LongChunk<? extends Values> getLongChunkForKeys(
            final ColumnSource<Long> columnSource,
            final RowSequence rowSequence,
            final ChunkSource.GetContext context,
            final boolean usePrev) {
        return getChunkForKeys(
                columnSource,
                rowSequence,
                context,
                usePrev).asLongChunk();
    }

    @NotNull
    static DoubleChunk<? extends Values> getDoubleChunkForKeys(
            final ColumnSource<Double> columnSource,
            final RowSequence rowSequence,
            final ChunkSource.GetContext context,
            final boolean usePrev) {
        return getChunkForKeys(
                columnSource,
                rowSequence,
                context,
                usePrev).asDoubleChunk();
    }

    @NotNull
    static BooleanChunk<? extends Values> getBooleanChunkForKeys(
            final ColumnSource<Boolean> columnSource,
            final RowSequence rowSequence,
            final ChunkSource.GetContext context,
            final boolean usePrev) {
        return getChunkForKeys(
                columnSource,
                rowSequence,
                context,
                usePrev).asBooleanChunk();
    }


    @NotNull
    static <T> Chunk<? extends Values> getChunkForKeys(
            final ColumnSource<T> columnSource,
            final RowSequence rowSequence,
            final ChunkSource.GetContext context, boolean usePrev) {
        return Objects.requireNonNull(usePrev ? columnSource.getPrevChunk(context, rowSequence)
                : columnSource.getChunk(context, rowSequence));
    }


    private ChunkRetrievalUtil() {}

}
