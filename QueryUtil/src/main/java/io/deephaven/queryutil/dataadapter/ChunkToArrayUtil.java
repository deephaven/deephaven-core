package io.deephaven.queryutil.dataadapter;

import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;

/**
 * Utility for efficiently populating arrays with data from {@link ColumnSource column sources}.
 */
public class ChunkToArrayUtil {
    public static <T> void populateObjArrFromChunk(
            final ColumnSource<T> columnSource,
            final RowSequence rowSequence,
            final int len,
            final ChunkSource.GetContext context,
            final T[] arr,
            final int arrOffset,
            final boolean usePrev) {
        final ObjectChunk<T, ?> chunk =
                ChunkRetrievalUtil.getObjChunkForKeys(columnSource, rowSequence, context, usePrev);
        chunk.copyToTypedArray(0, arr, arrOffset, len);
    }

    public static void populateArrFromChunk(
            final ColumnSource<Character> columnSource,
            final RowSequence rowSequence,
            final int len,
            final ChunkSource.GetContext context,
            final char[] arr,
            final int arrOffset,
            final boolean usePrev) {
        final CharChunk<?> chunk = ChunkRetrievalUtil.getCharChunkForKeys(columnSource, rowSequence, context, usePrev);
        chunk.copyToTypedArray(0, arr, arrOffset, len);
    }

    public static void populateArrFromChunk(
            final ColumnSource<Byte> columnSource,
            final RowSequence rowSequence,
            final int len,
            final ChunkSource.GetContext context,
            final byte[] arr,
            final int arrOffset,
            final boolean usePrev) {
        final ByteChunk<?> chunk = ChunkRetrievalUtil.getByteChunkForKeys(columnSource, rowSequence, context, usePrev);
        chunk.copyToTypedArray(0, arr, arrOffset, len);
    }

    public static void populateArrFromChunk(
            final ColumnSource<Short> columnSource,
            final RowSequence rowSequence,
            final int len,
            final ChunkSource.GetContext context,
            final short[] arr,
            final int arrOffset,
            final boolean usePrev) {
        final ShortChunk<?> chunk =
                ChunkRetrievalUtil.getShortChunkForKeys(columnSource, rowSequence, context, usePrev);
        chunk.copyToTypedArray(0, arr, arrOffset, len);
    }

    public static void populateArrFromChunk(
            final ColumnSource<Integer> columnSource,
            final RowSequence rowSequence,
            final int len,
            final ChunkSource.GetContext context,
            final int[] arr,
            final int arrOffset,
            final boolean usePrev) {
        final IntChunk<?> chunk = ChunkRetrievalUtil.getIntChunkForKeys(columnSource, rowSequence, context, usePrev);
        chunk.copyToTypedArray(0, arr, arrOffset, len);
    }

    public static void populateArrFromChunk(
            final ColumnSource<Float> columnSource,
            final RowSequence rowSequence,
            final int len,
            final ChunkSource.GetContext context,
            final float[] arr,
            final int arrOffset,
            final boolean usePrev) {
        final FloatChunk<?> chunk =
                ChunkRetrievalUtil.getFloatChunkForKeys(columnSource, rowSequence, context, usePrev);
        chunk.copyToTypedArray(0, arr, arrOffset, len);
    }

    public static void populateArrFromChunk(
            final ColumnSource<Long> columnSource,
            final RowSequence rowSequence,
            final int len,
            final ChunkSource.GetContext context,
            final long[] arr,
            final int arrOffset,
            final boolean usePrev) {
        final LongChunk<?> chunk = ChunkRetrievalUtil.getLongChunkForKeys(columnSource, rowSequence, context, usePrev);
        chunk.copyToTypedArray(0, arr, arrOffset, len);
    }

    public static void populateArrFromChunk(
            final ColumnSource<Double> columnSource,
            final RowSequence rowSequence,
            final int len,
            final ChunkSource.GetContext context,
            final double[] arr,
            final int arrOffset,
            final boolean usePrev) {
        final DoubleChunk<?> chunk =
                ChunkRetrievalUtil.getDoubleChunkForKeys(columnSource, rowSequence, context, usePrev);
        chunk.copyToTypedArray(0, arr, arrOffset, len);
    }

    private ChunkToArrayUtil() {}
}
