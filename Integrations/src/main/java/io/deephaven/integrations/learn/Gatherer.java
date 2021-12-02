package io.deephaven.integrations.learn;

import io.deephaven.chunk.ResettableWritableBooleanChunk;
import io.deephaven.chunk.ResettableWritableByteChunk;
import io.deephaven.chunk.ResettableWritableShortChunk;
import io.deephaven.chunk.ResettableWritableIntChunk;
import io.deephaven.chunk.ResettableWritableLongChunk;
import io.deephaven.chunk.ResettableWritableFloatChunk;
import io.deephaven.chunk.ResettableWritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;

/**
 * Gatherer takes Deephaven columnar data and places it into a buffer to be used by Python. The Python object will take
 * data from the buffer and use it to construct a 2d array of specified size.
 */
public class Gatherer {

    /**
     * Copy data from a table into a 2d tensor of Booleans.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static boolean[] tensorBuffer2DBoolean(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
        final int nRows = rowSeq.intSize();
        final int nCols = columnSources.length;
        final boolean[] tensor = new boolean[nRows * nCols];

        try (final ResettableWritableBooleanChunk<? extends Values> valueChunk =
                ResettableWritableBooleanChunk.makeResettableChunk();
                final SharedContext sharedContext = SharedContext.makeSharedContext()) {

            for (int ci = 0; ci < nCols; ++ci) {
                valueChunk.resetFromArray(tensor, ci * nRows, nRows);
                final ColumnSource<?> colSrc = columnSources[ci];

                try (final ChunkSource.FillContext fillContext = colSrc.makeFillContext(nRows, sharedContext)) {
                    // noinspection unchecked
                    colSrc.fillChunk(fillContext, valueChunk, rowSeq);
                }
            }
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of bytes.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static byte[] tensorBuffer2DByte(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
        final int nRows = rowSeq.intSize();
        final int nCols = columnSources.length;
        final byte[] tensor = new byte[nRows * nCols];

        try (final ResettableWritableByteChunk<? extends Values> valueChunk =
                ResettableWritableByteChunk.makeResettableChunk();
                final SharedContext sharedContext = SharedContext.makeSharedContext()) {

            for (int ci = 0; ci < nCols; ++ci) {
                valueChunk.resetFromArray(tensor, ci * nRows, nRows);
                final ColumnSource<?> colSrc = columnSources[ci];

                try (final ChunkSource.FillContext fillContext = colSrc.makeFillContext(nRows, sharedContext)) {
                    // noinspection unchecked
                    colSrc.fillChunk(fillContext, valueChunk, rowSeq);
                }
            }
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of shorts.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */

    public static short[] tensorBuffer2DShort(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
        final int nRows = rowSeq.intSize();
        final int nCols = columnSources.length;
        final short[] tensor = new short[nRows * nCols];

        try (final ResettableWritableShortChunk<? extends Values> valueChunk =
                ResettableWritableShortChunk.makeResettableChunk();
                final SharedContext sharedContext = SharedContext.makeSharedContext()) {

            for (int ci = 0; ci < nCols; ++ci) {
                valueChunk.resetFromArray(tensor, ci * nRows, nRows);
                final ColumnSource<?> colSrc = columnSources[ci];

                try (final ChunkSource.FillContext fillContext = colSrc.makeFillContext(nRows, sharedContext)) {
                    // noinspection unchecked
                    colSrc.fillChunk(fillContext, valueChunk, rowSeq);
                }
            }
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of ints.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static int[] tensorBuffer2DInt(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
        final int nRows = rowSeq.intSize();
        final int nCols = columnSources.length;
        final int[] tensor = new int[nRows * nCols];

        try (final ResettableWritableIntChunk<? extends Values> valueChunk =
                ResettableWritableIntChunk.makeResettableChunk();
                final SharedContext sharedContext = SharedContext.makeSharedContext()) {

            for (int ci = 0; ci < nCols; ++ci) {
                valueChunk.resetFromArray(tensor, ci * nRows, nRows);
                final ColumnSource<?> colSrc = columnSources[ci];

                try (final ChunkSource.FillContext fillContext = colSrc.makeFillContext(nRows, sharedContext)) {
                    // noinspection unchecked
                    colSrc.fillChunk(fillContext, valueChunk, rowSeq);
                }
            }
        }

        return tensor;
    }



    /**
     * Copy data from a table into a 2d tensor of longs.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */

    public static long[] tensorBuffer2DLong(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
        final int nRows = rowSeq.intSize();
        final int nCols = columnSources.length;
        final long[] tensor = new long[nRows * nCols];

        try (final ResettableWritableLongChunk<? extends Values> valueChunk =
                ResettableWritableLongChunk.makeResettableChunk();
                final SharedContext sharedContext = SharedContext.makeSharedContext()) {

            for (int ci = 0; ci < nCols; ++ci) {
                valueChunk.resetFromArray(tensor, ci * nRows, nRows);
                final ColumnSource<?> colSrc = columnSources[ci];

                try (final ChunkSource.FillContext fillContext = colSrc.makeFillContext(nRows, sharedContext)) {
                    // noinspection unchecked
                    colSrc.fillChunk(fillContext, valueChunk, rowSeq);
                }
            }
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of floats.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */

    public static float[] tensorBuffer2DFloat(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
        final int nRows = rowSeq.intSize();
        final int nCols = columnSources.length;
        final float[] tensor = new float[nRows * nCols];

        try (final ResettableWritableFloatChunk<? extends Values> valueChunk =
                ResettableWritableFloatChunk.makeResettableChunk();
                final SharedContext sharedContext = SharedContext.makeSharedContext()) {

            for (int ci = 0; ci < nCols; ++ci) {
                valueChunk.resetFromArray(tensor, ci * nRows, nRows);
                final ColumnSource<?> colSrc = columnSources[ci];

                try (final ChunkSource.FillContext fillContext = colSrc.makeFillContext(nRows, sharedContext)) {
                    // noinspection unchecked
                    colSrc.fillChunk(fillContext, valueChunk, rowSeq);
                }
            }
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of doubles.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static double[] tensorBuffer2DDouble(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
        final int nRows = rowSeq.intSize();
        final int nCols = columnSources.length;
        final double[] tensor = new double[nRows * nCols];

        try (final ResettableWritableDoubleChunk<? extends Values> valueChunk =
                ResettableWritableDoubleChunk.makeResettableChunk();
                final SharedContext sharedContext = SharedContext.makeSharedContext()) {

            for (int ci = 0; ci < nCols; ++ci) {
                valueChunk.resetFromArray(tensor, ci * nRows, nRows);
                final ColumnSource<?> colSrc = columnSources[ci];

                try (final ChunkSource.FillContext fillContext = colSrc.makeFillContext(nRows, sharedContext)) {
                    // noinspection unchecked
                    colSrc.fillChunk(fillContext, valueChunk, rowSeq);
                }
            }
        }

        return tensor;
    }

}
