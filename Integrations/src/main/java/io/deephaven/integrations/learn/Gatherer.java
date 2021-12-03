package io.deephaven.integrations.learn;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.util.SafeCloseableList;

/**
 * Gatherer takes Deephaven columnar data and places it into a buffer to be used by Python. The Python object will take
 * data from the buffer and use it to construct a 2d array of specified size.
 */
public class Gatherer {

    private static final int COPY_CHUNK_SIZE = 2048;

    /**
     * Copy data from a table into a 2d tensor of Booleans in column-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static boolean[] tensorBuffer2DBooleanColumns(final RowSequence rowSeq,
            final ColumnSource<?>[] columnSources) {
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
     * Copy data from a table into a 2d tensor of Booleans in row-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static boolean[] tensorBuffer2DBooleanRows(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
        final int nRows = rowSeq.intSize();
        final int nCols = columnSources.length;
        final boolean[] tensor = new boolean[nRows * nCols];

        try (final SafeCloseableList toClose = new SafeCloseableList()) {
            final RowSequence.Iterator rowKeys = toClose.add(rowSeq.getRowSequenceIterator());
            final SharedContext inputSharedContext = toClose.add(SharedContext.makeSharedContext());
            final ChunkSource.GetContext[] inputContexts = toClose.addArray(new ChunkSource.GetContext[nCols]);
            for (int ci = 0; ci < nCols; ++ci) {
                inputContexts[ci] = columnSources[ci].makeGetContext(COPY_CHUNK_SIZE, inputSharedContext);
            }

            // noinspection unchecked
            final BooleanChunk<? extends Values>[] inputColumnValues = new BooleanChunk[nCols];
            int ti = 0;
            while (rowKeys.hasMore()) {
                final RowSequence sliceRowKeys = rowKeys.getNextRowSequenceWithLength(COPY_CHUNK_SIZE);
                for (int ci = 0; ci < nCols; ++ci) {
                    inputColumnValues[ci] =
                            columnSources[ci].getChunk(inputContexts[ci], sliceRowKeys).asBooleanChunk();
                }

                final int sliceChunkSize = sliceRowKeys.intSize();
                for (int ri = 0; ri < sliceChunkSize; ++ri) {
                    for (int ci = 0; ci < nCols; ++ci) {
                        tensor[ti++] = inputColumnValues[ci].get(ri);
                    }
                }
                inputSharedContext.reset();
            }
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of bytes in column-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static byte[] tensorBuffer2DByteColumns(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
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
     * Copy data from a table into a 2d tensor of bytes in row-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static byte[] tensorBuffer2DByteRows(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
        final int nRows = rowSeq.intSize();
        final int nCols = columnSources.length;
        final byte[] tensor = new byte[nRows * nCols];

        try (final SafeCloseableList toClose = new SafeCloseableList()) {
            final RowSequence.Iterator rowKeys = toClose.add(rowSeq.getRowSequenceIterator());
            final SharedContext inputSharedContext = toClose.add(SharedContext.makeSharedContext());
            final ChunkSource.GetContext[] inputContexts = toClose.addArray(new ChunkSource.GetContext[nCols]);
            for (int ci = 0; ci < nCols; ++ci) {
                inputContexts[ci] = columnSources[ci].makeGetContext(COPY_CHUNK_SIZE, inputSharedContext);
            }

            // noinspection unchecked
            final ByteChunk<? extends Values>[] inputColumnValues = new ByteChunk[nCols];
            int ti = 0;
            while (rowKeys.hasMore()) {
                final RowSequence sliceRowKeys = rowKeys.getNextRowSequenceWithLength(COPY_CHUNK_SIZE);
                for (int ci = 0; ci < nCols; ++ci) {
                    inputColumnValues[ci] = columnSources[ci].getChunk(inputContexts[ci], sliceRowKeys).asByteChunk();
                }

                final int sliceChunkSize = sliceRowKeys.intSize();
                for (int ri = 0; ri < sliceChunkSize; ++ri) {
                    for (int ci = 0; ci < nCols; ++ci) {
                        tensor[ti++] = inputColumnValues[ci].get(ri);
                    }
                }
                inputSharedContext.reset();
            }
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of shorts in column-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */

    public static short[] tensorBuffer2DShortColumns(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
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
     * Copy data from a table into a 2d tensor of shorts in row-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static short[] tensorBuffer2DShortRows(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
        final int nRows = rowSeq.intSize();
        final int nCols = columnSources.length;
        final short[] tensor = new short[nRows * nCols];

        try (final SafeCloseableList toClose = new SafeCloseableList()) {
            final RowSequence.Iterator rowKeys = toClose.add(rowSeq.getRowSequenceIterator());
            final SharedContext inputSharedContext = toClose.add(SharedContext.makeSharedContext());
            final ChunkSource.GetContext[] inputContexts = toClose.addArray(new ChunkSource.GetContext[nCols]);
            for (int ci = 0; ci < nCols; ++ci) {
                inputContexts[ci] = columnSources[ci].makeGetContext(COPY_CHUNK_SIZE, inputSharedContext);
            }

            // noinspection unchecked
            final ShortChunk<? extends Values>[] inputColumnValues = new ShortChunk[nCols];
            int ti = 0;
            while (rowKeys.hasMore()) {
                final RowSequence sliceRowKeys = rowKeys.getNextRowSequenceWithLength(COPY_CHUNK_SIZE);
                for (int ci = 0; ci < nCols; ++ci) {
                    inputColumnValues[ci] = columnSources[ci].getChunk(inputContexts[ci], sliceRowKeys).asShortChunk();
                }

                final int sliceChunkSize = sliceRowKeys.intSize();
                for (int ri = 0; ri < sliceChunkSize; ++ri) {
                    for (int ci = 0; ci < nCols; ++ci) {
                        tensor[ti++] = inputColumnValues[ci].get(ri);
                    }
                }
                inputSharedContext.reset();
            }
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of ints in column-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static int[] tensorBuffer2DIntColumns(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
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
     * Copy data from a table into a 2d tensor of ints in row-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static int[] tensorBuffer2DIntRows(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
        final int nRows = rowSeq.intSize();
        final int nCols = columnSources.length;
        final int[] tensor = new int[nRows * nCols];

        try (final SafeCloseableList toClose = new SafeCloseableList()) {
            final RowSequence.Iterator rowKeys = toClose.add(rowSeq.getRowSequenceIterator());
            final SharedContext inputSharedContext = toClose.add(SharedContext.makeSharedContext());
            final ChunkSource.GetContext[] inputContexts = toClose.addArray(new ChunkSource.GetContext[nCols]);
            for (int ci = 0; ci < nCols; ++ci) {
                inputContexts[ci] = columnSources[ci].makeGetContext(COPY_CHUNK_SIZE, inputSharedContext);
            }

            // noinspection unchecked
            final IntChunk<? extends Values>[] inputColumnValues = new IntChunk[nCols];
            int ti = 0;
            while (rowKeys.hasMore()) {
                final RowSequence sliceRowKeys = rowKeys.getNextRowSequenceWithLength(COPY_CHUNK_SIZE);
                for (int ci = 0; ci < nCols; ++ci) {
                    inputColumnValues[ci] = columnSources[ci].getChunk(inputContexts[ci], sliceRowKeys).asIntChunk();
                }

                final int sliceChunkSize = sliceRowKeys.intSize();
                for (int ri = 0; ri < sliceChunkSize; ++ri) {
                    for (int ci = 0; ci < nCols; ++ci) {
                        tensor[ti++] = inputColumnValues[ci].get(ri);
                    }
                }
                inputSharedContext.reset();
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

    public static long[] tensorBuffer2DLongColumns(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
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
     * Copy data from a table into a 2d tensor of longs in row-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static long[] tensorBuffer2DLongRows(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
        final int nRows = rowSeq.intSize();
        final int nCols = columnSources.length;
        final long[] tensor = new long[nRows * nCols];

        try (final SafeCloseableList toClose = new SafeCloseableList()) {
            final RowSequence.Iterator rowKeys = toClose.add(rowSeq.getRowSequenceIterator());
            final SharedContext inputSharedContext = toClose.add(SharedContext.makeSharedContext());
            final ChunkSource.GetContext[] inputContexts = toClose.addArray(new ChunkSource.GetContext[nCols]);
            for (int ci = 0; ci < nCols; ++ci) {
                inputContexts[ci] = columnSources[ci].makeGetContext(COPY_CHUNK_SIZE, inputSharedContext);
            }

            // noinspection unchecked
            final LongChunk<? extends Values>[] inputColumnValues = new LongChunk[nCols];
            int ti = 0;
            while (rowKeys.hasMore()) {
                final RowSequence sliceRowKeys = rowKeys.getNextRowSequenceWithLength(COPY_CHUNK_SIZE);
                for (int ci = 0; ci < nCols; ++ci) {
                    inputColumnValues[ci] = columnSources[ci].getChunk(inputContexts[ci], sliceRowKeys).asLongChunk();
                }

                final int sliceChunkSize = sliceRowKeys.intSize();
                for (int ri = 0; ri < sliceChunkSize; ++ri) {
                    for (int ci = 0; ci < nCols; ++ci) {
                        tensor[ti++] = inputColumnValues[ci].get(ri);
                    }
                }
                inputSharedContext.reset();
            }
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of floats in column-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */

    public static float[] tensorBuffer2DFloatColumns(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
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
     * Copy data from a table into a 2d tensor of floats in row-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static float[] tensorBuffer2DFloatRows(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
        final int nRows = rowSeq.intSize();
        final int nCols = columnSources.length;
        final float[] tensor = new float[nRows * nCols];

        try (final SafeCloseableList toClose = new SafeCloseableList()) {
            final RowSequence.Iterator rowKeys = toClose.add(rowSeq.getRowSequenceIterator());
            final SharedContext inputSharedContext = toClose.add(SharedContext.makeSharedContext());
            final ChunkSource.GetContext[] inputContexts = toClose.addArray(new ChunkSource.GetContext[nCols]);
            for (int ci = 0; ci < nCols; ++ci) {
                inputContexts[ci] = columnSources[ci].makeGetContext(COPY_CHUNK_SIZE, inputSharedContext);
            }

            // noinspection unchecked
            final FloatChunk<? extends Values>[] inputColumnValues = new FloatChunk[nCols];
            int ti = 0;
            while (rowKeys.hasMore()) {
                final RowSequence sliceRowKeys = rowKeys.getNextRowSequenceWithLength(COPY_CHUNK_SIZE);
                for (int ci = 0; ci < nCols; ++ci) {
                    inputColumnValues[ci] = columnSources[ci].getChunk(inputContexts[ci], sliceRowKeys).asFloatChunk();
                }

                final int sliceChunkSize = sliceRowKeys.intSize();
                for (int ri = 0; ri < sliceChunkSize; ++ri) {
                    for (int ci = 0; ci < nCols; ++ci) {
                        tensor[ti++] = inputColumnValues[ci].get(ri);
                    }
                }
                inputSharedContext.reset();
            }
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of doubles in column-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static double[] tensorBuffer2DDoubleColumns(final RowSequence rowSeq,
            final ColumnSource<?>[] columnSources) {
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

    /**
     * Copy data from a table into a 2d tensor of doubles in row-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static double[] tensorBuffer2DDoubleRows(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
        final int nRows = rowSeq.intSize();
        final int nCols = columnSources.length;
        final double[] tensor = new double[nRows * nCols];

        try (final SafeCloseableList toClose = new SafeCloseableList()) {
            final RowSequence.Iterator rowKeys = toClose.add(rowSeq.getRowSequenceIterator());
            final SharedContext inputSharedContext = toClose.add(SharedContext.makeSharedContext());
            final ChunkSource.GetContext[] inputContexts = toClose.addArray(new ChunkSource.GetContext[nCols]);
            for (int ci = 0; ci < nCols; ++ci) {
                inputContexts[ci] = columnSources[ci].makeGetContext(COPY_CHUNK_SIZE, inputSharedContext);
            }

            // noinspection unchecked
            final DoubleChunk<? extends Values>[] inputColumnValues = new DoubleChunk[nCols];
            int ti = 0;
            while (rowKeys.hasMore()) {
                final RowSequence sliceRowKeys = rowKeys.getNextRowSequenceWithLength(COPY_CHUNK_SIZE);
                for (int ci = 0; ci < nCols; ++ci) {
                    inputColumnValues[ci] = columnSources[ci].getChunk(inputContexts[ci], sliceRowKeys).asDoubleChunk();
                }

                final int sliceChunkSize = sliceRowKeys.intSize();
                for (int ri = 0; ri < sliceChunkSize; ++ri) {
                    for (int ci = 0; ci < nCols; ++ci) {
                        tensor[ti++] = inputColumnValues[ci].get(ri);
                    }
                }
                inputSharedContext.reset();
            }
        }

        return tensor;
    }

}
