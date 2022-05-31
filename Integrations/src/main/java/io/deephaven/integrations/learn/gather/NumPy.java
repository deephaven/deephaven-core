package io.deephaven.integrations.learn.gather;

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
public class NumPy {

    private static final int COPY_CHUNK_SIZE = 2048;

    /**
     * Copy data from a table into a 2d tensor of Booleans.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @param columnMajorOrder true to return a column-major array; false to return a row-major array
     * @return contiguous RAM allocated for the tensor
     */
    public static boolean[] tensorBuffer2DBoolean(final RowSequence rowSeq, final ColumnSource<?>[] columnSources,
            boolean columnMajorOrder) {
        if (columnMajorOrder) {
            boolean[] tensor = tensorBuffer2DBooleanColumnMajor(rowSeq, columnSources);
            return tensor;
        } else {
            boolean[] tensor = tensorBuffer2DBooleanRowMajor(rowSeq, columnSources);
            return tensor;
        }
    }

    /**
     * Copy data from a table into a 2d tensor of Booleans in column-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor
     */
    private static boolean[] tensorBuffer2DBooleanColumnMajor(final RowSequence rowSeq,
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
     * @return contiguous RAM allocated for the tensor
     */
    private static boolean[] tensorBuffer2DBooleanRowMajor(final RowSequence rowSeq,
            final ColumnSource<?>[] columnSources) {
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
     * Copy data from a table into a 2d tensor of Bytes.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @param columnMajorOrder true to return a column-major array; false to return a row-major array
     * @return contiguous RAM allocated for the tensor
     */
    public static byte[] tensorBuffer2DByte(final RowSequence rowSeq, final ColumnSource<?>[] columnSources,
            boolean columnMajorOrder) {
        if (columnMajorOrder) {
            byte[] tensor = tensorBuffer2DByteColumnMajor(rowSeq, columnSources);
            return tensor;
        } else {
            byte[] tensor = tensorBuffer2DByteRowMajor(rowSeq, columnSources);
            return tensor;
        }
    }

    /**
     * Copy data from a table into a 2d tensor of bytes in column-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor
     */
    private static byte[] tensorBuffer2DByteColumnMajor(final RowSequence rowSeq,
            final ColumnSource<?>[] columnSources) {
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
     * @return contiguous RAM allocated for the tensor
     */
    private static byte[] tensorBuffer2DByteRowMajor(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
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
     * Copy data from a table into a 2d tensor of shorts.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @param columnMajorOrder true to return a column-major array; false to return a row-major array
     * @return contiguous RAM allocated for the tensor
     */
    public static short[] tensorBuffer2DShort(final RowSequence rowSeq, final ColumnSource<?>[] columnSources,
            boolean columnMajorOrder) {
        if (columnMajorOrder) {
            short[] tensor = tensorBuffer2DShortColumnMajor(rowSeq, columnSources);
            return tensor;
        } else {
            short[] tensor = tensorBuffer2DShortRowMajor(rowSeq, columnSources);
            return tensor;
        }
    }

    /**
     * Copy data from a table into a 2d tensor of shorts in column-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor
     */

    private static short[] tensorBuffer2DShortColumnMajor(final RowSequence rowSeq,
            final ColumnSource<?>[] columnSources) {
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
     * @return contiguous RAM allocated for the tensor
     */
    private static short[] tensorBuffer2DShortRowMajor(final RowSequence rowSeq,
            final ColumnSource<?>[] columnSources) {
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
     * Copy data from a table into a 2d tensor of ints.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @param columnMajorOrder true to return a column-major array; false to return a row-major array
     * @return contiguous RAM allocated for the tensor
     */
    public static int[] tensorBuffer2DInt(final RowSequence rowSeq, final ColumnSource<?>[] columnSources,
            boolean columnMajorOrder) {
        if (columnMajorOrder) {
            int[] tensor = tensorBuffer2DIntColumnMajor(rowSeq, columnSources);
            return tensor;
        } else {
            int[] tensor = tensorBuffer2DIntRowMajor(rowSeq, columnSources);
            return tensor;
        }
    }

    /**
     * Copy data from a table into a 2d tensor of ints in column-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor
     */
    private static int[] tensorBuffer2DIntColumnMajor(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
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
     * @return contiguous RAM allocated for the tensor
     */
    private static int[] tensorBuffer2DIntRowMajor(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
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
     * @param columnMajorOrder true to return a column-major array; false to return a row-major array
     * @return contiguous RAM allocated for the tensor
     */
    public static long[] tensorBuffer2DLong(final RowSequence rowSeq, final ColumnSource<?>[] columnSources,
            boolean columnMajorOrder) {
        if (columnMajorOrder) {
            long[] tensor = tensorBuffer2DLongColumnMajor(rowSeq, columnSources);
            return tensor;
        } else {
            long[] tensor = tensorBuffer2DLongRowMajor(rowSeq, columnSources);
            return tensor;
        }
    }

    /**
     * Copy data from a table into a 2d tensor of longs.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor
     */

    private static long[] tensorBuffer2DLongColumnMajor(final RowSequence rowSeq,
            final ColumnSource<?>[] columnSources) {
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
     * @return contiguous RAM allocated for the tensor
     */
    private static long[] tensorBuffer2DLongRowMajor(final RowSequence rowSeq, final ColumnSource<?>[] columnSources) {
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
     * Copy data from a table into a 2d tensor of floats.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @param columnMajorOrder true to return a column-major array; false to return a row-major array
     * @return contiguous RAM allocated for the tensor
     */
    public static float[] tensorBuffer2DFloat(final RowSequence rowSeq, final ColumnSource<?>[] columnSources,
            boolean columnMajorOrder) {
        if (columnMajorOrder) {
            float[] tensor = tensorBuffer2DFloatColumnMajor(rowSeq, columnSources);
            return tensor;
        } else {
            float[] tensor = tensorBuffer2DFloatRowMajor(rowSeq, columnSources);
            return tensor;
        }
    }

    /**
     * Copy data from a table into a 2d tensor of floats in column-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor
     */

    private static float[] tensorBuffer2DFloatColumnMajor(final RowSequence rowSeq,
            final ColumnSource<?>[] columnSources) {
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
     * @return contiguous RAM allocated for the tensor
     */
    private static float[] tensorBuffer2DFloatRowMajor(final RowSequence rowSeq,
            final ColumnSource<?>[] columnSources) {
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
     * Copy data from a table into a 2d tensor of doubles.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @param columnMajorOrder true to return a column-major array; false to return a row-major array
     * @return contiguous RAM allocated for the tensor
     */
    public static double[] tensorBuffer2DDouble(final RowSequence rowSeq, final ColumnSource<?>[] columnSources,
            boolean columnMajorOrder) {
        if (columnMajorOrder) {
            double[] tensor = tensorBuffer2DDoubleColumnMajor(rowSeq, columnSources);
            return tensor;
        } else {
            double[] tensor = tensorBuffer2DDoubleRowMajor(rowSeq, columnSources);
            return tensor;
        }
    }

    /**
     * Copy data from a table into a 2d tensor of doubles in column-major order.
     *
     * @param rowSeq indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor
     */
    private static double[] tensorBuffer2DDoubleColumnMajor(final RowSequence rowSeq,
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
     * @return contiguous RAM allocated for the tensor
     */
    private static double[] tensorBuffer2DDoubleRowMajor(final RowSequence rowSeq,
            final ColumnSource<?>[] columnSources) {
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
