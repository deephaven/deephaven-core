package io.deephaven.integrations.learn;

import io.deephaven.db.v2.sources.ColumnSource;
// REMOVE THIS AFTER REMOVING makeIndexSet!
import io.deephaven.db.tables.Table;

/**
 * Gatherer takes Deephaven columnar data and places it into a buffer to be used by Python. The Python object will take
 * data from the buffer and use it to construct a 2d array of specified size.
 */
public class Gatherer {

    /**
     * Generate an index set from a table. THIS IS JUST FOR EASY PYTHON TESTING. REMOVE BEFORE PROD.
     *
     * @param t table
     * @return index set
     */
    public static IndexSet makeIndexSet(final Table t) {
        final IndexSet is = new IndexSet(t.intSize());

        for (long idx : t.getIndex()) {
            is.add(idx);
        }

        return is;
    }

    /**
     * Copy data from a table into a 2d tensor of Booleans.
     *
     * @param indexSet indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static boolean[] tensorBuffer2DBoolean(final IndexSet indexSet,
                                                  final ColumnSource<?>[] columnSources) {

        final int nRows = indexSet.getSize();
        final int nCols = columnSources.length;
        final boolean[] tensor = new boolean[nRows * nCols];

        int j = 0;
        for (ColumnSource<?> cs : columnSources) {
            int i = 0;

            for (long idx : indexSet) {
                final int ij = nCols * i + j;
                tensor[ij] = cs.getBoolean(idx);
                i++;
            }

            j++;
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of bytes.
     *
     * @param indexSet indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static byte[] tensorBuffer2DByte(final IndexSet indexSet,
                                            final ColumnSource<?>[] columnSources) {

        final int nRows = indexSet.getSize();
        final int nCols = columnSources.length;
        final byte[] tensor = new byte[nRows * nCols];

        int j = 0;
        for (ColumnSource<?> cs : columnSources) {
            int i = 0;

            for (long idx : indexSet) {
                final int ij = nCols * i + j;
                tensor[ij] = cs.getByte(idx);
                i++;
            }

            j++;
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of shorts.
     *
     * @param indexSet indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */

    public static short[] tensorBuffer2DShort(final IndexSet indexSet,
                                              final ColumnSource<?>[] columnSources) {

        final int nRows = indexSet.getSize();
        final int nCols = columnSources.length;
        final short[] tensor = new short[nRows * nCols];

        int j = 0;
        for (ColumnSource<?> cs : columnSources) {
            int i = 0;

            for (long idx : indexSet) {
                final int ij = nCols * i + j;
                tensor[ij] = cs.getShort(idx);
                i++;
            }

            j++;
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of ints.
     *
     * @param indexSet indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static int[] tensorBuffer2DInt(final IndexSet indexSet,
                                          final ColumnSource<?>[] columnSources) {

        final int nRows = indexSet.getSize();
        final int nCols = columnSources.length;
        final int[] tensor = new int[nRows * nCols];

        int j = 0;
        for (ColumnSource<?> cs : columnSources) {
            int i = 0;

            for (long idx : indexSet) {
                final int ij = nCols * i + j;
                tensor[ij] = cs.getInt(idx);
                i++;
            }

            j++;
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of longs.
     *
     * @param indexSet indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */

    public static long[] tensorBuffer2DLong(final IndexSet indexSet,
                                            final ColumnSource<?>[] columnSources) {

        final int nRows = indexSet.getSize();
        final int nCols = columnSources.length;
        final long[] tensor = new long[nRows * nCols];

        int j = 0;
        for (ColumnSource<?> cs : columnSources) {
            int i = 0;

            for (long idx : indexSet) {
                final int ij = nCols * i + j;
                tensor[ij] = cs.getLong(idx);
                i++;
            }

            j++;
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of floats.
     *
     * @param indexSet indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */

    public static float[] tensorBuffer2DFloat(final IndexSet indexSet,
                                              final ColumnSource<?>[] columnSources) {

        final int nRows = indexSet.getSize();
        final int nCols = columnSources.length;
        final float[] tensor = new float[nRows * nCols];

        int j = 0;
        for (ColumnSource<?> cs : columnSources) {
            int i = 0;

            for (long idx : indexSet) {
                final int ij = nCols * i + j;
                tensor[ij] = cs.getFloat(idx);
                i++;
            }

            j++;
        }

        return tensor;
    }

    /**
     * Copy data from a table into a 2d tensor of doubles.
     *
     * @param indexSet indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static double[] tensorBuffer2DDouble(final IndexSet indexSet,
                                                final ColumnSource<?>[] columnSources) {

        final int nRows = indexSet.getSize();
        final int nCols = columnSources.length;
        final double[] tensor = new double[nRows * nCols];

        int j = 0;
        for (ColumnSource<?> cs : columnSources) {
            int i = 0;

            for (long idx : indexSet) {
                final int ij = nCols * i + j;
                tensor[ij] = cs.getDouble(idx);
                i++;
            }

            j++;
        }

        return tensor;
    }

}
