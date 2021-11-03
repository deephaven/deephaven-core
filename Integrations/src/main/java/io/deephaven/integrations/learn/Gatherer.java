package io.deephaven.integrations.learn;

import io.deephaven.db.v2.sources.ColumnSource;

public class Gatherer {

    /**
     * Copy data from a table into a 2d tensor of floats.
     *
     * @param indexSet indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */

    public static Boolean[] booleanTensorBuffer2D(final IndexSet indexSet, final ColumnSource<?>[] columnSources) {

        final int nRows = indexSet.getSize();
        final int nCols = columnSources.length;
        final Boolean[] tensor = new Boolean[nRows * nCols];

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

    public static byte[] byteTensorBuffer2D(final IndexSet indexSet, final ColumnSource<?>[] columnSources) {

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
     * Copy data from a table into a 2d tensor of doubles.
     *
     * @param indexSet indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static double[] doubleTensorBuffer2D(final IndexSet indexSet, final ColumnSource<?>[] columnSources) {

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

    /**
     * Copy data from a table into a 2d tensor of floats.
     *
     * @param indexSet indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */

    public static float[] floatTensorBuffer2D(final IndexSet indexSet, final ColumnSource<?>[] columnSources) {

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
     * Copy data from a table into a 2d tensor of ints.
     *
     * @param indexSet indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */

    public static int[] intTensorBuffer2D(final IndexSet indexSet, final ColumnSource<?>[] columnSources) {

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

    public static long[] longTensorBuffer2D(final IndexSet indexSet, final ColumnSource<?>[] columnSources) {

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
     * Copy data from a table into a 2d tensor of shorts.
     *
     * @param indexSet indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */

    public static short[] shortTensorBuffer2D(final IndexSet indexSet, final ColumnSource<?>[] columnSources) {

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

}
