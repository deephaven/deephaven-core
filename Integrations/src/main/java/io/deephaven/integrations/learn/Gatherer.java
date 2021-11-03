package io.deephaven.integrations.learn;

import io.deephaven.db.v2.sources.ColumnSource;

public class Gatherer {

    /**
     * Copy data from a table into a 2d tensor.
     *
     * @param indexSet indices of the rows of the table to put into the tensor
     * @param columnSources columns of data to put into the tensor
     * @return contiguous RAM allocated for the tensor. When a numpy tensor is passed in for this argument, jpy will
     *         handle passing the memory reference as a 1d java array here.
     */
    public static double[] create_2d_tensor(final IndexSet indexSet, final ColumnSource<?>[] columnSources) {

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
