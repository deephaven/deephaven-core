package io.deephaven.queryutil.dataadapter.datafetch.bulk.simple;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.queryutil.dataadapter.ContextHolder;
import io.deephaven.queryutil.dataadapter.datafetch.bulk.AbstractTableDataArrayRetriever;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;

import java.lang.reflect.Array;

/**
 * Created by rbasralian on 3/7/22
 */
@Deprecated
public class SimpleTableDataArrayRetriever extends AbstractTableDataArrayRetriever {

    /**
     * Utilities to retrieve chunks of data from each of the {@link #columnSources}.
     */
    @SuppressWarnings("rawtypes")
    private final ChunkedArrPopulator[] chunkToArrPopulators;

    public SimpleTableDataArrayRetriever(final ColumnSource<?>[] columnSources) {
        super(columnSources);

        chunkToArrPopulators = new ChunkedArrPopulator[nCols];

        for (int i = 0; i < columnSources.length; i++) {
            chunkToArrPopulators[i] = ChunkedArrPopulator.getChunkToArrPopulator(columnSources[i].getType());
        }
    }

    /**
     * Create arrays to hold the data from each column source.
     *
     * @param len The length of the arrays
     * @return An array of empty arrays, parallel to the array of {@link #columnSources}
     */
    @Override
    public Object[] createDataArrays(final int len) {
        final Object[] recordDataArrs = new Object[nCols];
        for (int i = 0; i < nCols; i++) {
            ColumnSource<?> columnSource = columnSources[i];
            recordDataArrs[i] = Array.newInstance(columnSource.getType(), len);
        }
        return recordDataArrs;
    }

    protected void populateArrsForRowSequence(boolean usePrev, Object[] dataArrs, int arrIdx, ContextHolder contextHolder, RowSequence rowSequence, int rowSequenceSize) {
        for (int i = 0; i < nCols; i++) {
            //noinspection unchecked
            chunkToArrPopulators[i].populateArrFromChunk(
                    columnSources[i],
                    rowSequence,
                    rowSequenceSize,
                    contextHolder.getGetContext(i),
                    dataArrs[i],
                    arrIdx,
                    usePrev
            );
        }
    }

}
