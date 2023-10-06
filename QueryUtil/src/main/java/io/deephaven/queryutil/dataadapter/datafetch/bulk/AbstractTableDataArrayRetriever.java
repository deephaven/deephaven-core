package io.deephaven.queryutil.dataadapter.datafetch.bulk;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.queryutil.dataadapter.ContextHolder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.LongConsumer;

public abstract class AbstractTableDataArrayRetriever implements TableDataArrayRetriever {

    private static final int MAX_CHUNK_SIZE = 4096;

    /**
     * The column sources whose data is retrieved.
     */
    @SuppressWarnings("rawtypes")
    protected final ColumnSource[] columnSources;

    /**
     * The number of columns for which data is retrieved (i.e. length of {@link #columnSources}).
     */
    protected final int nCols;

    public AbstractTableDataArrayRetriever(final ColumnSource<?>[] columnSources) {
        this.columnSources = columnSources;
        this.nCols = this.columnSources.length;

        Assert.neqNull(columnSources, "columnSources");
        Assert.gtZero(nCols, "nCols");
        for (int i = 0; i < nCols; i++) {
            Assert.neqNull(columnSources[i], "columnSources[" + i + ']');
        }
    }

    @Override
    public List<ColumnSource<?>> getColumnSources() {
        return Collections.unmodifiableList(Arrays.<ColumnSource<?>>asList(columnSources));
    }

    public void fillDataArrays(final boolean usePrev, final RowSet tableIndex, final Object[] dataArrs,
            final LongConsumer keyConsumer) {
        final int size = tableIndex.intSize();

        final int chunkSize = Math.min(MAX_CHUNK_SIZE, size);

        int arrIdx = 0;
        try (final ContextHolder contextHolder = new ContextHolder(chunkSize, columnSources)) {
            for (final RowSequence.Iterator iter = tableIndex.getRowSequenceIterator(); iter.hasMore();) {
                final RowSequence rowSequence = iter.getNextRowSequenceWithLength(chunkSize);
                final int rowSequenceSize = rowSequence.intSize();

                populateArrsForRowSequence(usePrev, dataArrs, arrIdx, contextHolder, rowSequence, rowSequenceSize);

                if (keyConsumer != null) {
                    rowSequence.forAllRowKeys(keyConsumer);
                }
                arrIdx += rowSequenceSize;
            }
        } catch (Exception e) {
            throw new RuntimeException("Exception while populating arrays or closing contexts", e);
        }

    }

    /**
     * Populates the data arrays with data for the given index keys ({@code orderedKeys}).
     *
     * @param usePrev Whether to use prev values ({@code true} while LTM is updating)
     * @param dataArrs The arrays to populate with data
     * @param arrIdx The starting array index to populate {@code dataArrs}
     * @param contextHolder The context holder for managing {@link io.deephaven.engine.table.ChunkSource.GetContext
     *        GetContexts}
     * @param rowSequence The index keys for which to retrieve data
     * @param rowSequenceSize The number of rows for which to retrieve data
     */
    protected abstract void populateArrsForRowSequence(boolean usePrev, Object[] dataArrs, int arrIdx,
            ContextHolder contextHolder, RowSequence rowSequence, int rowSequenceSize);
}
