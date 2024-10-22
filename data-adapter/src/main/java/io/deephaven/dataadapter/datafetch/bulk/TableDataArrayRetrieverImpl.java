//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.datafetch.bulk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.dataadapter.ContextHolder;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.LongConsumer;

public final class TableDataArrayRetrieverImpl implements TableDataArrayRetriever {

    private static final int MAX_CHUNK_SIZE = ChunkUtils.DEFAULT_CHUNK_SIZE;

    /**
     * The column sources whose data is retrieved.
     */
    @SuppressWarnings("rawtypes")
    private final ColumnSource[] columnSources;

    /**
     * The number of columns for which data is retrieved (i.e. length of {@link #columnSources}).
     */
    private final int nCols;

    public TableDataArrayRetrieverImpl(final ColumnSource<?>[] columnSources) {
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

    public @NotNull Object[] createDataArrays(final int len) {
        return createDataArrays(len, columnSources);
    }

    public static @NotNull Object[] createDataArrays(final int len, final ColumnSource<?>[] columnSources) {
        final Object[] arrays = new Object[columnSources.length];
        for (int i = 0; i < columnSources.length; i++) {
            // TODO: meh, maekArray makes Object[] arrays not SomeActualType[] arrays
            // arrays[i] = chunkTypes[i].makeArray(len);
            ColumnSource<?> colSource = columnSources[i];
            final ChunkType chunkType = colSource.getChunkType();
            if (chunkType != ChunkType.Object) {
                arrays[i] = chunkType.makeArray(len);
            } else {
                // narrower type makes it simpler when populating records (don't have to cast when extracting from arr)
                arrays[i] = Array.newInstance(colSource.getType(), len);
            }
        }
        return arrays;
    }

    public void fillDataArrays(final boolean usePrev, final RowSet tableIndex, final Object[] dataArrs,
            final LongConsumer keyConsumer) {
        final int chunkSize = Math.min(MAX_CHUNK_SIZE, tableIndex.intSize());
        try (final ContextHolder contextHolder = new ContextHolder(chunkSize, columnSources)) {
            fillDataArrays(usePrev, tableIndex, columnSources, dataArrs, keyConsumer, contextHolder);
        }
    }

    public static void fillDataArrays(final boolean usePrev,
            final RowSet tableIndex,
            final ColumnSource<?>[] columnSources,
            final Object[] dataArrs,
            final LongConsumer keyConsumer,
            final ContextHolder contextHolder) {
        int arrIdx = 0;
        try (final RowSequence.Iterator iter = tableIndex.getRowSequenceIterator()) {
            while (iter.hasMore()) {
                final RowSequence rowSequence = iter.getNextRowSequenceWithLength(contextHolder.getChunkSize());
                final int rowSequenceSize = rowSequence.intSize();

                populateArraysForRowSequence(
                        rowSequence,
                        columnSources,
                        usePrev,
                        contextHolder,
                        dataArrs,
                        arrIdx,
                        rowSequenceSize);

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
     * @param rowSeq The index keys for which to retrieve data
     * @param columnSources The column sources to retrieve data from
     * @param usePrev Whether to use prev values ({@code true} while LTM is updating)
     * @param typedContext The context holder for managing FillContexts and chunks
     * @param arrays The arrays to populate with data
     * @param arrOffset The starting array index to populate in the {@code arrays}
     * @param nRows The number of rows for which to retrieve data
     */
    public static void populateArraysForRowSequence(final RowSequence rowSeq,
            final ColumnSource<?>[] columnSources,
            final boolean usePrev,
            final ContextHolder typedContext,
            final Object[] arrays,
            final int arrOffset,
            final int nRows) {
        for (int ci = 0; ci < columnSources.length; ++ci) {
            final ChunkSource.FillContext fillContext = typedContext.getFillContext(ci);
            final ResettableWritableChunk<Values> chunk = typedContext.getResettableChunk(ci);
            final Object array = arrays[ci];
            chunk.resetFromArray(array, arrOffset, nRows);
            final ColumnSource<?> colSrc = columnSources[ci];

            if (usePrev) {
                colSrc.fillPrevChunk(fillContext, chunk, rowSeq);
            } else {
                colSrc.fillChunk(fillContext, chunk, rowSeq);
            }

            // TODO: why clear()? who cares?
            chunk.clear();
        }
    }
}
