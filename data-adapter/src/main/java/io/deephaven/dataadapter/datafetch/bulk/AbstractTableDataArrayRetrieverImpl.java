//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.datafetch.bulk;

import gnu.trove.list.TLongList;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.dataadapter.ContextHolder;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableDefinition;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.IntFunction;


public abstract class AbstractTableDataArrayRetrieverImpl implements TableDataArrayRetriever {
    protected static final int MAX_CHUNK_SIZE = 4096;

    private final List<String> colNamesToRetrieve;

    private final @NotNull IntFunction<Object[]> dataArraysFactory;

    public AbstractTableDataArrayRetrieverImpl(List<String> colNamesToRetrieve, TableDefinition tableDefinition) {
        Assert.gtZero(colNamesToRetrieve.size(), "colNamesToRetrieve.size()");
        tableDefinition.checkHasColumns(colNamesToRetrieve);

        this.colNamesToRetrieve = colNamesToRetrieve;

        // Create a factory to create appropriately-sized arrays for retrieving the data
        dataArraysFactory = createDataArraysFactory(colNamesToRetrieve.stream()
                .map(tableDefinition::getColumn)
                .map(ColumnDefinition::getDataType)
                .toArray(Class<?>[]::new));
    }

    @Override
    public List<String> getColumnNames() {
        return Collections.unmodifiableList(colNamesToRetrieve);
    }

    @Deprecated
    public static @NotNull Object[] createDataArrays(final int len, final ColumnSource<?>[] columnSources) {
        // just get the colTypes and call the version of createDataArrays that takes colTypes
        return createDataArrays(len, Arrays.stream(columnSources).map(ColumnSource::getType).toArray(Class<?>[]::new));
    }

    @Deprecated // use createDataArraysFactory()
    public static @NotNull Object[] createDataArrays(final int len, final Class<?>[] colTypes) {
        final Object[] arrays = new Object[colTypes.length];
        for (int i = 0; i < colTypes.length; i++) {
            Class<?> colType = colTypes[i];

            final ChunkType chunkType = ChunkType.fromColumnDataType(colType);
            if (chunkType != ChunkType.Object) {
                arrays[i] = chunkType.makeArray(len);
            } else {
                // narrower type makes it simpler when populating records (don't have to cast when extracting from arr)
                arrays[i] = Array.newInstance(colType, len);
            }
        }
        return arrays;
    }

    public @NotNull Object[] createDataArrays(final int len) {
        return dataArraysFactory.apply(len);
    }

    public static @NotNull IntFunction<Object[]> createDataArraysFactory(final Class<?>[] colTypes) {
        final int nCols = colTypes.length;

        // noinspection unchecked
        final IntFunction<Object>[] arrayCreators = new IntFunction[nCols];
        for (int i = 0; i < nCols; i++) {
            final Class<?> colType = colTypes[i];
            final ChunkType chunkType = ChunkType.fromColumnDataType(colType);
            if (chunkType != ChunkType.Object) {
                arrayCreators[i] = chunkType::makeArray;
            } else {
                // narrower type makes it simpler when populating records (don't have to cast when extracting from arr)
                arrayCreators[i] = len -> Array.newInstance(colType, len);
            }
        }

        return len -> {
            final Object[] arrays = new Object[nCols];
            for (int i = 0; i < nCols; i++) {
                arrays[i] = arrayCreators[i].apply(len);
            }
            return arrays;
        };
    }

    public static void fillDataArrays(final ColumnSource<?>[] columnSources,
            final boolean usePrev,
            final RowSet rowSet,
            final Object[] dataArrs,
            final TLongList recordDataKeys) {
        final int chunkSize = Math.min(MAX_CHUNK_SIZE, rowSet.intSize());
        try (final ContextHolder contextHolder = new ContextHolder(chunkSize, columnSources)) {
            AbstractTableDataArrayRetrieverImpl.fillDataArrays(usePrev, rowSet, columnSources, dataArrs,
                    recordDataKeys, contextHolder, 0);
        }
    }

    public static void fillDataArrays(final boolean usePrev,
            final RowSet rowSet,
            final ColumnSource<?>[] columnSources,
            final Object[] dataArrs,
            final TLongList recordDataKeys,
            final ContextHolder contextHolder,
            final int startOffset) {
        int arrIdx = startOffset;
        try (final RowSequence.Iterator iter = rowSet.getRowSequenceIterator()) {
            while (iter.hasMore()) {
                final RowSequence rowSequence = iter.getNextRowSequenceWithLength(contextHolder.getChunkSize());
                final int rowSequenceSize = rowSequence.intSize();

                AbstractTableDataArrayRetrieverImpl.populateArraysForRowSequence(
                        rowSequence,
                        columnSources,
                        usePrev,
                        contextHolder,
                        dataArrs,
                        arrIdx,
                        rowSequenceSize);

                if (recordDataKeys != null) {
                    rowSequence.forAllRowKeys(recordDataKeys::add);
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
        }
    }

}
