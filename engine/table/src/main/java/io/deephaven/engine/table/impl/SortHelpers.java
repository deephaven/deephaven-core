//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sort.LongMegaMergeKernel;
import io.deephaven.engine.table.impl.sort.LongSortKernel;
import io.deephaven.engine.table.impl.sort.findruns.FindRunsKernel;
import io.deephaven.engine.table.impl.sort.permute.PermuteKernel;
import io.deephaven.engine.table.impl.sort.timsort.LongIntTimsortKernel;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.sources.regioned.SymbolTableSource;
import io.deephaven.engine.table.impl.util.ContiguousWritableRowRedirection;
import io.deephaven.engine.table.impl.util.GroupedWritableRowRedirection;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.function.LongPredicate;

public class SortHelpers {

    public static boolean sortBySymbolTable =
            Configuration.getInstance().getBooleanWithDefault("QueryTable.sortBySymbolTable", true);

    /**
     * If we have more than this many entries per group, instead of creating a large flat row redirection, we create a
     * row redirection that is composed of the group indices and an accumulated cardinality cache. This can save a
     * significant amount of memory when the groups are large and storing them using our RowSet structure is more
     * efficient.
     */
    public static int groupedRedirectionThreshold =
            Configuration.getInstance().getIntegerWithDefault("SortHelpers.groupedRedirectionThreshold", 32);

    /**
     * When the sort is greater than or equal to than megaSortSize, instead of sorting one large chunk, we will sort
     * individual chunks of sortChunkSize and then merge them into ArrayBackedColumnSources with the
     * LongMegaMergeKernel.
     * <p>
     * There are some boundary conditions in the chunk sizing math that make Integer.MAX_VALUE fail; you could probably
     * back off to Integer.MAX_VALUE - 32 safely. We're being very conservative with 1 << 30 instead.
     */
    @VisibleForTesting
    static int megaSortSize = Configuration.getInstance().getIntegerWithDefault("QueryTable.sortChunkSize", 1 << 30);
    /**
     * The size of each chunk of a sort in a mega-merge based sort.
     */
    @VisibleForTesting
    static int sortChunkSize = Configuration.getInstance().getIntegerWithDefault("QueryTable.sortChunkSize", 1 << 30);

    interface SortMapping extends LongSizedDataStructure {
        long size();

        long[] getArrayMapping();

        boolean forEachLong(LongPredicate consumer);

        WritableRowRedirection makeHistoricalRowRedirection();
    }

    final static class ArraySortMapping implements SortMapping {
        /**
         * The total size of the mapping.
         */
        final int size;

        /**
         * The mapping as an array.
         */
        @NotNull
        final long[] mapping;

        private ArraySortMapping(long[] mapping) {
            size = mapping.length;
            this.mapping = mapping;
        }

        @Override
        final public long size() {
            return size;
        }

        @NotNull
        public long[] getArrayMapping() {
            return Require.neqNull(mapping, "mapping");
        }

        @Override
        public boolean forEachLong(LongPredicate consumer) {
            for (int ii = 0; ii < mapping.length; ++ii) {
                if (!consumer.test(mapping[ii])) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public WritableRowRedirection makeHistoricalRowRedirection() {
            return new ContiguousWritableRowRedirection(mapping);
        }
    }

    final static class ColumnSourceSortMapping implements SortMapping {
        /**
         * The total size of the mapping.
         */
        final long size;

        /**
         * The mapping as a column source, in locations 0..size-1
         */
        final LongArraySource columnSource;

        private ColumnSourceSortMapping(LongArraySource columnSource, long size) {
            this.size = size;
            this.columnSource = columnSource;
        }

        @Override
        public long size() {
            return size;
        }

        @NotNull
        public long[] getArrayMapping() {
            throw new ArrayIndexOutOfBoundsException();
        }

        @Override
        public boolean forEachLong(LongPredicate consumer) {
            assert null != columnSource;
            for (long ii = 0; ii < size; ++ii) {
                if (!consumer.test(columnSource.getLong(ii))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public WritableRowRedirection makeHistoricalRowRedirection() {
            return new LongColumnSourceWritableRowRedirection(columnSource);
        }
    }

    final static class IndexedSortMapping implements SortMapping {
        private final long size;
        private final long[] groupSize;
        private final RowSet[] groups;

        private IndexedSortMapping(long size, long[] groupSize, RowSet[] groups) {
            this.size = size;
            this.groupSize = groupSize;
            this.groups = groups;
        }

        @Override
        public long size() {
            return size;
        }

        @NotNull
        public long[] getArrayMapping() {
            if (size <= Integer.MAX_VALUE) {
                final long[] mapping = new long[(int) size];
                final MutableInt pos = new MutableInt(0);
                forEachLong(idx -> {
                    mapping[pos.getAndIncrement()] = idx;
                    return true;
                });
                return mapping;
            } else {
                throw new ArrayIndexOutOfBoundsException();
            }
        }

        @Override
        public boolean forEachLong(LongPredicate consumer) {
            for (int ii = 0; ii < groups.length; ++ii) {
                if (!groups[ii].forEachRowKey(consumer::test)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public WritableRowRedirection makeHistoricalRowRedirection() {
            return new GroupedWritableRowRedirection(size, groupSize, groups);
        }
    }

    static private final SortMapping EMPTY_SORT_MAPPING = new ArraySortMapping(ArrayTypeUtils.EMPTY_LONG_ARRAY);

    /**
     * Note that if usePrev is true, then rowSetToSort is the previous RowSet; not the current RowSet, and we should not
     * need to call prev().
     */
    static SortMapping getSortedKeys(
            final SortingOrder[] order,
            final ColumnSource<Comparable<?>>[] originalColumnsToSortBy,
            final ColumnSource<Comparable<?>>[] columnsToSortBy,
            final DataIndex dataIndex,
            final RowSet rowSetToSort,
            final boolean usePrev) {
        return getSortedKeys(order, originalColumnsToSortBy, columnsToSortBy, dataIndex, rowSetToSort, usePrev,
                sortBySymbolTable);
    }

    /**
     * Note that if usePrev is true, then rowSetToSort is the previous RowSet; not the current RowSet, and we should not
     * need to call prev().
     */
    static SortMapping getSortedKeys(
            final SortingOrder[] order,
            final ColumnSource<Comparable<?>>[] originalColumnsToSortBy,
            final ColumnSource<Comparable<?>>[] columnsToSortBy,
            final DataIndex dataIndex,
            final RowSet rowSetToSort,
            final boolean usePrev,
            final boolean allowSymbolTable) {
        if (rowSetToSort.isEmpty()) {
            return EMPTY_SORT_MAPPING;
        }

        // Don't use a full index if it is too large.
        if (dataIndex != null
                && dataIndex.keyColumnNames().size() == columnsToSortBy.length
                && rowSetToSort.size() > dataIndex.table().size() * 2L) {
            return getSortMappingIndexed(order, originalColumnsToSortBy, dataIndex, rowSetToSort, usePrev);
        }

        if (columnsToSortBy.length == 1) {
            if (allowSymbolTable && columnsToSortBy[0] instanceof SymbolTableSource
                    && ((SymbolTableSource<Comparable<?>>) columnsToSortBy[0]).hasSymbolTable(rowSetToSort)) {
                return doSymbolTableMapping(order[0], columnsToSortBy[0], rowSetToSort, usePrev);
            } else {
                return getSortMappingOne(order[0], columnsToSortBy[0], rowSetToSort, usePrev);
            }
        }

        return getSortMappingMulti(order, originalColumnsToSortBy, columnsToSortBy, dataIndex, rowSetToSort, usePrev);
    }

    private static class SparseSymbolMapping {
        private final int maxMapping;
        private final int minTrailing;
        private final int[][] lookupTable;

        private SparseSymbolMapping(int maxMapping, int minTrailing, int[][] lookupTable) {
            this.maxMapping = maxMapping;
            this.minTrailing = minTrailing;
            this.lookupTable = lookupTable;
        }

        private int getMaxMapping() {
            return maxMapping;
        }

        private byte lookupByte(long symTabId) {
            if (symTabId == QueryConstants.NULL_LONG) {
                return QueryConstants.NULL_BYTE;
            }
            return (byte) doIntLookup(symTabId);
        }

        private short lookupShort(long symTabId) {
            if (symTabId == QueryConstants.NULL_LONG) {
                return QueryConstants.NULL_SHORT;
            }
            return (short) doIntLookup(symTabId);
        }

        private int lookupInt(long symTabId) {
            if (symTabId == QueryConstants.NULL_LONG) {
                return QueryConstants.NULL_INT;
            }
            return doIntLookup(symTabId);
        }

        private int doIntLookup(long symTabId) {
            final int region = (int) (symTabId >> (32 + minTrailing));
            final int id = (int) symTabId;
            return lookupTable[region][id];
        }

        private static SparseSymbolMapping createMapping(
                @NotNull final LongChunk<Values> originalSymbol,
                @NotNull final LongChunk<Values> mappedIndex) {
            // figure out what the maximum region is, and determine how many bits of it there are
            int maxUpperPart = 0;
            int minTrailing = 32;
            int maxSymbol = 0;
            for (int ii = 0; ii < originalSymbol.size(); ++ii) {
                final long symTabId = originalSymbol.get(ii);
                final long upperPart = (symTabId >> 32);
                maxUpperPart = Math.max(maxUpperPart, (int) upperPart);
                minTrailing = Math.min(minTrailing, Integer.numberOfTrailingZeros((int) upperPart));
                maxSymbol = Math.max(maxSymbol, (int) symTabId);
            }
            final int maxShiftedRegion = maxUpperPart >> minTrailing;
            if (minTrailing == 32) {
                // this means we only found a zero region, in which case we do not want to shift by 64, but rather by
                // zero to just truncate the region entirely
                Assert.eqZero(maxShiftedRegion, "maxShiftedRegion");
                minTrailing = 0;
            }

            final int[][] lookupTable = new int[maxShiftedRegion + 1][maxSymbol + 1];

            // maxMapping ends up being the number of unique string values that we have. We compute it so that we can
            // map symbol IDs to these unique integers using the narrowest primitive sorting kernel possible.
            int maxMapping = 0;

            for (int ii = 0; ii < originalSymbol.size(); ++ii) {
                final long symTabId = originalSymbol.get(ii);
                final int region = (int) (symTabId >> (32 + minTrailing));
                final int id = (int) symTabId;
                final int mappedId = Math.toIntExact(mappedIndex.get(ii));
                maxMapping = Math.max(maxMapping, mappedId);
                lookupTable[region][id] = mappedId;
            }

            return new SparseSymbolMapping(maxMapping, minTrailing, lookupTable);
        }
    }

    private static final String SORTED_INDEX_COLUMN_NAME = "SortedIndex";

    private static SortMapping doSymbolTableMapping(SortingOrder order, ColumnSource<Comparable<?>> columnSource,
            RowSet rowSet, boolean usePrev) {
        final int sortSize = rowSet.intSize();

        final ColumnSource<Long> reinterpreted = columnSource.reinterpret(long.class);
        final Table symbolTable = ((SymbolTableSource<?>) columnSource).getStaticSymbolTable(rowSet, true);

        if (symbolTable.isEmpty()) {
            // All nulls, so we can just return the row set as the sort mapping
            return new IndexedSortMapping(rowSet.size(), new long[] {rowSet.size()}, new RowSet[] {rowSet});
        }

        if (symbolTable.size() >= sortSize) {
            // the very first thing we will do is sort the symbol table, using a regular sort; if it is larger than the
            // actual table we care to sort, then it is wasteful to use the symbol table sorting
            return getSortMappingOne(order, columnSource, rowSet, usePrev);
        }

        final QueryTable groupedSymbols = (QueryTable) symbolTable.sort(SymbolTableSource.SYMBOL_COLUMN_NAME)
                .groupBy(SymbolTableSource.SYMBOL_COLUMN_NAME).coalesce();
        final Map<String, ColumnSource<?>> extraColumn;
        if (groupedSymbols.isFlat()) {
            extraColumn = Map.of(SORTED_INDEX_COLUMN_NAME, RowKeyColumnSource.INSTANCE);
        } else {
            extraColumn = Map.of(SORTED_INDEX_COLUMN_NAME, new RowPositionColumnSource(groupedSymbols.getRowSet()));
        }
        final Table idMapping = groupedSymbols.withAdditionalColumns(extraColumn)
                .ungroup()
                .view(SymbolTableSource.ID_COLUMN_NAME, SORTED_INDEX_COLUMN_NAME);

        final int symbolEntries = idMapping.intSize();

        final SparseSymbolMapping mapping;

        try (final WritableLongChunk<Values> originalSymbol = WritableLongChunk.makeWritableChunk(symbolEntries);
                final WritableLongChunk<Values> mappedIndex = WritableLongChunk.makeWritableChunk(symbolEntries)) {
            final ColumnSource<?> idSource = idMapping.getColumnSource(SymbolTableSource.ID_COLUMN_NAME);
            try (final ColumnSource.FillContext idContext = idSource.makeFillContext(symbolEntries)) {
                idSource.fillChunk(idContext, originalSymbol, idMapping.getRowSet());
            }

            final ColumnSource<?> sortedRowSetSource = idMapping.getColumnSource(SORTED_INDEX_COLUMN_NAME);
            try (final ColumnSource.FillContext sortedIndexContext =
                    sortedRowSetSource.makeFillContext(symbolEntries)) {
                sortedRowSetSource.fillChunk(sortedIndexContext, mappedIndex, idMapping.getRowSet());
            }

            mapping = SparseSymbolMapping.createMapping(originalSymbol, mappedIndex);
        }

        // Read the symbol table values into the unmappedValues chunk. The reinterpreted source provides the region and
        // the symbol ID within the region as a packed long, which we then unpack in the type-specific loops below.
        try (final WritableLongChunk<Values> unmappedValues =
                makeAndFillValues(usePrev, rowSet, reinterpreted).asWritableLongChunk()) {

            // Mapped values generic is the chunk of unique integer keys for the sort operation, using the narrowest
            // possible primitive type (byte, short, or int).
            final WritableChunk<Any> mappedValuesGeneric;
            final LongSortKernel<Any, RowKeys> sortContext;

            if (mapping.getMaxMapping() <= Byte.MAX_VALUE) {
                final WritableByteChunk<Any> mappedValues;
                mappedValues = WritableByteChunk.makeWritableChunk(rowSet.intSize());
                for (int ii = 0; ii < unmappedValues.size(); ++ii) {
                    // symTabId is the packed ID, nulls get converted to the right kind of null, other values are pulled
                    // apart into region and id; and then used as offsets in the lookup table to get the unique sorted
                    // id
                    // and the mapped value is added to the mappedValues chunk
                    mappedValues.set(ii, mapping.lookupByte(unmappedValues.get(ii)));
                }
                sortContext = LongSortKernel.makeContext(ChunkType.Byte, order, sortSize, false);
                mappedValuesGeneric = mappedValues;
            } else if (mapping.getMaxMapping() <= Short.MAX_VALUE) {
                final WritableShortChunk<Any> mappedValues = WritableShortChunk.makeWritableChunk(rowSet.intSize());
                for (int ii = 0; ii < unmappedValues.size(); ++ii) {
                    // symTabId is the packed ID, nulls get converted to the right kind of null, other values are pulled
                    // apart into region and id; and then used as offsets in the lookup table to get the unique sorted
                    // id
                    // and the mapped value is added to the mappedValues chunk
                    mappedValues.set(ii, mapping.lookupShort(unmappedValues.get(ii)));
                }
                sortContext = LongSortKernel.makeContext(ChunkType.Short, order, sortSize, false);
                mappedValuesGeneric = mappedValues;
            } else {
                final WritableIntChunk<Any> mappedValues = WritableIntChunk.makeWritableChunk(rowSet.intSize());
                for (int ii = 0; ii < unmappedValues.size(); ++ii) {
                    // symTabId is the packed ID, nulls get converted to the right kind of null, other values are pulled
                    // apart into region and id; and then used as offsets in the lookup table to get the unique sorted
                    // id
                    // and the mapped value is added to the mappedValues chunk
                    mappedValues.set(ii, mapping.lookupInt(unmappedValues.get(ii)));

                }
                sortContext = LongSortKernel.makeContext(ChunkType.Int, order, sortSize, false);
                mappedValuesGeneric = mappedValues;
            }

            // Fill a chunk that is Writable, and does not have an ordered tag with the row keys that we are sorting,
            // the RowSet would do something very similar inside
            // io.deephaven.engine.table.impl.util.RowSequence.asRowKeyChunk; but provides a LongChunk<OrderedRowKeys>
            // as its return.
            final long[] rowKeysArray = new long[sortSize];
            final WritableLongChunk<RowKeys> rowKeys = WritableLongChunk.writableChunkWrap(rowKeysArray);
            rowSet.fillRowKeyChunk(rowKeys);
            sortContext.sort(rowKeys, mappedValuesGeneric);
            sortContext.close();
            mappedValuesGeneric.close();

            return new ArraySortMapping(rowKeysArray);
        }
    }

    private static SortMapping getSortMappingOne(SortingOrder order, ColumnSource<Comparable<?>> columnSource,
            RowSet rowSet, boolean usePrev) {
        final long sortSize = rowSet.size();

        if (sortSize >= megaSortSize) {
            return doMegaSortOne(order, columnSource, rowSet, usePrev, sortSize);
        } else {
            return new ArraySortMapping(doChunkSortingOne(order, columnSource, rowSet, usePrev, (int) sortSize));
        }
    }

    @NotNull
    private static SortMapping doMegaSortOne(SortingOrder order, ColumnSource<Comparable<?>> columnSource,
            RowSet rowSet, boolean usePrev, long sortSize) {
        final LongArraySource resultIndices = new LongArraySource();
        resultIndices.ensureCapacity(sortSize, false);
        final WritableColumnSource<?> valuesToMerge =
                ArrayBackedColumnSource.getMemoryColumnSource(0, columnSource.getType());
        valuesToMerge.ensureCapacity(sortSize, false);

        long accumulatedSize = 0;
        final LongMegaMergeKernel<Values, RowKeys> longMegaMergeKernel =
                LongMegaMergeKernel.makeContext(columnSource.getChunkType(), order);
        try (final LongSortKernel<Values, RowKeys> sortContext =
                LongSortKernel.makeContext(columnSource.getChunkType(), order, sortChunkSize, true);
                final RowSequence.Iterator rsIt = rowSet.getRowSequenceIterator()) {
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(sortChunkSize);
                final int chunkSize = chunkOk.intSize();

                try (final WritableChunk<Values> partialValues = makeAndFillValues(usePrev, chunkOk, columnSource)) {
                    final long[] partialKeysArray = new long[chunkSize];
                    final WritableLongChunk<RowKeys> partialKeys =
                            WritableLongChunk.writableChunkWrap(partialKeysArray);
                    chunkOk.fillRowKeyChunk(partialKeys);

                    sortContext.sort(partialKeys, partialValues);

                    longMegaMergeKernel.merge(resultIndices, valuesToMerge, 0, accumulatedSize, partialKeys,
                            partialValues);
                    accumulatedSize += chunkSize;
                }
            }
        }

        return new ColumnSourceSortMapping(resultIndices, sortSize);
    }

    @NotNull
    private static long[] doChunkSortingOne(SortingOrder order, ColumnSource<Comparable<?>> columnSource,
            RowSequence rowSequence, boolean usePrev, int chunkSize) {
        try (final WritableChunk<Values> values = makeAndFillValues(usePrev, rowSequence, columnSource)) {
            final long[] rowKeysArray = new long[chunkSize];
            final WritableLongChunk<RowKeys> rowKeys = WritableLongChunk.writableChunkWrap(rowKeysArray);
            rowSequence.fillRowKeyChunk(rowKeys);

            try (final LongSortKernel<Values, RowKeys> sortContext =
                    LongSortKernel.makeContext(columnSource.getChunkType(), order, chunkSize, false)) {
                sortContext.sort(rowKeys, values);
            }

            return rowKeysArray;
        }
    }

    private static SortMapping getSortMappingIndexed(SortingOrder[] order, ColumnSource<Comparable<?>>[] columnSources,
            DataIndex dataIndex, RowSet rowSet, boolean usePrev) {
        Assert.neqNull(dataIndex, "dataIndex");

        final Table indexTable = dataIndex.table();
        final RowSet indexRowSet = usePrev ? indexTable.getRowSet().prev() : indexTable.getRowSet();
        // noinspection unchecked
        final ColumnSource<Comparable<?>>[] originalIndexKeyColumns =
                (ColumnSource<Comparable<?>>[]) dataIndex.keyColumns(columnSources);
        // noinspection unchecked
        final ColumnSource<Comparable<?>>[] indexKeyColumns =
                (ColumnSource<Comparable<?>>[]) ReinterpretUtils.maybeConvertToPrimitive(originalIndexKeyColumns);
        final SortMapping indexMapping = getSortedKeys(order, originalIndexKeyColumns, indexKeyColumns, null,
                indexRowSet, usePrev);

        final String rowSetColumnName = dataIndex.rowSetColumnName();
        final ColumnSource<RowSet> rawRowSetColumn = dataIndex.table().getColumnSource(rowSetColumnName, RowSet.class);
        final ColumnSource<RowSet> redirectedRowSetColumn = RedirectedColumnSource.alwaysRedirect(
                indexMapping.makeHistoricalRowRedirection(),
                usePrev ? rawRowSetColumn.getPrevSource() : rawRowSetColumn);

        final boolean fitsIntoArray = rowSet.size() < (long) Integer.MAX_VALUE;
        final long elementsPerGroup = rowSet.size() / indexRowSet.size();
        if (fitsIntoArray && (elementsPerGroup < groupedRedirectionThreshold)) {
            final long[] rowKeysArray = new long[rowSet.intSize()];

            final MutableInt nextOutputOffset = new MutableInt(0);
            try (final RowSequence flat = RowSequenceFactory.forRange(0, indexRowSet.size() - 1);
                    final CloseableIterator<RowSet> groups = ChunkedColumnIterator.make(redirectedRowSetColumn, flat)) {
                groups.forEachRemaining((final RowSet group) -> group
                        .forAllRowKeys(rowKey -> rowKeysArray[nextOutputOffset.getAndIncrement()] = rowKey));
            }

            return new ArraySortMapping(rowKeysArray);
        } else {
            final int indexSize = indexRowSet.intSize();
            final long[] cumulativeSize = new long[indexSize];
            final RowSet[] groupRowSet = new RowSet[indexSize];

            final MutableInt nextGroupOffset = new MutableInt(0);
            try (final RowSequence flat = RowSequenceFactory.forRange(0, indexRowSet.size() - 1);
                    final CloseableIterator<RowSet> groups = ChunkedColumnIterator.make(redirectedRowSetColumn, flat)) {
                groups.forEachRemaining((final RowSet group) -> {
                    final int go = nextGroupOffset.getAndIncrement();
                    cumulativeSize[go] = go == 0 ? group.size() : cumulativeSize[go - 1] + group.size();
                    groupRowSet[go] = group; // No need to copy(): either static, or never used after instantiation
                });
            }

            Assert.eq(cumulativeSize[indexSize - 1], "cumulativeSize[indexSize - 1]", rowSet.size(), "rowSet.size()");
            return new IndexedSortMapping(rowSet.size(), cumulativeSize, groupRowSet);
        }
    }

    private static SortMapping getSortMappingMulti(
            final SortingOrder[] order,
            final ColumnSource<Comparable<?>>[] originalColumnSources,
            final ColumnSource<Comparable<?>>[] columnSources,
            final DataIndex dataIndex,
            final RowSet rowSet,
            boolean usePrev) {
        Assert.gt(columnSources.length, "columnSources.length", 1);
        final int sortSize = rowSet.intSize();

        final long[] rowKeysArray = new long[sortSize];
        final WritableLongChunk<RowKeys> rowKeys = WritableLongChunk.writableChunkWrap(rowKeysArray);

        WritableIntChunk<ChunkPositions> offsetsOut = WritableIntChunk.makeWritableChunk((sortSize + 1) / 2);
        WritableIntChunk<ChunkLengths> lengthsOut = WritableIntChunk.makeWritableChunk((sortSize + 1) / 2);

        ColumnSource<Comparable<?>> columnSource = columnSources[0];

        // Can we utilize an existing index on the first column?
        if (dataIndex != null
                && dataIndex.keyColumnNames().size() == 1
                && dataIndex.keyColumnNamesByIndexedColumn().containsKey(originalColumnSources[0])) {
            final Table indexTable = dataIndex.table();
            final RowSet indexRowSet = usePrev ? indexTable.getRowSet().prev() : indexTable.getRowSet();
            final ColumnSource<Comparable<?>> originalIndexKeyColumn = indexTable.getColumnSource(
                    dataIndex.keyColumnNamesByIndexedColumn().get(originalColumnSources[0]),
                    originalColumnSources[0].getType());
            // noinspection unchecked
            final ColumnSource<Comparable<?>> indexColumn =
                    (ColumnSource<Comparable<?>>) ReinterpretUtils.maybeConvertToPrimitive(originalIndexKeyColumn);

            final SortMapping indexMapping = getSortMappingOne(order[0], indexColumn, indexRowSet, usePrev);

            final String rowSetColumnName = dataIndex.rowSetColumnName();
            final ColumnSource<RowSet> rawRowSetColumn =
                    dataIndex.table().getColumnSource(rowSetColumnName, RowSet.class);
            final ColumnSource<RowSet> redirectedRowSetColumn = RedirectedColumnSource.alwaysRedirect(
                    indexMapping.makeHistoricalRowRedirection(),
                    usePrev ? rawRowSetColumn.getPrevSource() : rawRowSetColumn);

            rowKeys.setSize(0);
            offsetsOut.setSize(0);
            lengthsOut.setSize(0);
            final WritableIntChunk<ChunkPositions> fOffsetsOut = offsetsOut;
            final WritableIntChunk<ChunkLengths> fLengthsOut = lengthsOut;
            try (final RowSequence flat = RowSequenceFactory.forRange(0, indexRowSet.size() - 1);
                    final CloseableIterator<RowSet> groups = ChunkedColumnIterator.make(redirectedRowSetColumn, flat)) {
                groups.forEachRemaining((final RowSet group) -> {
                    if (group.size() > 1) {
                        fOffsetsOut.add(rowKeys.size());
                        fLengthsOut.add(group.intSize());
                    }
                    group.forAllRowKeys(rowKeys::add);
                });
            }
        } else {
            rowSet.fillRowKeyChunk(rowKeys);

            final ChunkType chunkType = columnSource.getChunkType();

            final WritableChunk<Values> values = makeAndFillValues(usePrev, rowSet, columnSource);
            try (final LongSortKernel<Values, RowKeys> sortContext =
                    LongSortKernel.makeContext(chunkType, order[0], sortSize, true)) {
                sortContext.sort(rowKeys, values);
            }

            final FindRunsKernel findRunsKernel = FindRunsKernel.getInstance(chunkType);
            findRunsKernel.findRuns(values, offsetsOut, lengthsOut);
            values.close();
        }
        if (offsetsOut.size() == 0) {
            lengthsOut.close();
            offsetsOut.close();
            return new ArraySortMapping(rowKeysArray);
        }

        final int totalRunLength = sumChunk(lengthsOut);

        final WritableLongChunk<RowKeys> indicesToFetch = WritableLongChunk.makeWritableChunk(totalRunLength);
        final WritableIntChunk<ChunkPositions> originalPositions = WritableIntChunk.makeWritableChunk(totalRunLength);
        final LongIntTimsortKernel.LongIntSortKernelContext<RowKeys, ChunkPositions> sortIndexContext =
                LongIntTimsortKernel.createContext(totalRunLength);

        ChunkType chunkType = columnSources[1].getChunkType();
        int maximumSecondarySize =
                computeIndicesToFetch(rowKeys, offsetsOut, lengthsOut, indicesToFetch, originalPositions);
        WritableChunk<Values> values = fetchSecondaryValues(usePrev, columnSources[1], indicesToFetch,
                originalPositions, sortIndexContext, maximumSecondarySize);
        try (final LongSortKernel<Values, RowKeys> sortContext =
                LongSortKernel.makeContext(chunkType, order[1], indicesToFetch.size(), columnSources.length != 2)) {
            // and we can sort the stuff within the run now
            sortContext.sort(rowKeys, values, offsetsOut, lengthsOut);
        }

        WritableIntChunk<ChunkPositions> offsetsIn = offsetsOut;
        WritableIntChunk<ChunkLengths> lengthsIn = lengthsOut;
        if (columnSources.length > 2) {
            offsetsOut = WritableIntChunk.makeWritableChunk((totalRunLength + 1) / 2);
            lengthsOut = WritableIntChunk.makeWritableChunk((totalRunLength + 1) / 2);

            for (int columnIndex = 2; columnIndex < columnSources.length; ++columnIndex) {
                columnSource = columnSources[columnIndex];

                final FindRunsKernel findRunsKernel = FindRunsKernel.getInstance(chunkType);
                findRunsKernel.findRuns(values, offsetsIn, lengthsIn, offsetsOut, lengthsOut);
                if (offsetsOut.size() == 0) {
                    break;
                }

                chunkType = columnSource.getChunkType();

                maximumSecondarySize =
                        computeIndicesToFetch(rowKeys, offsetsOut, lengthsOut, indicesToFetch, originalPositions);

                values.close();
                values = fetchSecondaryValues(usePrev, columnSources[columnIndex], indicesToFetch, originalPositions,
                        sortIndexContext, maximumSecondarySize);

                try (final LongSortKernel<Values, RowKeys> sortContext = LongSortKernel.makeContext(chunkType,
                        order[columnIndex], indicesToFetch.size(), columnIndex != columnSources.length - 1)) {
                    // and we can sort the stuff within the run now
                    sortContext.sort(rowKeys, values, offsetsOut, lengthsOut);
                }

                final WritableIntChunk<ChunkPositions> tempOff = offsetsIn;
                final WritableIntChunk<ChunkLengths> tempLen = lengthsIn;

                offsetsIn = offsetsOut;
                lengthsIn = lengthsOut;

                offsetsOut = tempOff;
                lengthsOut = tempLen;
            }

            offsetsIn.close();
            lengthsIn.close();
        }
        values.close();
        lengthsOut.close();
        offsetsOut.close();
        sortIndexContext.close();
        originalPositions.close();
        indicesToFetch.close();

        return new ArraySortMapping(rowKeysArray);
    }

    private static WritableChunk<Values> fetchSecondaryValues(boolean usePrev, ColumnSource<?> columnSource,
            WritableLongChunk<RowKeys> indicesToFetch, WritableIntChunk<ChunkPositions> originalPositions,
            LongIntTimsortKernel.LongIntSortKernelContext<RowKeys, ChunkPositions> sortIndexContext,
            int maximumSecondarySize) {
        sortIndexContext.sort(originalPositions, indicesToFetch);

        try (final WritableChunk<Values> secondaryValues = makeAndFillValues(usePrev,
                RowSequenceFactory.wrapRowKeysChunkAsRowSequence(WritableLongChunk.downcast(indicesToFetch)),
                columnSource)) {

            final ChunkType chunkType = columnSource.getChunkType();

            // make the big chunk that can hold all the relevant secondary values, in their desired position
            final WritableChunk<Values> values = chunkType.makeWritableChunk(maximumSecondarySize);

            final PermuteKernel permuteKernel = PermuteKernel.makePermuteKernel(chunkType);
            permuteKernel.permute(secondaryValues, originalPositions, values);
            return values;
        }
    }

    private static int computeIndicesToFetch(WritableLongChunk<RowKeys> rowKeys,
            WritableIntChunk<ChunkPositions> offsetsOut, WritableIntChunk<ChunkLengths> lengthsOut,
            WritableLongChunk<RowKeys> indicesToFetch, WritableIntChunk<ChunkPositions> originalPositions) {
        indicesToFetch.setSize(0);
        originalPositions.setSize(0);
        int maximumSecondarySize = 0;
        for (int ii = 0; ii < offsetsOut.size(); ++ii) {
            final int runStart = offsetsOut.get(ii);
            final int runLength = lengthsOut.get(ii);

            maximumSecondarySize = Math.max(maximumSecondarySize, runStart + runLength);

            for (int jj = 0; jj < runLength; ++jj) {
                indicesToFetch.add(rowKeys.get(runStart + jj));
                originalPositions.add(runStart + jj);
            }
        }
        return maximumSecondarySize;
    }

    private static int sumChunk(IntChunk<? extends Any> lengthsOut) {
        int sum = 0;
        for (int ii = 0; ii < lengthsOut.size(); ++ii) {
            sum += lengthsOut.get(ii);
        }
        return sum;
    }

    @NotNull
    private static WritableChunk<Values> makeAndFillValues(boolean usePrev, RowSequence ok,
            ColumnSource<?> columnSource) {
        final int sortSize = LongSizedDataStructure.intSize("SortHelper.makeAndFillValues", ok.size());

        final WritableChunk<Values> values = columnSource.getChunkType().makeWritableChunk(sortSize);

        try (final ColumnSource.FillContext primaryColumnSourceContext = columnSource.makeFillContext(sortSize)) {
            if (usePrev) {
                columnSource.fillPrevChunk(primaryColumnSourceContext, values, ok);
            } else {
                columnSource.fillChunk(primaryColumnSourceContext, values, ok);
            }
        }

        return values;
    }

    @NotNull
    static WritableRowRedirection createSortRowRedirection() {
        final WritableColumnSource<Long> sparseLongSource = new LongSparseArraySource();
        return new LongColumnSourceWritableRowRedirection(sparseLongSource);
    }
}
