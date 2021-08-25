package io.deephaven.db.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sort.LongMegaMergeKernel;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.utils.*;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.sort.findruns.FindRunsKernel;
import io.deephaven.db.v2.sort.timsort.LongIntTimsortKernel;
import io.deephaven.db.v2.sort.permute.PermuteKernel;
import io.deephaven.db.v2.sort.LongSortKernel;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LongArraySource;
import io.deephaven.db.v2.sources.LongSparseArraySource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.sources.regioned.SymbolTableSource;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.type.TypeUtils;

import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Map;
import java.util.function.LongPredicate;

public class SortHelpers {
    public static boolean sortBySymbolTable =
            Configuration.getInstance().getBooleanWithDefault("QueryTable.sortBySymbolTable", true);

    /**
     * If we have more than this many entries per group, instead of creating a large flat redirection Index, we create a
     * redirection index that is composed of the group indices and an accumulated cardinality cache. This can save a
     * significant amount of memory when the groups are large and storing them using our Index structure is more
     * efficient.
     */
    public static int groupedRedirectionThreshold =
            Configuration.getInstance().getIntegerWithDefault("SortHelpers.groupedRedirectionThreshold", 32);

    /**
     * When the sort is greater than or equal to than megaSortSize, instead of sorting one large chunk, we will sort
     * individual chunks of sortChunkSize and then merge them into ArrayBackedColumnSources with the
     * LongMegaMergeKernel.
     *
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

        RedirectionIndex makeHistoricalRedirectionIndex();
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
        final public long[] getArrayMapping() {
            return Require.neqNull(mapping, "mapping");
        }

        @Override
        final public boolean forEachLong(LongPredicate consumer) {
            for (int ii = 0; ii < mapping.length; ++ii) {
                if (!consumer.test(mapping[ii])) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public RedirectionIndex makeHistoricalRedirectionIndex() {
            return new ContiguousRedirectionIndexImpl(mapping);
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
        final public long size() {
            return size;
        }

        @NotNull
        final public long[] getArrayMapping() {
            throw new ArrayIndexOutOfBoundsException();
        }

        @Override
        final public boolean forEachLong(LongPredicate consumer) {
            assert null != columnSource;
            for (long ii = 0; ii < size; ++ii) {
                if (!consumer.test(columnSource.getLong(ii))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public RedirectionIndex makeHistoricalRedirectionIndex() {
            return new LongColumnSourceRedirectionIndex(columnSource);
        }
    }

    final static class GroupedSortMapping implements SortMapping {
        private final long size;
        private final long[] groupSize;
        private final Index[] groups;

        private GroupedSortMapping(long size, long[] groupSize, Index[] groups) {
            this.size = size;
            this.groupSize = groupSize;
            this.groups = groups;
        }

        @Override
        final public long size() {
            return size;
        }

        @NotNull
        final public long[] getArrayMapping() {
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
        final public boolean forEachLong(LongPredicate consumer) {
            for (int ii = 0; ii < groups.length; ++ii) {
                if (!groups[ii].forEachLong(consumer::test)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public RedirectionIndex makeHistoricalRedirectionIndex() {
            return new GroupedRedirectionIndex(size, groupSize, groups);
        }
    }

    static private final SortMapping EMPTY_SORT_MAPPING = new ArraySortMapping(CollectionUtil.ZERO_LENGTH_LONG_ARRAY);

    /**
     * Note that if usePrev is true, then indexToSort is the previous index; not the current index, and we should not
     * need to call getPrevIndex.
     */
    static SortMapping getSortedKeys(SortingOrder[] order, ColumnSource<Comparable<?>>[] columnsToSortBy,
            ReadOnlyIndex indexToSort, boolean usePrev) {
        return getSortedKeys(order, columnsToSortBy, indexToSort, usePrev, sortBySymbolTable);
    }

    /**
     * Note that if usePrev is true, then indexToSort is the previous index; not the current index, and we should not
     * need to call getPrevIndex.
     */
    static SortMapping getSortedKeys(SortingOrder[] order, ColumnSource<Comparable<?>>[] columnsToSortBy,
            ReadOnlyIndex indexToSort, boolean usePrev, boolean allowSymbolTable) {
        if (indexToSort.size() == 0) {
            return EMPTY_SORT_MAPPING;
        }

        if (columnsToSortBy.length == 1) {
            if (indexToSort.hasGrouping(columnsToSortBy[0])) {
                if (!usePrev || columnsToSortBy[0].isImmutable()) {
                    return getSortMappingGrouped(order[0], columnsToSortBy[0], indexToSort);
                } else {
                    return getSortMappingOne(order[0], columnsToSortBy[0], indexToSort, usePrev);
                }
            } else {
                if (allowSymbolTable && columnsToSortBy[0] instanceof SymbolTableSource
                        && ((SymbolTableSource<Comparable<?>>) columnsToSortBy[0]).hasSymbolTable(indexToSort)) {
                    return doSymbolTableMapping(order[0], columnsToSortBy[0], indexToSort, usePrev);
                } else {
                    return getSortMappingOne(order[0], columnsToSortBy[0], indexToSort, usePrev);
                }
            }
        }

        return getSortMappingMulti(order, columnsToSortBy, indexToSort, usePrev);
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

        private static SparseSymbolMapping createMapping(LongChunk originalSymbol, IntChunk mappedIndex) {
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
                final int mappedId = mappedIndex.get(ii);
                maxMapping = Math.max(maxMapping, mappedId);
                lookupTable[region][id] = mappedId;
            }

            return new SparseSymbolMapping(maxMapping, minTrailing, lookupTable);
        }
    }

    private static final String SORTED_INDEX_COLUMN_NAME = "SortedIndex";
    private static final String SORTED_INDEX_COLUMN_UPDATE = SORTED_INDEX_COLUMN_NAME + "=i";

    private static SortMapping doSymbolTableMapping(SortingOrder order, ColumnSource<Comparable<?>> columnSource,
            ReadOnlyIndex index, boolean usePrev) {
        final int sortSize = index.intSize();

        final ColumnSource<Long> reinterpreted = columnSource.reinterpret(long.class);
        final Table symbolTable = ((SymbolTableSource) columnSource).getStaticSymbolTable(index, true);

        if (symbolTable.size() >= sortSize) {
            // the very first thing we will do is sort the symbol table, using a regular sort; if it is larger than the
            // actual table we care to sort, then it is wasteful to use the symbol table sorting
            return getSortMappingOne(order, columnSource, index, usePrev);
        }

        final Table idMapping = symbolTable.sort(SymbolTableSource.SYMBOL_COLUMN_NAME)
                .by(SymbolTableSource.SYMBOL_COLUMN_NAME).update(SORTED_INDEX_COLUMN_UPDATE).ungroup()
                .view(SymbolTableSource.ID_COLUMN_NAME, SORTED_INDEX_COLUMN_NAME);

        final int symbolEntries = idMapping.intSize();

        final SparseSymbolMapping mapping;

        try (final WritableLongChunk<Values> originalSymbol = WritableLongChunk.makeWritableChunk(symbolEntries);
                final WritableIntChunk<Values> mappedIndex = WritableIntChunk.makeWritableChunk(symbolEntries)) {
            final ColumnSource idSource = idMapping.getColumnSource(SymbolTableSource.ID_COLUMN_NAME);
            try (final ColumnSource.FillContext idContext = idSource.makeFillContext(symbolEntries)) {
                idSource.fillChunk(idContext, originalSymbol, idMapping.getIndex());
            }

            final ColumnSource sortedIndexSource = idMapping.getColumnSource(SORTED_INDEX_COLUMN_NAME);
            try (final ColumnSource.FillContext sortedIndexContext = sortedIndexSource.makeFillContext(symbolEntries)) {
                sortedIndexSource.fillChunk(sortedIndexContext, mappedIndex, idMapping.getIndex());
            }

            mapping = SparseSymbolMapping.createMapping(originalSymbol, mappedIndex);
        }

        // Read the symbol table values into the unmappedValues chunk. The reinterpreted source provides the region and
        // the symbol ID within the region as a packed long, which we then unpack in the type-specific loops below.
        try (final WritableLongChunk<Values> unmappedValues =
                makeAndFillValues(usePrev, index, reinterpreted).asWritableLongChunk()) {

            // Mapped values generic is the chunk of unique integer keys for the sort operation, using the narrowest
            // possible primitive type (byte, short, or int).
            final WritableChunk<Any> mappedValuesGeneric;
            final LongSortKernel<Any, KeyIndices> sortContext;

            if (mapping.getMaxMapping() <= Byte.MAX_VALUE) {
                final WritableByteChunk<Any> mappedValues;
                mappedValues = WritableByteChunk.makeWritableChunk(index.intSize());
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
                final WritableShortChunk<Any> mappedValues = WritableShortChunk.makeWritableChunk(index.intSize());
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
                final WritableIntChunk<Any> mappedValues = WritableIntChunk.makeWritableChunk(index.intSize());
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

            // Fill a chunk that is Writable, and does not have an ordered tag with the index keys that we are sorting,
            // the
            // index would does something very similar inside of io.deephaven.db.v2.utils.OrderedKeys.asKeyIndicesChunk;
            // but provides a LongChunk<OrderedKeyIndices> as its return.
            final long[] indexKeysArray = new long[sortSize];
            final WritableLongChunk<KeyIndices> indexKeys = WritableLongChunk.writableChunkWrap(indexKeysArray);
            index.fillKeyIndicesChunk(indexKeys);
            sortContext.sort(indexKeys, mappedValuesGeneric);
            sortContext.close();
            mappedValuesGeneric.close();

            return new ArraySortMapping(indexKeysArray);
        }
    }

    private static SortMapping getSortMappingOne(SortingOrder order, ColumnSource<Comparable<?>> columnSource,
            ReadOnlyIndex index, boolean usePrev) {
        final long sortSize = index.size();

        if (sortSize >= megaSortSize) {
            return doMegaSortOne(order, columnSource, index, usePrev, sortSize);
        } else {
            return new ArraySortMapping(doChunkSortingOne(order, columnSource, index, usePrev, (int) sortSize));
        }
    }

    @NotNull
    private static SortMapping doMegaSortOne(SortingOrder order, ColumnSource<Comparable<?>> columnSource,
            ReadOnlyIndex index, boolean usePrev, long sortSize) {
        final LongArraySource resultIndices = new LongArraySource();
        resultIndices.ensureCapacity(sortSize, false);
        final ArrayBackedColumnSource<?> valuesToMerge =
                ArrayBackedColumnSource.getMemoryColumnSource(0, columnSource.getType());
        valuesToMerge.ensureCapacity(sortSize, false);

        long accumulatedSize = 0;
        final LongMegaMergeKernel<Values, KeyIndices> longMegaMergeKernel =
                LongMegaMergeKernel.makeContext(columnSource.getChunkType(), order);
        try (final LongSortKernel<Values, KeyIndices> sortContext =
                LongSortKernel.makeContext(columnSource.getChunkType(), order, sortChunkSize, true);
                final OrderedKeys.Iterator okit = index.getOrderedKeysIterator()) {
            while (okit.hasMore()) {
                final OrderedKeys chunkOk = okit.getNextOrderedKeysWithLength(sortChunkSize);
                final int chunkSize = chunkOk.intSize();

                try (final WritableChunk<Values> partialValues = makeAndFillValues(usePrev, chunkOk, columnSource)) {
                    final long[] partialKeysArray = new long[chunkSize];
                    final WritableLongChunk<KeyIndices> partialKeys =
                            WritableLongChunk.writableChunkWrap(partialKeysArray);
                    chunkOk.fillKeyIndicesChunk(partialKeys);

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
            OrderedKeys index, boolean usePrev, int chunkSize) {
        try (final WritableChunk<Values> values = makeAndFillValues(usePrev, index, columnSource)) {
            final long[] indexKeysArray = new long[chunkSize];
            final WritableLongChunk<KeyIndices> indexKeys = WritableLongChunk.writableChunkWrap(indexKeysArray);
            index.fillKeyIndicesChunk(indexKeys);

            try (final LongSortKernel<Values, KeyIndices> sortContext =
                    LongSortKernel.makeContext(columnSource.getChunkType(), order, chunkSize, false)) {
                sortContext.sort(indexKeys, values);
            }

            return indexKeysArray;
        }
    }

    private static SortMapping getSortMappingGrouped(SortingOrder order, ColumnSource<Comparable<?>> columnSource,
            ReadOnlyIndex index) {
        final Map<Object, Index> groupToRange = index.getGrouping(columnSource);
        final Object[] keys = groupToRange.keySet().toArray((Object[]) Array.newInstance(
                io.deephaven.util.type.TypeUtils.getBoxedType(columnSource.getType()), groupToRange.size()));

        Arrays.sort((Comparable<?>[]) keys, order.getComparator());

        final boolean fitsIntoArray = index.size() < (long) Integer.MAX_VALUE;
        final long elementsPerGroup = index.size() / (long) groupToRange.size();

        if (fitsIntoArray && (elementsPerGroup < groupedRedirectionThreshold)) {
            final long[] indexKeysArray = new long[index.intSize()];
            final MutableInt outputIdx = new MutableInt(0);
            for (final Object key : keys) {
                final Index group = groupToRange.get(key).intersect(index);
                group.forAllLongs(indexKey -> {
                    indexKeysArray[outputIdx.intValue()] = indexKey;
                    outputIdx.increment();
                });
            }

            return new ArraySortMapping(indexKeysArray);
        } else {
            // create a grouped redirection index
            final long[] groupSize = new long[groupToRange.size()];
            final Index[] groupIndex = new Index[groupToRange.size()];

            long outputSize = 0;
            int ii = 0;
            for (final Object key : keys) {
                final Index group = groupToRange.get(key).intersect(index);
                outputSize += group.size();
                groupSize[ii] = outputSize;
                groupIndex[ii++] = group;
            }

            return new GroupedSortMapping(outputSize, groupSize, groupIndex);
        }
    }

    private static SortMapping getSortMappingMulti(SortingOrder[] order, ColumnSource<Comparable<?>>[] columnSources,
            ReadOnlyIndex index, boolean usePrev) {
        Assert.gt(columnSources.length, "columnSources.length", 1);
        final int sortSize = index.intSize();

        final long[] indexKeysArray = new long[sortSize];
        final WritableLongChunk<KeyIndices> indexKeys = WritableLongChunk.writableChunkWrap(indexKeysArray);

        WritableIntChunk<ChunkPositions> offsetsOut = WritableIntChunk.makeWritableChunk((sortSize + 1) / 2);
        WritableIntChunk<ChunkLengths> lengthsOut = WritableIntChunk.makeWritableChunk((sortSize + 1) / 2);

        ColumnSource<Comparable<?>> columnSource = columnSources[0];

        if (index.hasGrouping(columnSources[0])) {
            final Map<Comparable<?>, Index> groupToRange = columnSource.getGroupToRange();
            final Object[] keys = groupToRange.keySet().toArray(
                    (Object[]) Array.newInstance(TypeUtils.getBoxedType(columnSource.getType()), groupToRange.size()));

            Arrays.sort((Comparable<?>[]) keys, order[0].getComparator());

            offsetsOut.setSize(0);
            lengthsOut.setSize(0);

            final MutableInt outputIdx = new MutableInt(0);
            for (final Object key : keys) {
                // noinspection SuspiciousMethodCalls
                final Index group = groupToRange.get(key).intersect(index);
                if (group.size() > 1) {
                    offsetsOut.add(outputIdx.intValue());
                    lengthsOut.add(group.intSize());
                }
                group.forAllLongs(indexKey -> {
                    indexKeysArray[outputIdx.intValue()] = indexKey;
                    outputIdx.increment();
                });
            }
        } else {
            index.fillKeyIndicesChunk(indexKeys);

            final ChunkType chunkType = columnSource.getChunkType();

            final WritableChunk<Values> values = makeAndFillValues(usePrev, index, columnSource);
            try (final LongSortKernel<Values, KeyIndices> sortContext =
                    LongSortKernel.makeContext(chunkType, order[0], sortSize, true)) {
                sortContext.sort(indexKeys, values);
            }

            try (final FindRunsKernel findRunsContext = FindRunsKernel.makeContext(chunkType)) {
                findRunsContext.findRuns(values, offsetsOut, lengthsOut);
            }
            values.close();
        }
        if (offsetsOut.size() == 0) {
            lengthsOut.close();
            offsetsOut.close();
            return new ArraySortMapping(indexKeysArray);
        }

        final int totalRunLength = sumChunk(lengthsOut);

        final WritableLongChunk<KeyIndices> indicesToFetch = WritableLongChunk.makeWritableChunk(totalRunLength);
        final WritableIntChunk<ChunkPositions> originalPositions = WritableIntChunk.makeWritableChunk(totalRunLength);
        final LongIntTimsortKernel.LongIntSortKernelContext<KeyIndices, ChunkPositions> sortIndexContext =
                LongIntTimsortKernel.createContext(totalRunLength);

        ChunkType chunkType = columnSources[1].getChunkType();
        int maximumSecondarySize =
                computeIndicesToFetch(indexKeys, offsetsOut, lengthsOut, indicesToFetch, originalPositions);
        WritableChunk<Values> values = fetchSecondaryValues(usePrev, columnSources[1], indicesToFetch,
                originalPositions, sortIndexContext, maximumSecondarySize);
        try (final LongSortKernel<Values, KeyIndices> sortContext =
                LongSortKernel.makeContext(chunkType, order[1], indicesToFetch.size(), columnSources.length != 2)) {
            // and we can sort the stuff within the run now
            sortContext.sort(indexKeys, values, offsetsOut, lengthsOut);
        }

        WritableIntChunk<ChunkPositions> offsetsIn = offsetsOut;
        WritableIntChunk<ChunkLengths> lengthsIn = lengthsOut;
        if (columnSources.length > 2) {
            offsetsOut = WritableIntChunk.makeWritableChunk((totalRunLength + 1) / 2);
            lengthsOut = WritableIntChunk.makeWritableChunk((totalRunLength + 1) / 2);

            for (int columnIndex = 2; columnIndex < columnSources.length; ++columnIndex) {
                columnSource = columnSources[columnIndex];

                try (final FindRunsKernel findRunsContext = FindRunsKernel.makeContext(chunkType)) {
                    findRunsContext.findRuns(values, offsetsIn, lengthsIn, offsetsOut, lengthsOut);
                    if (offsetsOut.size() == 0) {
                        break;
                    }
                }

                chunkType = columnSource.getChunkType();

                maximumSecondarySize =
                        computeIndicesToFetch(indexKeys, offsetsOut, lengthsOut, indicesToFetch, originalPositions);

                values.close();
                values = fetchSecondaryValues(usePrev, columnSources[columnIndex], indicesToFetch, originalPositions,
                        sortIndexContext, maximumSecondarySize);

                try (final LongSortKernel<Values, KeyIndices> sortContext = LongSortKernel.makeContext(chunkType,
                        order[columnIndex], indicesToFetch.size(), columnIndex != columnSources.length - 1)) {
                    // and we can sort the stuff within the run now
                    sortContext.sort(indexKeys, values, offsetsOut, lengthsOut);
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

        return new ArraySortMapping(indexKeysArray);
    }

    private static WritableChunk<Values> fetchSecondaryValues(boolean usePrev, ColumnSource columnSource,
            WritableLongChunk<KeyIndices> indicesToFetch, WritableIntChunk<ChunkPositions> originalPositions,
            LongIntTimsortKernel.LongIntSortKernelContext<KeyIndices, ChunkPositions> sortIndexContext,
            int maximumSecondarySize) {
        sortIndexContext.sort(originalPositions, indicesToFetch);

        try (final WritableChunk<Values> secondaryValues = makeAndFillValues(usePrev,
                OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(WritableLongChunk.downcast(indicesToFetch)),
                columnSource)) {

            final ChunkType chunkType = columnSource.getChunkType();

            // make the big chunk that can hold all the relevant secondary values, in their desired position
            final WritableChunk<Values> values = chunkType.makeWritableChunk(maximumSecondarySize);

            final PermuteKernel permuteKernel = PermuteKernel.makePermuteKernel(chunkType);
            permuteKernel.permute(secondaryValues, originalPositions, values);
            return values;
        }
    }

    private static int computeIndicesToFetch(WritableLongChunk<KeyIndices> indexKeys,
            WritableIntChunk<ChunkPositions> offsetsOut, WritableIntChunk<ChunkLengths> lengthsOut,
            WritableLongChunk<KeyIndices> indicesToFetch, WritableIntChunk<ChunkPositions> originalPositions) {
        indicesToFetch.setSize(0);
        originalPositions.setSize(0);
        int maximumSecondarySize = 0;
        for (int ii = 0; ii < offsetsOut.size(); ++ii) {
            final int runStart = offsetsOut.get(ii);
            final int runLength = lengthsOut.get(ii);

            maximumSecondarySize = Math.max(maximumSecondarySize, runStart + runLength);

            for (int jj = 0; jj < runLength; ++jj) {
                indicesToFetch.add(indexKeys.get(runStart + jj));
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
    private static WritableChunk<Values> makeAndFillValues(boolean usePrev, OrderedKeys ok,
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
    static RedirectionIndex createSortRedirectionIndex() {
        final WritableSource<Long> sparseLongSource = new LongSparseArraySource();
        return new LongColumnSourceRedirectionIndex(sparseLongSource);
    }
}
