package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.engine.table.GroupingBuilder;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.util.WrappedRowSetRowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.GroupingProvider.INDEX_COL_NAME;

/**
 * The base implementation for {@link GroupingBuilder} provides fields and methods that are common to all
 * implementations.
 */
public abstract class AbstractGroupingBuilder implements GroupingBuilder {
    private static final int CHUNK_SIZE = 2048;

    protected final List<Function<Table, Table>> regionMutators = new ArrayList<>();
    protected RowSet rowSetOfInterest;
    protected boolean strictIntersect = false;
    protected boolean sortByFirstKey = false;
    protected RowSet inverter;
    protected boolean matchCase;
    protected boolean invert;
    protected Object[] groupKeys;

    private static class MemoKey implements GroupingMemoKey {
        private final RowSet indexOfInterest;
        private final boolean strictIntersect;
        private final boolean sortByFirstKey;
        private final RowSet inverter;
        private final List<GroupingMemoKey> regionKeys;
        private final boolean matchCase;
        private final boolean invert;
        private final Object[] groupKeys;

        public MemoKey(RowSet indexOfInterest,
                boolean strictIntersect,
                boolean sortByFirstKey,
                RowSet inverter,
                List<GroupingMemoKey> regionKeys,
                boolean matchCase,
                boolean invert,
                Object[] groupKeys) {
            this.indexOfInterest = indexOfInterest;
            this.strictIntersect = strictIntersect;
            this.sortByFirstKey = sortByFirstKey;
            this.inverter = inverter;
            this.regionKeys = regionKeys;
            this.matchCase = matchCase;
            this.invert = invert;
            this.groupKeys = groupKeys;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            MemoKey memoKey = (MemoKey) o;
            return strictIntersect == memoKey.strictIntersect &&
                    sortByFirstKey == memoKey.sortByFirstKey &&
                    matchCase == memoKey.matchCase &&
                    invert == memoKey.invert &&
                    Objects.equals(indexOfInterest, memoKey.indexOfInterest) &&
                    Objects.equals(inverter, memoKey.inverter) &&
                    Objects.equals(regionKeys, memoKey.regionKeys) &&
                    Arrays.equals(groupKeys, memoKey.groupKeys);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(indexOfInterest, strictIntersect, sortByFirstKey, inverter, regionKeys, matchCase,
                    invert);
            result = 31 * result + Arrays.hashCode(groupKeys);
            return result;
        }
    }

    @NotNull
    @Override
    public GroupingBuilder addRegionMutator(@NotNull Function<Table, Table> mutator) {
        regionMutators.add(mutator);
        return this;
    }

    @NotNull
    @Override
    public GroupingBuilder sortByFirstKey() {
        this.sortByFirstKey = true;
        return this;
    }

    @NotNull
    @Override
    public GroupingBuilder matching(boolean matchCase, boolean invert, Object... groupKeys) {
        if (groupKeys == null || groupKeys.length == 0) {
            throw new IllegalArgumentException("A non-null, non-empty list of keys to match must be provided");
        }

        this.matchCase = matchCase;
        this.invert = invert;
        this.groupKeys = groupKeys;

        return this;
    }

    @NotNull
    @Override
    public GroupingBuilder clampToIndex(@NotNull RowSet indexOfInterest, boolean strictIntersect) {
        this.rowSetOfInterest = indexOfInterest;
        this.strictIntersect = strictIntersect;
        return this;
    }

    public GroupingBuilder positionally(@NotNull RowSet inverter) {
        this.inverter = inverter;
        return this;
    }

    @Nullable
    @Override
    public <T> Map<T, RowSet> buildGroupingMap() {
        final Table groupingTable = buildTable();
        if (groupingTable == null) {
            return null;
        }

        if (groupingTable.isEmpty()) {
            return Collections.emptyMap();
        }

        return QueryPerformanceRecorder.withNugget("Convert Grouping Table to Map:", () -> convertToMap(groupingTable));
    }

    /**
     * Convert the final table into a Map of key.
     *
     * @param groupingTable the table to convert
     * @param <DATA_TYPE> the type of the key
     * @return the grouping table converted into a {@link Map}
     */
    @NotNull
    protected <DATA_TYPE> Map<DATA_TYPE, RowSet> convertToMap(@Nullable final Table groupingTable) {
        if (groupingTable == null) {
            return null;
        }

        final Map<DATA_TYPE, RowSet> grouping = new LinkedHashMap<>();
        // noinspection unchecked
        final ColumnSource<DATA_TYPE> keySource = groupingTable.getColumnSource(getValueColumnName());
        // noinspection unchecked
        final ColumnSource<RowSet> indexSource = groupingTable.getColumnSource(INDEX_COL_NAME);

        final int chunkSize = Math.min(CHUNK_SIZE, groupingTable.intSize());
        try (final ChunkSource.GetContext keyGetContext = keySource.makeGetContext(chunkSize);
                final ChunkSource.GetContext indexGetContext = indexSource.makeGetContext(chunkSize);
                final ChunkBoxer.BoxerKernel boxer =
                        ChunkBoxer.getBoxer(ChunkType.fromElementType(keySource.getType()), chunkSize);
                final RowSequence.Iterator okIt = groupingTable.getRowSet().getRowSequenceIterator()) {

            while (okIt.hasMore()) {
                final RowSequence nextKeys = okIt.getNextRowSequenceWithLength(chunkSize);
                // noinspection unchecked
                final ObjectChunk<DATA_TYPE, ? extends Values> keyChunk =
                        (ObjectChunk<DATA_TYPE, ? extends Values>) boxer
                                .box(keySource.getChunk(keyGetContext, nextKeys));
                final ObjectChunk<RowSet, ? extends Values> indexChunk =
                        indexSource.getChunk(indexGetContext, nextKeys).asObjectChunk();

                for (int ii = 0; ii < nextKeys.size(); ii++) {
                    grouping.put(keyChunk.get(ii), indexChunk.get(ii));
                }
            }
        }

        return grouping;
    }

    /**
     * Apply strict intersection and invert operations as required by the current builder state.
     *
     * @param groupingTable the table to apply the operations to.
     * @return the table with intersections and inversions applied.
     */
    protected Table applyIntersectAndInvert(@NotNull final Table groupingTable) {
        if (!strictIntersect && inverter == null) {
            return groupingTable;
        }

        final Function<RowSet, RowSet> mutator;
        if (strictIntersect && inverter == null) {
            mutator = index -> index.intersect(rowSetOfInterest);
        } else if (!strictIntersect) {
            mutator = index -> inverter.invert(index);
        } else {
            mutator = index -> {
                try (final WritableRowSet intersected = index.intersect(rowSetOfInterest)) {
                    return inverter.invert(intersected);
                }
            };
        }

        final ColumnSource<?> keySource = groupingTable.getColumnSource(getValueColumnName());
        // noinspection unchecked
        final ColumnSource<RowSet> indexSource = groupingTable.getColumnSource(INDEX_COL_NAME);

        final RowSetBuilderSequential redirectionBuilder = RowSetFactory.builderSequential();
        final ObjectArraySource<RowSet> resultIndexSource =
                (ObjectArraySource<RowSet>) ArrayBackedColumnSource.getMemoryColumnSource(RowSet.class, null);

        final int chunkSize = Math.min(CHUNK_SIZE, groupingTable.intSize());
        try (final RowSequence.Iterator okIt = groupingTable.getRowSet().getRowSequenceIterator();
                final ChunkSource.GetContext indexCtx = indexSource.makeGetContext(chunkSize)) {

            long outputPosition = 0;
            while (okIt.hasMore()) {
                final RowSequence ok = okIt.getNextRowSequenceWithLength(chunkSize);
                resultIndexSource.ensureCapacity(outputPosition + ok.size());

                final LongChunk<OrderedRowKeys> okChunk = ok.asRowKeyChunk();
                final ObjectChunk<RowSet, ? extends Values> indexChunk =
                        indexSource.getChunk(indexCtx, ok).asObjectChunk();

                for (int ii = 0; ii < ok.size(); ii++) {
                    final RowSet permutedRowSet = mutator.apply(indexChunk.get(ii));
                    if (permutedRowSet != null && permutedRowSet.isNonempty()) {
                        resultIndexSource.set(outputPosition++, permutedRowSet);
                        redirectionBuilder.appendKey(okChunk.get(ii));
                    }
                }
            }

            final WrappedRowSetRowRedirection redirection =
                    new WrappedRowSetRowRedirection(redirectionBuilder.build().toTracking());
            final ColumnSource<?> resultKeySource = RedirectedColumnSource.maybeRedirect(redirection, keySource);
            final Map<String, ColumnSource<?>> csm = new LinkedHashMap<>();
            csm.put(getValueColumnName(), resultKeySource);
            csm.put(INDEX_COL_NAME, resultIndexSource);
            return new QueryTable(RowSetFactory.flat(outputPosition).toTracking(), csm);
        }
    }

    /**
     * If requested, sort the input table by the first key of it's index column.
     *
     * @param grouping the input grouping.
     * @return the table sorted by first key, if requested.
     */
    protected Table maybeSortByFirsKey(final @NotNull Table grouping) {
        if (!sortByFirstKey) {
            return grouping;
        }

        return grouping.updateView("FirstKey=" + getIndexColumnName() + ".firstKey()")
                .sort("FirstKey")
                .dropColumns("FirstKey");
    }

    /**
     * If {@link #matching(boolean, boolean, Object...) matching} was requested, apply it to the parameter table.
     *
     * @param baseTable the table to apply to
     * @return the table with matching applied if required.
     */
    protected Table maybeApplyMatch(final @NotNull Table baseTable) {
        if (groupKeys == null) {
            return baseTable;
        }

        return baseTable.where(new MatchFilter(
                matchCase ? MatchFilter.CaseSensitivity.MatchCase : MatchFilter.CaseSensitivity.IgnoreCase,
                invert ? MatchFilter.MatchType.Inverted : MatchFilter.MatchType.Regular,
                getValueColumnName(),
                groupKeys));
    }

    protected Table condenseGrouping(final @NotNull Table baseTable) {
        final long beforeSize = baseTable.size();
        final Table byGroup = baseTable.groupBy(getValueColumnName());

        // If the table size is the same after a by() it means that there were no duplicate groups, so we
        // don't have to waste time and memory combining them, we can just return the thing.
        if (byGroup.size() == beforeSize) {
            return baseTable;
        }

        return byGroup.update("Index=com.illumon.iris.db.v2.dataindex.DataIndexProviderImpl.combineIndices(Index)");
    }

    /**
     * Create a {@link GroupingMemoKey} for the current builder state.
     * 
     * @return a memo key
     */
    protected GroupingMemoKey makeMemoKey() {
        if (!isMemoizable()) {
            return null;
        }

        return new MemoKey(rowSetOfInterest,
                strictIntersect,
                sortByFirstKey,
                inverter,
                regionMutators.isEmpty() ? null
                        : regionMutators.stream().map(m -> (GroupingMemoKey) m).collect(Collectors.toList()),
                matchCase,
                invert,
                groupKeys);
    }

    /**
     * Check if the current configuration is memoizable.
     * 
     * @return true if it is memoizable
     */
    protected boolean isMemoizable() {
        if (!regionMutators.isEmpty()) {
            for (Function<Table, Table> mutator : regionMutators) {
                if (!(mutator instanceof GroupingMemoKey)) {
                    return false;
                }
            }
        }

        return true;
    }
}
