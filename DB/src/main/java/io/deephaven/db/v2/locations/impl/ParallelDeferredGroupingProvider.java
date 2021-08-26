/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations.impl;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.locations.KeyRangeGroupingProvider;
import io.deephaven.db.v2.utils.CurrentOnlyIndex;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Grouping provider that loads column location metadata and assembles grouping indexes lazily on demand.
 */
public class ParallelDeferredGroupingProvider<DATA_TYPE> implements KeyRangeGroupingProvider<DATA_TYPE> {

    private static final boolean SORT_RANGES = false;

    private final ColumnDefinition<DATA_TYPE> columnDefinition;

    public ParallelDeferredGroupingProvider(@NotNull final ColumnDefinition<DATA_TYPE> columnDefinition) {
        this.columnDefinition = columnDefinition;
    }

    private interface RangeAccessor<T> extends Comparator<T> {

        @Override
        default int compare(@NotNull final T o1, @NotNull final T o2) {
            return Long.compare(getRangeStartInclusive(o1), getRangeStartInclusive(o2));
        }

        long getRangeStartInclusive(@NotNull T range);

        long getRangeEndInclusive(@NotNull T range);
    }

    private static final RangeAccessor<int[]> INT_RANGE_ACCESSOR = new RangeAccessor<int[]>() {

        @Override
        public long getRangeStartInclusive(@NotNull final int[] range) {
            return range[0];
        }

        @Override
        public long getRangeEndInclusive(@NotNull final int[] range) {
            return range[1] - 1;
        }
    };

    private static final RangeAccessor<long[]> LONG_RANGE_ACCESSOR = new RangeAccessor<long[]>() {

        @Override
        public long getRangeStartInclusive(@NotNull final long[] range) {
            return range[0];
        }

        @Override
        public long getRangeEndInclusive(@NotNull final long[] range) {
            return range[1] - 1;
        }
    };

    private static class Source<DATA_TYPE, RANGE_TYPE> {

        private final ColumnLocation columnLocation;
        private final long firstKey;
        private final long lastKey;

        /**
         * We a reference to the resultant grouping information. This allows us to avoid re-reading grouping
         * information.
         * <p>
         * The reference is soft to avoid having the cache result in an OOM.
         */
        private SoftReference<Map<DATA_TYPE, RANGE_TYPE>> cachedResult;

        private Source(@NotNull final ColumnLocation columnLocation, final long firstKey, final long lastKey) {
            Require.neqNull(columnLocation, "columnLocation");
            Require.leq(firstKey, "firstKey", lastKey, "lastKey");
            this.columnLocation = columnLocation;
            this.firstKey = firstKey;
            this.lastKey = lastKey;
        }

        /**
         * Load the grouping metadata for from this source.
         * <p>
         * If the grouping information was already read, get it from the cached results map. Any grouping information
         * read by this method will be stored in the cached results map.
         *
         * @param columnDefinition The definition of this column
         * @return Grouping metadata as a map from value to position range within this source, or null if the grouping
         *         information was not present
         */
        private Map<DATA_TYPE, RANGE_TYPE> loadMetadata(@NotNull final ColumnDefinition<DATA_TYPE> columnDefinition) {
            if (!columnLocation.exists()) {
                // noinspection unchecked
                return (Map<DATA_TYPE, RANGE_TYPE>) Collections.singletonMap(null,
                        new long[] {0, lastKey - firstKey + 1});
            }

            Map<DATA_TYPE, RANGE_TYPE> valuesToLocationIndexRange = null;
            if (cachedResult != null) {
                valuesToLocationIndexRange = cachedResult.get();
            }

            if (valuesToLocationIndexRange == null) {
                // noinspection unchecked
                if ((valuesToLocationIndexRange =
                        (Map<DATA_TYPE, RANGE_TYPE>) columnLocation.getMetadata(columnDefinition)) != null) {
                    cachedResult = new SoftReference<>(valuesToLocationIndexRange);
                }
            }

            return valuesToLocationIndexRange;
        }

        /**
         * Apply validation and transformation steps for this source's result.
         *
         * @param valuesToLocationIndexRange The metadata as a map from value to position range within this source
         * @return A list of grouping items to be applied to grouping builder.
         */
        private List<GroupingItem<DATA_TYPE>> validateAndTransformMetadata(
                @NotNull final Map<DATA_TYPE, RANGE_TYPE> valuesToLocationIndexRange) {
            final long locationSize = lastKey - firstKey + 1;
            if (valuesToLocationIndexRange.isEmpty()) {
                // NB: It's impossible for the location to be legitimately empty, since the constructor validates that
                // firstKey <= lastKey.
                throw new IllegalStateException(
                        "Invalid empty grouping for " + columnLocation + ": expected " + locationSize + " rows");
            }

            final Object indicativeValue =
                    valuesToLocationIndexRange.values().stream().findAny().orElseThrow(IllegalStateException::new);
            final RangeAccessor<RANGE_TYPE> rangeAccessor;
            if (indicativeValue.getClass() == int[].class) {
                // noinspection unchecked
                rangeAccessor = (RangeAccessor<RANGE_TYPE>) INT_RANGE_ACCESSOR;
            } else if (indicativeValue.getClass() == long[].class) {
                // noinspection unchecked
                rangeAccessor = (RangeAccessor<RANGE_TYPE>) LONG_RANGE_ACCESSOR;
            } else {
                throw new UnsupportedOperationException("Unexpected range type " + indicativeValue.getClass()
                        + " in grouping metadata for " + columnLocation);
            }

            final List<GroupingItem<DATA_TYPE>> result = new ArrayList<>(valuesToLocationIndexRange.size());
            long totalRangeSize = 0;
            if (SORT_RANGES) {
                final Map<RANGE_TYPE, DATA_TYPE> reversedMap = new TreeMap<>(rangeAccessor);
                for (final Map.Entry<DATA_TYPE, RANGE_TYPE> entry : valuesToLocationIndexRange.entrySet()) {
                    reversedMap.put(entry.getValue(), entry.getKey());
                }
                for (final Map.Entry<RANGE_TYPE, DATA_TYPE> entry : reversedMap.entrySet()) {
                    final long firstPositionInclusive = rangeAccessor.getRangeStartInclusive(entry.getKey());
                    final long lastPositionInclusive = rangeAccessor.getRangeEndInclusive(entry.getKey());
                    result.add(new GroupingItem<>(entry.getValue(), firstPositionInclusive + firstKey,
                            lastPositionInclusive + firstKey));
                    totalRangeSize += lastPositionInclusive - firstPositionInclusive + 1;
                }
            } else {
                for (final Map.Entry<DATA_TYPE, RANGE_TYPE> entry : valuesToLocationIndexRange.entrySet()) {
                    final long firstPositionInclusive = rangeAccessor.getRangeStartInclusive(entry.getValue());
                    final long lastPositionInclusive = rangeAccessor.getRangeEndInclusive(entry.getValue());
                    result.add(new GroupingItem<>(entry.getKey(), firstPositionInclusive + firstKey,
                            lastPositionInclusive + firstKey));
                    totalRangeSize += lastPositionInclusive - firstPositionInclusive + 1;
                }
            }
            if (locationSize != totalRangeSize) {
                throw new IllegalStateException("Invalid grouping for " + columnLocation + ": found " + totalRangeSize
                        + " rows, but expected " + locationSize);
            }
            return result;
        }

        /**
         * Get a list of grouping items that represent the grouping information from this source, or null if grouping
         * information was not present.
         * <p>
         * If the grouping information was already read, get it from the cached results map. Any grouping information
         * read by this method will be stored in the cached results map.
         *
         * @param columnDefinition The definition of this column
         * @return A list of grouping items on success, else null
         */
        private List<GroupingItem<DATA_TYPE>> getTransformedMetadata(
                @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition) {
            final Map<DATA_TYPE, RANGE_TYPE> metadata = loadMetadata(columnDefinition);
            return metadata == null ? null : validateAndTransformMetadata(metadata);
        }
    }

    private static class GroupingItem<DATA_TYPE> {

        private final DATA_TYPE value;
        private final long firstKey;
        private final long lastKey;

        private GroupingItem(final DATA_TYPE value, final long firstKey, final long lastKey) {
            this.value = value;
            this.firstKey = firstKey;
            this.lastKey = lastKey;
        }

        private void updateBuilder(@NotNull final Map<DATA_TYPE, Index.SequentialBuilder> valueToBuilder) {
            valueToBuilder.computeIfAbsent(value, v -> Index.FACTORY.getSequentialBuilder()).appendRange(firstKey,
                    lastKey);
        }
    }

    private final List<Source<DATA_TYPE, ?>> sources = new ArrayList<>();

    @Override
    public void addSource(@NotNull final ColumnLocation columnLocation,
            @NotNull final ReadOnlyIndex locationIndexInTable) {
        final long firstKey = locationIndexInTable.firstKey();
        final long lastKey = locationIndexInTable.lastKey();
        if (lastKey - firstKey + 1 != locationIndexInTable.size()) {
            /*
             * TODO (https://github.com/deephaven/deephaven-core/issues/816): This constraint is valid for all existing
             * formats that support grouping. Address when we integrate grouping/index tables.
             */
            throw new IllegalArgumentException(
                    ParallelDeferredGroupingProvider.class + " only supports a single range per location");
        }
        sources.add(new Source<>(columnLocation, firstKey, lastKey));
    }

    private Map<DATA_TYPE, Index> buildGrouping(@NotNull final List<Source<DATA_TYPE, ?>> includedSources) {
        return QueryPerformanceRecorder.withNugget("Build deferred grouping", () -> {
            // noinspection unchecked
            final List<GroupingItem<DATA_TYPE>>[] perSourceGroupingLists =
                    QueryPerformanceRecorder.withNugget("Read and transform grouping metadata",
                            () -> includedSources.parallelStream()
                                    .map(source -> source.getTransformedMetadata(columnDefinition))
                                    .toArray(List[]::new));

            final Map<DATA_TYPE, Index.SequentialBuilder> valueToBuilder =
                    QueryPerformanceRecorder.withNugget("Integrate grouping metadata", () -> {
                        final Map<DATA_TYPE, Index.SequentialBuilder> result = new LinkedHashMap<>();
                        for (final List<GroupingItem<DATA_TYPE>> groupingList : perSourceGroupingLists) {
                            if (groupingList == null) {
                                return null;
                            }
                            for (final GroupingItem<DATA_TYPE> grouping : groupingList) {
                                grouping.updateBuilder(result);
                            }
                        }
                        return result;
                    });
            if (valueToBuilder == null) {
                return null;
            }

            return QueryPerformanceRecorder.withNugget("Build and aggregate group indexes",
                    () -> valueToBuilder.entrySet().parallelStream()
                            .map(e -> new Pair<>(e.getKey(), e.getValue().getIndex()))
                            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond, Assert::neverInvoked,
                                    LinkedHashMap::new)));
        });
    }

    @Override
    public Map<DATA_TYPE, Index> getGroupToRange() {
        return buildGrouping(sources);
    }

    @Override
    public Pair<Map<DATA_TYPE, Index>, Boolean> getGroupToRange(@NotNull final Index hint) {
        final List<Source<DATA_TYPE, ?>> includedSources = sources.stream()
                .filter(source -> CurrentOnlyIndex.FACTORY.getIndexByRange(source.firstKey, source.lastKey)
                        .overlaps(hint))
                .collect(Collectors.toList());
        return new Pair<>(buildGrouping(includedSources), includedSources.size() == sources.size());
    }
}
