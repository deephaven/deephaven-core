/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.util.*;

/**
 * Grouping provider that loads column location metadata and assembles grouping indexes lazily on demand.
 */
class DeferredLegacyMetadataGroupingProvider<DATA_TYPE> implements KeyRangeGroupingProvider<DATA_TYPE> {

    private static final boolean SORT_RANGES = false;
    private final ColumnDefinition columnDefinition;

    DeferredLegacyMetadataGroupingProvider(ColumnDefinition columnDefinition) {
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

    private class Source<RANGE_TYPE> {

        private final ColumnLocation columnLocation;
        private final long firstKey;
        private final long lastKey;

        /**
         * We a reference to the resultant grouping information.  This allows us to avoid re-reading grouping information.
         *
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
         * Update the grouping builder with the grouping information from this source.
         *
         * If the grouping information was already read, get it from the cached results map.  Any grouping
         * information read by this method will be stored in the cached results map.
         *
         * @param builder the grouping builder to add DATA_TYPE -> index entries to
         * @return true on success, false if there was no grouping information present in this column to add.
         */
        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        private boolean updateGroupingBuilder(RandomGroupingBuilder<DATA_TYPE> builder) {
            if(!columnLocation.exists()) {
                builder.addGrouping(null, firstKey, lastKey);
                return true;
            }

            Map<DATA_TYPE, RANGE_TYPE> valuesToLocationIndexRange = null;
            if (cachedResult != null) {
                valuesToLocationIndexRange = cachedResult.get();
            }

            if (valuesToLocationIndexRange == null) {
                //noinspection unchecked
                if ((valuesToLocationIndexRange = (Map<DATA_TYPE, RANGE_TYPE>)columnLocation.getMetadata(columnDefinition)) == null) {
                    return false;
                }
                cachedResult = new SoftReference<>(valuesToLocationIndexRange);
            }

            if (valuesToLocationIndexRange.isEmpty()) {
                return true;
            }

            final Object indicativeValue = valuesToLocationIndexRange.values().stream().findAny().orElseThrow(IllegalStateException::new);
            final RangeAccessor<RANGE_TYPE> rangeAccessor;
            if  (indicativeValue.getClass() == int[].class) {
                //noinspection unchecked
                rangeAccessor = (RangeAccessor<RANGE_TYPE>)INT_RANGE_ACCESSOR;
            } else if (indicativeValue.getClass() == long[].class) {
                //noinspection unchecked
                rangeAccessor = (RangeAccessor<RANGE_TYPE>)LONG_RANGE_ACCESSOR;
            } else {
                throw new UnsupportedOperationException("Unexpected range type " + indicativeValue + " in grouping metadata for " + columnLocation);
            }
            if(SORT_RANGES) {
                Map<RANGE_TYPE, DATA_TYPE> reversedMap = new TreeMap<>(rangeAccessor);
                for(Map.Entry<DATA_TYPE, RANGE_TYPE> entry : valuesToLocationIndexRange.entrySet()) {
                    reversedMap.put(entry.getValue(), entry.getKey());
                }
                for(Map.Entry<RANGE_TYPE, DATA_TYPE> entry : reversedMap.entrySet()) {
                    builder.addGrouping(entry.getValue(),
                            rangeAccessor.getRangeStartInclusive(entry.getKey()) + firstKey,
                            rangeAccessor.getRangeEndInclusive(entry.getKey()) + firstKey);
                }
            } else {
                for(Map.Entry<DATA_TYPE, RANGE_TYPE> entry : valuesToLocationIndexRange.entrySet()) {
                    builder.addGrouping(entry.getKey(),
                            rangeAccessor.getRangeStartInclusive(entry.getValue()) + firstKey,
                            rangeAccessor.getRangeEndInclusive(entry.getValue()) + firstKey);
                }
            }
            return true;
        }
    }

    private final List<Source> sources = new ArrayList<>();

    @Override
    public void addSource(@NotNull final ColumnLocation columnLocation, final long firstKey, final long lastKey) {
        sources.add(new Source(columnLocation, firstKey, lastKey));
    }

    @Override
    public Map<DATA_TYPE, Index> getGroupToRange() {
        RandomGroupingBuilder<DATA_TYPE> builder = new RandomGroupingBuilder<>();
        for(Source source : sources) {
            //noinspection unchecked
            if(!source.updateGroupingBuilder(builder)) {
                return null;
            }
        }
        return builder.getGroupToIndex();
    }

    @Override
    public Pair<Map<DATA_TYPE, Index>, Boolean> getGroupToRange(Index hint) {
        RandomGroupingBuilder<DATA_TYPE> builder = new RandomGroupingBuilder<>();

        int includedSources = 0;

        for(Source source : sources) {
            if (Index.FACTORY.getIndexByRange(source.firstKey, source.lastKey).overlaps(hint)) {
                includedSources++;
                //noinspection unchecked
                if (!source.updateGroupingBuilder(builder)) {
                    return null;
                }
            }
        }

        return new Pair<>(builder.getGroupToIndex(), includedSources == sources.size());
    }
}
