/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.indexer.RowSetIndexer;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource.getMemoryColumnSource;

/**
 * Utilities for creating or interacting with grouping information.
 */
public class GroupingUtils {

    /**
     * Get a map from unique, boxed values in this column to a long[2] range of keys.
     *
     * @param rowSet The RowSet that defines the column along with the column source
     * @param columnSource The column source that defines the column along with the RowSet
     * @return A new value to range map (i.e. grouping metadata)
     */
    public static <TYPE> Map<TYPE, long[]> getValueToRangeMap(@NotNull final TrackingRowSet rowSet,
            @Nullable final ColumnSource<TYPE> columnSource) {
        final long size = rowSet.size();
        if (columnSource == null) {
            return Collections.singletonMap(null, new long[] {0, size});
        }
        // noinspection unchecked
        return ((Map<TYPE, RowSet>) RowSetIndexer.of(rowSet).getGrouping(columnSource)).entrySet().stream()
                .sorted(java.util.Comparator.comparingLong(e -> e.getValue().firstRowKey())).collect(Collectors.toMap(
                        Map.Entry::getKey,
                        new Function<>() {
                            private long prevLastKey = -1L;
                            private long currentSize = 0;

                            @Override
                            public long[] apply(@NotNull final Map.Entry<TYPE, RowSet> entry) {
                                final RowSet rowSet = entry.getValue();
                                Assert.gt(rowSet.firstRowKey(), "rowSet.firstRowKey()", prevLastKey, "prevLastKey");
                                prevLastKey = rowSet.lastRowKey();
                                return new long[] {currentSize, currentSize += rowSet.size()};
                            }
                        },
                        Assert::neverInvoked,
                        LinkedHashMap::new));
    }

    /**
     * Consume all groups in a group-to-RowSet map.
     *
     * @param groupToRowSet The group-to-RowSet map to consume
     * @param groupConsumer Consumer for responsive groups
     */
    public static <TYPE> void forEachGroup(@NotNull final Map<TYPE, RowSet> groupToRowSet,
            @NotNull final BiConsumer<TYPE, WritableRowSet> groupConsumer) {
        groupToRowSet.entrySet().stream()
                .filter(kie -> kie.getValue().isNonempty())
                .sorted(java.util.Comparator.comparingLong(kie -> kie.getValue().firstRowKey()))
                .forEachOrdered(kie -> groupConsumer.accept(kie.getKey(), kie.getValue().copy()));
    }

    /**
     * Convert a group-to-RowSet map to a pair of flat in-memory column sources, one for the keys and one for the
     * indexes.
     *
     * @param originalKeyColumnSource The key column source whose contents are reflected by the group-to-RowSet map
     *        (used for typing, only)
     * @param groupToRowSet The group-to-RowSet map to convert
     * @return A pair of a flat key column source and a flat RowSet column source
     */
    @SuppressWarnings("unused")
    public static <TYPE> Pair<ArrayBackedColumnSource<TYPE>, ObjectArraySource<TrackingWritableRowSet>> groupingToFlatSources(
            @NotNull final ColumnSource<TYPE> originalKeyColumnSource, @NotNull final Map<TYPE, RowSet> groupToRowSet) {
        final int numGroups = groupToRowSet.size();
        final ArrayBackedColumnSource<TYPE> resultKeyColumnSource = getMemoryColumnSource(
                numGroups, originalKeyColumnSource.getType(), originalKeyColumnSource.getComponentType());
        final ObjectArraySource<TrackingWritableRowSet> resultIndexColumnSource =
                new ObjectArraySource<>(TrackingWritableRowSet.class);
        resultIndexColumnSource.ensureCapacity(numGroups);

        final MutableInt processedGroupCount = new MutableInt(0);
        forEachGroup(groupToRowSet, (final TYPE key, final WritableRowSet rowSet) -> {
            final long groupIndex = processedGroupCount.longValue();
            resultKeyColumnSource.set(groupIndex, key);
            resultIndexColumnSource.set(groupIndex, rowSet.toTracking());
            processedGroupCount.increment();
        });
        Assert.eq(processedGroupCount.intValue(), "processedGroupCount.intValue()", numGroups, "numGroups");
        return new Pair<>(resultKeyColumnSource, resultIndexColumnSource);
    }

    /**
     * Consume all responsive groups in a group-to-RowSet map.
     *
     * @param groupToRowSet The group-to-RowSet map to consume
     * @param intersect Limit indices to values contained within intersect, eliminating empty result groups
     * @param groupConsumer Consumer for responsive groups
     */
    public static <TYPE> void forEachResponsiveGroup(@NotNull final Map<TYPE, RowSet> groupToRowSet,
            @NotNull final RowSet intersect,
            @NotNull final BiConsumer<TYPE, WritableRowSet> groupConsumer) {
        groupToRowSet.entrySet().stream()
                .map(kie -> new Pair<>(kie.getKey(), kie.getValue().intersect(intersect)))
                .filter(kip -> kip.getSecond().isNonempty())
                .sorted(java.util.Comparator.comparingLong(kip -> kip.getSecond().firstRowKey()))
                .forEachOrdered(kip -> groupConsumer.accept(kip.getFirst(), kip.getSecond().copy()));
    }

    /**
     * Convert a group-to-RowSet map to a pair of flat in-memory column sources, one for the keys and one for the
     * indexes.
     *
     * @param originalKeyColumnSource The key column source whose contents are reflected by the group-to-RowSet map
     *        (used for typing, only)
     * @param groupToRowSet The group-to-RowSet map to convert
     * @param intersect Limit returned indices to values contained within intersect
     * @param responsiveGroups Set to the number of responsive groups on exit
     * @return A pair of a flat key column source and a flat rowSet column source
     */
    public static <TYPE> Pair<ArrayBackedColumnSource<TYPE>, ObjectArraySource<TrackingWritableRowSet>> groupingToFlatSources(
            @NotNull final ColumnSource<TYPE> originalKeyColumnSource,
            @NotNull final Map<TYPE, RowSet> groupToRowSet,
            @NotNull final RowSet intersect,
            @NotNull final MutableInt responsiveGroups) {
        final int numGroups = groupToRowSet.size();
        final ArrayBackedColumnSource<TYPE> resultKeyColumnSource = getMemoryColumnSource(
                numGroups, originalKeyColumnSource.getType(), originalKeyColumnSource.getComponentType());
        final ObjectArraySource<TrackingWritableRowSet> resultIndexColumnSource =
                new ObjectArraySource<>(TrackingWritableRowSet.class);
        resultIndexColumnSource.ensureCapacity(numGroups);

        responsiveGroups.setValue(0);
        forEachResponsiveGroup(groupToRowSet, intersect, (final TYPE key, final WritableRowSet rowSet) -> {
            final long groupIndex = responsiveGroups.longValue();
            resultKeyColumnSource.set(groupIndex, key);
            resultIndexColumnSource.set(groupIndex, rowSet.toTracking());
            responsiveGroups.increment();
        });

        return new Pair<>(resultKeyColumnSource, resultIndexColumnSource);
    }

    /**
     * Convert a group-to-RowSet map to a flat, immutable, in-memory column of keys.
     *
     * @param originalKeyColumnSource The key column source whose contents are reflected by the group-to-RowSet map
     *        (used for typing, only)
     * @param groupToRowSet The group-to-RowSet map to convert
     * @return A flat, immutable, in-memory column of keys
     */
    public static <TYPE> WritableColumnSource<TYPE> groupingKeysToImmutableFlatSource(
            @NotNull final ColumnSource<TYPE> originalKeyColumnSource,
            @NotNull final Map<TYPE, RowSet> groupToRowSet) {
        final WritableColumnSource<TYPE> destination = InMemoryColumnSource.makeImmutableSource(
                originalKeyColumnSource.getType(), originalKeyColumnSource.getComponentType());
        destination.ensureCapacity(groupToRowSet.size());
        int ri = 0;
        for (final TYPE key : groupToRowSet.keySet()) {
            destination.set(ri++, key);
        }
        return destination;
    }
}
