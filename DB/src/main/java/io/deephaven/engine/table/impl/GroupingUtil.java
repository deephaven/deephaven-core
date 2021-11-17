package io.deephaven.engine.table.impl;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.GroupingRowSetHelper;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.v2.sources.ArrayBackedColumnSource;
import io.deephaven.engine.v2.sources.ObjectArraySource;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.deephaven.engine.v2.sources.ArrayBackedColumnSource.getMemoryColumnSource;

/**
 * Utilities for creating or interacting with grouping information.
 */
public class GroupingUtil {

    /**
     * Get a map from unique, boxed values in this column to a long[2] range of keys.
     *
     * @param rowSet The rowSet that defines the column along with the column source
     * @param columnSource The column source that defines the column along with the rowSet
     * @return A new value to range map (i.e. grouping metadata)
     */
    public static <TYPE> Map<TYPE, long[]> getValueToRangeMap(@NotNull final TrackingRowSet rowSet,
            @Nullable final ColumnSource<TYPE> columnSource) {
        final long size = rowSet.size();
        if (columnSource == null) {
            return Collections.singletonMap(null, new long[] {0, size});
        }
        // noinspection unchecked
        return ((Map<TYPE, RowSet>) rowSet.getGrouping(columnSource)).entrySet().stream()
                .sorted(java.util.Comparator.comparingLong(e -> e.getValue().firstRowKey())).collect(Collectors.toMap(
                        Map.Entry::getKey,
                        new Function<>() {
                            private long prevLastKey = -1L;
                            private long currentSize = 0;

                            @Override
                            public long[] apply(@NotNull final Map.Entry<TYPE, RowSet> entry) {
                                final RowSet rowSet = entry.getValue();
                                Assert.instanceOf(rowSet, "rowSet", GroupingRowSetHelper.class);
                                Assert.gt(rowSet.firstRowKey(), "rowSet.firstRowKey()", prevLastKey, "prevLastKey");
                                prevLastKey = rowSet.lastRowKey();
                                return new long[]{currentSize, currentSize += rowSet.size()};
                            }
                        },
                        Assert::neverInvoked,
                        LinkedHashMap::new));
    }

    /**
     * Consume all groups in a group-to-rowSet map.
     *
     * @param groupToIndex The group-to-rowSet map to consume
     * @param groupConsumer Consumer for responsive groups
     */
    public static <TYPE> void forEachGroup(@NotNull final Map<TYPE, RowSet> groupToIndex,
            @NotNull final BiConsumer<TYPE, WritableRowSet> groupConsumer) {
        groupToIndex.entrySet().stream()
                .filter(kie -> kie.getValue().isNonempty())
                .sorted(java.util.Comparator.comparingLong(kie -> kie.getValue().firstRowKey()))
                .forEachOrdered(kie -> groupConsumer.accept(kie.getKey(), kie.getValue().copy()));
    }

    /**
     * Convert a group-to-rowSet map to a pair of flat in-memory column sources, one for the keys and one for the
     * indexes.
     *
     * @param originalKeyColumnSource The key column source whose contents are reflected by the group-to-rowSet map
     *        (used for typing, only)
     * @param groupToIndex The group-to-rowSet map to convert
     * @return A pair of a flat key column source and a flat rowSet column source
     */
    @SuppressWarnings("unused")
    public static <TYPE> Pair<ArrayBackedColumnSource<TYPE>, ObjectArraySource<TrackingWritableRowSet>> groupingToFlatSources(
            @NotNull final ColumnSource<TYPE> originalKeyColumnSource, @NotNull final Map<TYPE, RowSet> groupToIndex) {
        final int numGroups = groupToIndex.size();
        final ArrayBackedColumnSource<TYPE> resultKeyColumnSource = getMemoryColumnSource(
                numGroups, originalKeyColumnSource.getType(), originalKeyColumnSource.getComponentType());
        final ObjectArraySource<TrackingWritableRowSet> resultIndexColumnSource =
                new ObjectArraySource<>(TrackingWritableRowSet.class);
        resultIndexColumnSource.ensureCapacity(numGroups);

        final MutableInt processedGroupCount = new MutableInt(0);
        forEachGroup(groupToIndex, (final TYPE key, final WritableRowSet rowSet) -> {
            final long groupIndex = processedGroupCount.longValue();
            resultKeyColumnSource.set(groupIndex, key);
            resultIndexColumnSource.set(groupIndex, rowSet.toTracking());
            processedGroupCount.increment();
        });
        Assert.eq(processedGroupCount.intValue(), "processedGroupCount.intValue()", numGroups, "numGroups");
        return new Pair<>(resultKeyColumnSource, resultIndexColumnSource);
    }

    /**
     * Consume all responsive groups in a group-to-rowSet map.
     *
     * @param groupToIndex The group-to-rowSet map to consume
     * @param intersect Limit indices to values contained within intersect, eliminating empty result groups
     * @param groupConsumer Consumer for responsive groups
     */
    public static <TYPE> void forEachResponsiveGroup(@NotNull final Map<TYPE, RowSet> groupToIndex,
            @NotNull final RowSet intersect,
            @NotNull final BiConsumer<TYPE, WritableRowSet> groupConsumer) {
        groupToIndex.entrySet().stream()
                .map(kie -> new Pair<>(kie.getKey(), kie.getValue().intersect(intersect)))
                .filter(kip -> kip.getSecond().isNonempty())
                .sorted(java.util.Comparator.comparingLong(kip -> kip.getSecond().firstRowKey()))
                .forEachOrdered(kip -> groupConsumer.accept(kip.getFirst(), kip.getSecond().copy()));
    }

    /**
     * Convert a group-to-rowSet map to a pair of flat in-memory column sources, one for the keys and one for the
     * indexes.
     *
     * @param originalKeyColumnSource The key column source whose contents are reflected by the group-to-rowSet map
     *        (used for typing, only)
     * @param groupToIndex The group-to-rowSet map to convert
     * @param intersect Limit returned indices to values contained within intersect
     * @param responsiveGroups Set to the number of responsive groups on exit
     * @return A pair of a flat key column source and a flat rowSet column source
     */
    public static <TYPE> Pair<ArrayBackedColumnSource<TYPE>, ObjectArraySource<TrackingWritableRowSet>> groupingToFlatSources(
            @NotNull final ColumnSource<TYPE> originalKeyColumnSource,
            @NotNull final Map<TYPE, RowSet> groupToIndex,
            @NotNull final RowSet intersect,
            @NotNull final MutableInt responsiveGroups) {
        final int numGroups = groupToIndex.size();
        final ArrayBackedColumnSource<TYPE> resultKeyColumnSource = getMemoryColumnSource(
                numGroups, originalKeyColumnSource.getType(), originalKeyColumnSource.getComponentType());
        final ObjectArraySource<TrackingWritableRowSet> resultIndexColumnSource =
                new ObjectArraySource<>(TrackingWritableRowSet.class);
        resultIndexColumnSource.ensureCapacity(numGroups);

        responsiveGroups.setValue(0);
        forEachResponsiveGroup(groupToIndex, intersect, (final TYPE key, final WritableRowSet rowSet) -> {
            final long groupIndex = responsiveGroups.longValue();
            resultKeyColumnSource.set(groupIndex, key);
            resultIndexColumnSource.set(groupIndex, rowSet.toTracking());
            responsiveGroups.increment();
        });

        return new Pair<>(resultKeyColumnSource, resultIndexColumnSource);
    }
}
