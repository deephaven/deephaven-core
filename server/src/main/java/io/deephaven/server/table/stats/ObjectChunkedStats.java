//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharacterChunkedStats and run "./gradlew replicateColumnStats" to regenerate
//
// @formatter:off
package io.deephaven.server.table.stats;

import io.deephaven.util.type.ArrayTypeUtils;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.table.iterators.ObjectColumnIterator;
import io.deephaven.engine.table.iterators.ChunkedObjectColumnIterator;
import io.deephaven.engine.util.TableTools;
import it.unimi.dsi.fastutil.objects.Object2LongMaps;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ObjectChunkedStats implements ChunkedStatsKernel {
    private final int maxUniqueToCollect;
    private final int maxUniqueToDisplay;

    public ObjectChunkedStats(int maxUniqueToCollect, int maxUniqueToDisplay) {
        this.maxUniqueToCollect = maxUniqueToCollect;
        this.maxUniqueToDisplay = maxUniqueToDisplay;
    }

    @Override
    public Table processChunks(final RowSet rowSet, final ColumnSource<?> columnSource, boolean usePrev) {
        long count = 0;
        int uniqueCount = 0;

        final Object2LongOpenHashMap<Object> countValues = new Object2LongOpenHashMap<>();
        boolean useSet = false;
        final ObjectOpenHashSet<Object> uniqueValues = new ObjectOpenHashSet<>();

        try (ObjectColumnIterator<Object> iterator =
                new ChunkedObjectColumnIterator<>(usePrev ? columnSource.getPrevSource() : columnSource, rowSet)) {
            while (iterator.hasNext()) {
                Object val = iterator.next();
                if (val == null) {
                    continue;
                }
                count++;

                // Object2LongOpenHashMap.addTo returns the previous value (defaultReturnValue() == 0 if absent).
                // Counts start at 1 and only grow, so a 0 return means the key was absent on this call.
                if (countValues.addTo(val, 1) == 0 && ++uniqueCount > maxUniqueToCollect) {
                    // we no longer want to track counts for these items; fall back to a Set to get at least a count
                    uniqueValues.addAll(countValues.keySet());
                    countValues.clear();
                    useSet = true;
                    break;
                }
            }
            while (iterator.hasNext()) {
                // items still remain, count non-nulls and uniques
                Object val = iterator.next();
                if (val == null) {
                    continue;
                }
                count++;

                uniqueValues.add(val);
            }
        }


        if (useSet) {
            return TableTools.newTable(
                    TableTools.longCol("COUNT", count),
                    TableTools.longCol("SIZE", rowSet.size()),
                    TableTools.intCol("UNIQUE_VALUES", uniqueValues.size()));
        }
        List<Map.Entry<String, Long>> sorted = new ArrayList<>(countValues.size());

        // region add_entries
        if (columnSource.getType().isArray()) {
            Object2LongMaps.fastForEach(countValues, entry -> sorted
                    .add(Map.entry(ArrayTypeUtils.toString(entry.getKey()), entry.getLongValue())));
        } else {
            Object2LongMaps.fastForEach(countValues, entry -> sorted
                    .add(Map.entry(Objects.toString(entry.getKey()), entry.getLongValue())));
        }
        // endregion add_entries
        sorted.sort(Map.Entry.<String, Long>comparingByValue().reversed());

        int resultCount = Math.min(maxUniqueToDisplay, sorted.size());
        String[] uniqueKeys = new String[resultCount];
        long[] uniqueCounts = new long[resultCount];
        Iterator<Map.Entry<String, Long>> iter = sorted.iterator();
        for (int i = 0; i < resultCount; i++) {
            Map.Entry<String, Long> entry = iter.next();
            uniqueKeys[i] = entry.getKey();
            uniqueCounts[i] = entry.getValue();
        }
        return TableTools.newTable(
                TableTools.longCol("COUNT", count),
                TableTools.longCol("SIZE", rowSet.size()),
                TableTools.intCol("UNIQUE_VALUES", countValues.size()),
                new ColumnHolder<>("UNIQUE_KEYS", String[].class, String.class, false, uniqueKeys),
                new ColumnHolder<>("UNIQUE_COUNTS", long[].class, long.class, false, uniqueCounts));
    }
}
