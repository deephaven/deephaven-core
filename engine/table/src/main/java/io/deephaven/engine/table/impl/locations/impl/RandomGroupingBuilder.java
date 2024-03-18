//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builder for constructing groupings from one or more {@code <value, key range>} pairs, with no requirement that key
 * ranges be sequential.
 */
public class RandomGroupingBuilder<DATA_TYPE> {

    private Map<DATA_TYPE, RowSetBuilderRandom> groupToIndexBuilder = new LinkedHashMap<>();

    private Map<DATA_TYPE, RowSet> groupToIndex;

    /**
     * Add a mapping from value [firstRowKey, lastRowKey] to the groupings under construction.
     *
     * @param value The value for the grouping
     * @param firstKey The first key in the range
     * @param lastKey The last key in the range
     */
    public void addToGrouping(@Nullable DATA_TYPE value, long firstKey, long lastKey) {
        // if we've already created the groupToIndex, then our groupToIndexBuilder is going to be in a bad state
        Require.eqNull(groupToIndex, "groupToIndex");
        Require.neqNull(groupToIndexBuilder, "groupToIndexBuilder");

        final RowSetBuilderRandom indexBuilder =
                groupToIndexBuilder.computeIfAbsent(value, (k) -> RowSetFactory.builderRandom());
        indexBuilder.addRange(firstKey, lastKey);
    }

    /**
     * Get the groupings under construction in a form usable by AbstractColumnSource implementations.
     *
     * @return A mapping from grouping value to its matching RowSet
     */
    public Map<DATA_TYPE, RowSet> getGrouping() {
        if (groupToIndex != null) {
            return groupToIndex;
        }
        groupToIndex = new LinkedHashMap<>(groupToIndexBuilder.size() * 4 / 3 + 1);
        for (Map.Entry<DATA_TYPE, RowSetBuilderRandom> entry : groupToIndexBuilder.entrySet()) {
            groupToIndex.put(entry.getKey(), entry.getValue().build());
        }
        groupToIndexBuilder = null;
        return groupToIndex;
    }
}
