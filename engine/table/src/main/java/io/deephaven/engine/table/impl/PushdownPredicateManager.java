//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public interface PushdownPredicateManager extends PushdownFilterMatcher {
    /**
     * Return the shared pushdown predicate manager for the given column sources, if one exists. Otherwise, return null.
     * 
     * @param columnSources The column sources to check.
     * @return The shared pushdown predicate manager, or null if none exists.
     */
    static PushdownPredicateManager getSharedPPM(Collection<ColumnSource<?>> columnSources) {
        final MutableObject<PushdownPredicateManager> result = new MutableObject<>();
        if (columnSources.stream().allMatch(cs -> {
            if (!(cs instanceof AbstractColumnSource)) {
                return false;
            }
            final PushdownPredicateManager ppm = ((AbstractColumnSource) cs).pushdownManager();
            if (ppm == null)
                return false;
            if (result.getValue() == ppm) {
                return true;
            } else if (result.getValue() == null) {
                result.setValue(ppm);
                return true;
            }
            return false;
        })) {
            return result.getValue();
        }
        return null;
    }

    /**
     * Generate a map of the column names as represented in the filter to the column names as represented in an
     * underlying table.
     *
     * @param columnSources the column sources used by the filter
     * @param filterColumns the names used by the filter (parallel with columnSources)
     * @param columnSourceToName an IdentityHashMap from the column sources we manage to their names (as the sources
     *        know them)
     * @return a map of filter column names to table column names, for those columns which renames are necessary
     */
    static @NotNull Map<String, String> computeRenameMap(List<ColumnSource<?>> columnSources,
            List<String> filterColumns, IdentityHashMap<ColumnSource<?>, String> columnSourceToName) {
        final Map<String, String> renameMap = new HashMap<>();
        for (int ii = 0; ii < filterColumns.size(); ii++) {
            final String filterColumnName = filterColumns.get(ii);
            final ColumnSource<?> filterSource = columnSources.get(ii);
            final String localColumnName = columnSourceToName.get(filterSource);
            if (localColumnName == null) {
                throw new IllegalArgumentException(
                        "No associated source for '" + filterColumnName + "' found in column sources");
            }

            // Add the rename (if needed)
            if (localColumnName.equals(filterColumnName)) {
                continue;
            }
            renameMap.put(filterColumnName, localColumnName);
        }
        return renameMap;
    }
}
