//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.Collection;

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
}
