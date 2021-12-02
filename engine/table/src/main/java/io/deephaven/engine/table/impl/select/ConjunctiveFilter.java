/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.SafeCloseable;

import java.util.*;

public class ConjunctiveFilter extends ComposedFilter {

    private ConjunctiveFilter(WhereFilter[] componentFilters) {
        super(componentFilters);
    }

    public static WhereFilter of(WhereFilter... filters) {
        return makeConjunctiveFilter(filters);
    }

    public static WhereFilter makeConjunctiveFilter(WhereFilter... componentFilters) {
        if (componentFilters.length == 1)
            return componentFilters[0];

        final List<WhereFilter> rawComponents = new ArrayList<>();
        for (int ii = 0; ii < componentFilters.length; ++ii) {
            if (componentFilters[ii] instanceof ConjunctiveFilter) {
                rawComponents.addAll(Arrays.asList(((ConjunctiveFilter) componentFilters[ii]).getComponentFilters()));
            } else {
                rawComponents.add(componentFilters[ii]);
            }
        }

        return new ConjunctiveFilter(rawComponents.toArray(WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY));
    }

    @Override
    public WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        WritableRowSet matched = selection.copy();

        for (WhereFilter filter : componentFilters) {
            if (Thread.interrupted()) {
                throw new CancellationException("interrupted while filtering");
            }

            try (final SafeCloseable ignored = matched) { // Ensure we close old matched
                matched = filter.filter(matched, fullSet, table, usePrev);
            }
        }

        return matched;
    }

    @Override
    public ConjunctiveFilter copy() {
        return new ConjunctiveFilter(
                Arrays.stream(getComponentFilters()).map(WhereFilter::copy).toArray(WhereFilter[]::new));
    }

    @Override
    public String toString() {
        return "ConjunctiveFilter(" + Arrays.toString(componentFilters) + ')';
    }
}
