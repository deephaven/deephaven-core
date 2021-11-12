/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.select;

import io.deephaven.engine.exceptions.QueryCancellationException;
import io.deephaven.engine.tables.Table;
import io.deephaven.engine.rowset.MutableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.SafeCloseable;

import java.util.*;

public class ConjunctiveFilter extends ComposedFilter {

    private ConjunctiveFilter(SelectFilter[] componentFilters) {
        super(componentFilters);
    }

    public static SelectFilter of(SelectFilter... filters) {
        return makeConjunctiveFilter(filters);
    }

    public static SelectFilter makeConjunctiveFilter(SelectFilter... componentFilters) {
        if (componentFilters.length == 1)
            return componentFilters[0];

        final List<SelectFilter> rawComponents = new ArrayList<>();
        for (int ii = 0; ii < componentFilters.length; ++ii) {
            if (componentFilters[ii] instanceof ConjunctiveFilter) {
                rawComponents.addAll(Arrays.asList(((ConjunctiveFilter) componentFilters[ii]).getComponentFilters()));
            } else {
                rawComponents.add(componentFilters[ii]);
            }
        }

        return new ConjunctiveFilter(rawComponents.toArray(SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY));
    }

    @Override
    public MutableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        MutableRowSet matched = selection.copy();

        for (SelectFilter filter : componentFilters) {
            if (Thread.interrupted()) {
                throw new QueryCancellationException("interrupted while filtering");
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
                Arrays.stream(getComponentFilters()).map(SelectFilter::copy).toArray(SelectFilter[]::new));
    }

    @Override
    public String toString() {
        return "ConjunctiveFilter(" + Arrays.toString(componentFilters) + ')';
    }
}
