/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.filter.FilterAnd;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.SafeCloseable;

import java.util.*;

import static io.deephaven.engine.table.impl.select.DisjunctiveFilter.orImpl;

public class ConjunctiveFilter extends ComposedFilter {

    private ConjunctiveFilter(WhereFilter[] componentFilters) {
        super(componentFilters);
    }

    public static WhereFilter of(FilterAnd ands) {
        return makeConjunctiveFilter(WhereFilter.from(ands.filters()));
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

    static WritableRowSet andImpl(RowSet selection, RowSet fullSet, Table table, boolean usePrev, boolean invert,
            WhereFilter[] filters) {
        WritableRowSet matched = selection.copy();
        for (WhereFilter filter : filters) {
            if (Thread.interrupted()) {
                throw new CancellationException("interrupted while filtering");
            }
            try (final SafeCloseable ignored = matched) { // Ensure we close old matched
                matched = filter.filter(matched, fullSet, table, usePrev, invert);
            }
        }
        return matched;
    }

    @Override
    public WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        return andImpl(selection, fullSet, table, usePrev, false, componentFilters);
    }

    @Override
    public WritableRowSet filterInverse(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        return orImpl(selection, fullSet, table, usePrev, true, componentFilters);
    }

    @Override
    public ConjunctiveFilter copy() {
        return new ConjunctiveFilter(WhereFilter.copyFrom(getComponentFilters()));
    }

    @Override
    public String toString() {
        return "ConjunctiveFilter(" + Arrays.toString(componentFilters) + ')';
    }
}
