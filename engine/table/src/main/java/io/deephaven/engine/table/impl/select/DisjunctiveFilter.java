//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.filter.FilterOr;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.*;

import static io.deephaven.engine.table.impl.select.ConjunctiveFilter.andImpl;

public class DisjunctiveFilter extends ComposedFilter {
    private DisjunctiveFilter(WhereFilter[] componentFilters) {
        super(componentFilters);
    }

    public static WhereFilter of(FilterOr ors) {
        return DisjunctiveFilter.makeDisjunctiveFilter(WhereFilter.from(ors.filters()));
    }

    public static WhereFilter makeDisjunctiveFilter(WhereFilter... componentFilters) {
        if (componentFilters.length == 1) {
            return componentFilters[0];
        }

        final List<WhereFilter> rawComponents = new ArrayList<>();
        for (int ii = 0; ii < componentFilters.length; ++ii) {
            if (componentFilters[ii] instanceof DisjunctiveFilter) {
                rawComponents.addAll(Arrays.asList(((DisjunctiveFilter) componentFilters[ii]).getComponentFilters()));
            } else {
                rawComponents.add(componentFilters[ii]);
            }
        }

        return new DisjunctiveFilter(rawComponents.toArray(WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY));
    }

    static WritableRowSet orImpl(RowSet selection, RowSet fullSet, Table table, boolean usePrev, boolean invert,
            WhereFilter[] filters) {
        WritableRowSet matched = null;
        try (WritableRowSet remaining = selection.copy()) {
            for (WhereFilter filter : filters) {
                if (Thread.interrupted()) {
                    throw new CancellationException("interrupted while filtering");
                }
                // If a previous clause has already matched a row, we do not need to re-evaluate it
                if (matched != null) {
                    remaining.remove(matched);
                }
                final WritableRowSet filterMatched = filter.filter(remaining, fullSet, table, usePrev, invert);
                // All matched entries get put into the value
                if (matched == null) {
                    matched = filterMatched;
                } else {
                    try (final SafeCloseable ignored = filterMatched) {
                        matched.insert(filterMatched);
                    }
                }
                if (matched.size() == selection.size()) {
                    // Everything in the input set already belongs in the output set
                    break;
                }
            }
        }
        return matched == null ? selection.copy() : matched.copy();
    }

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        return orImpl(selection, fullSet, table, usePrev, false, componentFilters);
    }

    @NotNull
    @Override
    public WritableRowSet filterInverse(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        return andImpl(selection, fullSet, table, usePrev, true, componentFilters);
    }

    @Override
    public DisjunctiveFilter copy() {
        return new DisjunctiveFilter(WhereFilter.copyFrom(getComponentFilters()));
    }

    @Override
    public String toString() {
        return "DisjunctiveFilter(" + Arrays.toString(componentFilters) + ')';
    }
}
