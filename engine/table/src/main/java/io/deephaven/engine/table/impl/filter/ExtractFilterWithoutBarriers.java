//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.impl.select.*;

/**
 * Extract a simple filter from a {@link WhereFilter}, walking the tree and removing all barrier and serial wrappers.
 */
public class ExtractFilterWithoutBarriers implements WhereFilter.Visitor<WhereFilter> {
    public static final ExtractFilterWithoutBarriers INSTANCE = new ExtractFilterWithoutBarriers();

    public static WhereFilter of(final WhereFilter filter) {
        return filter.walkWhereFilter(INSTANCE);
    }

    @Override
    public WhereFilter visitWhereFilter(final WhereFilter filter) {
        final WhereFilter retValue = WhereFilter.Visitor.super.visitWhereFilter(filter);
        if (retValue == null) {
            return filter;
        }
        return retValue;
    }

    @Override
    public WhereFilter visitWhereFilter(final WhereFilterInvertedImpl filter) {
        return WhereFilterInvertedImpl.of(of(filter.getWrappedFilter())); // must unwrap, then re-wrap inverted
    }

    @Override
    public WhereFilter visitWhereFilter(final WhereFilterSerialImpl filter) {
        return of(filter.getWrappedFilter()); // must unwrap
    }

    @Override
    public WhereFilter visitWhereFilter(final WhereFilterWithDeclaredBarriersImpl filter) {
        return of(filter.getWrappedFilter()); // must unwrap
    }

    @Override
    public WhereFilter visitWhereFilter(final WhereFilterWithRespectedBarriersImpl filter) {
        return of(filter.getWrappedFilter()); // must unwrap
    }

    @Override
    public WhereFilter visitWhereFilter(final DisjunctiveFilter filter) {
        final WhereFilter[] innerUnwrapped = filter.getFilters().stream()
                .map(this::visitWhereFilter)
                .toArray(WhereFilter[]::new);

        // Verify we have exactly the same number of filters after unwrapping.
        Assert.eq(innerUnwrapped.length, "innerUnwrapped.length", filter.getFilters().size(),
                "filter.getFilters()..size()");

        // return a single DisjunctiveFilter containing all the unwrapped inner filters.
        return DisjunctiveFilter.of(innerUnwrapped);
    }

    @Override
    public WhereFilter visitWhereFilter(final ConjunctiveFilter filter) {
        final WhereFilter[] innerUnwrapped = filter.getFilters().stream()
                .map(this::visitWhereFilter)
                .toArray(WhereFilter[]::new);

        // Verify we have exactly the same number of filters after unwrapping.
        Assert.eq(innerUnwrapped.length, "innerUnwrapped.length", filter.getFilters().size(),
                "filter.getFilters()..size()");

        // return a single ConjunctiveFilter containing all the unwrapped inner filters.
        return ConjunctiveFilter.of(innerUnwrapped);
    }
}
