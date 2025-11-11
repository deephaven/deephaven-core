//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.impl.select.*;

/**
 * Extract a simple filter from a {@link WhereFilter}, walking the tree and removing all barrier and serial wrappers.
 */
public enum ExtractFilterWithoutBarriers implements WhereFilter.Visitor<WhereFilter> {
    EXTRACT_FILTER_WITHOUT_BARRIERS;

    public static WhereFilter of(final WhereFilter filter) {
        return filter.walk(EXTRACT_FILTER_WITHOUT_BARRIERS);
    }

    @Override
    public WhereFilter visitOther(final WhereFilter filter) {
        return filter;
    }

    @Override
    public WhereFilter visit(final WhereFilterInvertedImpl filter) {
        return WhereFilterInvertedImpl.of(of(filter.getWrappedFilter())); // must unwrap, then re-wrap inverted
    }

    @Override
    public WhereFilter visit(final WhereFilterSerialImpl filter) {
        return of(filter.getWrappedFilter()); // must unwrap
    }

    @Override
    public WhereFilter visit(final WhereFilterWithDeclaredBarriersImpl filter) {
        return of(filter.getWrappedFilter()); // must unwrap
    }

    @Override
    public WhereFilter visit(final WhereFilterWithRespectedBarriersImpl filter) {
        return of(filter.getWrappedFilter()); // must unwrap
    }

    @Override
    public WhereFilter visit(final DisjunctiveFilter filter) {
        final WhereFilter[] innerUnwrapped = filter.getFilters().stream()
                .map(ExtractFilterWithoutBarriers::of)
                .toArray(WhereFilter[]::new);

        // Verify we have exactly the same number of filters after unwrapping.
        Assert.eq(innerUnwrapped.length, "innerUnwrapped.length", filter.getFilters().size(),
                "filter.getFilters().size()");

        // return a single DisjunctiveFilter containing all the unwrapped inner filters.
        return DisjunctiveFilter.of(innerUnwrapped);
    }

    @Override
    public WhereFilter visit(final ConjunctiveFilter filter) {
        final WhereFilter[] innerUnwrapped = filter.getFilters().stream()
                .map(ExtractFilterWithoutBarriers::of)
                .toArray(WhereFilter[]::new);

        // Verify we have exactly the same number of filters after unwrapping.
        Assert.eq(innerUnwrapped.length, "innerUnwrapped.length", filter.getFilters().size(),
                "filter.getFilters().size()");

        // return a single ConjunctiveFilter containing all the unwrapped inner filters.
        return ConjunctiveFilter.of(innerUnwrapped);
    }
}
