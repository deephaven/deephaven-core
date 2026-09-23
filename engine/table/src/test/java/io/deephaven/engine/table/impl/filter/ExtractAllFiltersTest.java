//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.api.RawString;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterDelegating;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.testutil.filters.RowSetCapturingFilter;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ExtractAllFilters}.
 */
public class ExtractAllFiltersTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static WhereFilter wrapped(final WhereFilter filter) {
        assertTrue("sanity: " + filter.getClass() + " is a wrapper", filter instanceof WhereFilterDelegating);
        return ((WhereFilterDelegating) filter).getWrappedFilter();
    }

    /**
     * Every filter in the tree is streamed, parents before children, including the contents of a
     * {@link WhereFilterDelegating} wrapper that is not one of the known wrapper types.
     */
    @Test
    public void testStreamsEveryFilterInTree() {
        final WhereFilter a = WhereFilter.of(RawString.of("X > 1"));
        final WhereFilter inverted = WhereFilterInvertedImpl.of(WhereFilter.of(RawString.of("X > 5")));
        final WhereFilter captured = new RowSetCapturingFilter(WhereFilter.of(RawString.of("X < 3")));
        final WhereFilter or = DisjunctiveFilter.of(a, inverted, captured);
        final WhereFilter declared = or.withDeclaredBarriers("BARRIER");
        final WhereFilter root = declared.withSerial();

        assertSame("sanity: serial wraps the barrier wrapper", declared, wrapped(root));
        assertSame("sanity: the barrier wrapper wraps the disjunction", or, wrapped(declared));

        final List<WhereFilter> expected = List.of(
                root, declared, or, a, inverted, wrapped(inverted), captured, wrapped(captured));
        final List<WhereFilter> actual = ExtractAllFilters.stream(root).collect(Collectors.toList());

        assertEquals(expected.size(), actual.size());
        for (int ii = 0; ii < expected.size(); ii++) {
            assertSame("filter " + ii, expected.get(ii), actual.get(ii));
        }
    }
}
