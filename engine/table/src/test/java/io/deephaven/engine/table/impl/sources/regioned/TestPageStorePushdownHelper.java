//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.testing.JMockRule;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PageStorePushdownHelper}.
 */
public class TestPageStorePushdownHelper {

    @Rule
    public final JMockRule jmock = new JMockRule();

    private static final RegionedPageStore.Parameters PARAMETERS =
            new RegionedPageStore.Parameters((1L << 40) - 1, 0, 0);

    @SuppressWarnings("unchecked")
    private ColumnRegionInt.StaticPageStore<Values> emptyPageStore() {
        return new ColumnRegionInt.StaticPageStore<>(PARAMETERS, new ColumnRegionInt[0],
                jmock.mock(ColumnLocation.class));
    }

    @Test
    public void testEmptyPageStoreDeclines() {
        final ColumnRegionInt.StaticPageStore<Values> pageStore = emptyPageStore();
        assertEquals(0, pageStore.supportedActions().size());
        try (final RowSet selection = RowSetFactory.fromRange(0, 9)) {
            assertEquals(PushdownResult.UNSUPPORTED_ACTION_COST,
                    pageStore.estimatePushdownAction(null, null, selection, false, null, null));
        }
    }

    @Test
    public void testEmptyPageStorePerformPreservesInput() {
        // A page store with no subregions has proven nothing about the selection, so it must return its input
        // unchanged; excluding rows would permanently drop them from the filter result.
        try (final RowSet selection = RowSetFactory.fromRange(0, 9);
                final RowSet match = RowSetFactory.fromRange(0, 2);
                final RowSet maybeMatch = RowSetFactory.fromRange(3, 9);
                final PushdownResult input = PushdownResult.of(selection, match, maybeMatch);
                final PushdownResult result = emptyPageStore().performPushdownAction(
                        null, null, selection, input, false, null, null)) {
            assertEquals(match, result.match());
            assertEquals(maybeMatch, result.maybeMatch());
        }
    }
}
