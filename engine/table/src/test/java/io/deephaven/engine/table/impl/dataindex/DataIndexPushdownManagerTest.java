//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.api.RawString;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.impl.BasePushdownFilterContextImpl;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownFilterMatcher;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DataIndexPushdownManager}.
 */
public class DataIndexPushdownManagerTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    /** An {@link IntegerArraySource} whose pushdown contexts record whether they were closed. */
    private static final class CloseTrackingSource extends IntegerArraySource {
        CloseTrackingContext lastContext;

        @Override
        public PushdownFilterContext makePushdownFilterContext(
                final WhereFilter filter,
                final List<ColumnSource<?>> filterSources) {
            return lastContext = new CloseTrackingContext(filter, filterSources);
        }
    }

    private static final class CloseTrackingContext extends BasePushdownFilterContextImpl {
        boolean closed;

        CloseTrackingContext(final WhereFilter filter, final List<ColumnSource<?>> columnSources) {
            super(filter, columnSources);
        }

        @Override
        public void close() {
            closed = true;
            super.close();
        }
    }

    private static <T extends IntegerArraySource> T fill(final T source, final int size) {
        source.ensureCapacity(size, false);
        for (int ii = 0; ii < size; ii++) {
            source.set(ii, ii % 3);
        }
        return source;
    }

    /**
     * The wrapped matcher's context is created before the data-index context that will own it. If the outer
     * construction fails, nothing owns the wrapped context yet, so the factory must close it rather than leak it.
     */
    @Test
    public void testWrappedContextClosedWhenOuterConstructionFails() {
        final int size = 10;
        final CloseTrackingSource filterSource = fill(new CloseTrackingSource(), size);
        final IntegerArraySource indexedSource = fill(new IntegerArraySource(), size);
        final QueryTable table = new QueryTable(RowSetFactory.flat(size).toTracking(),
                Map.of("X", filterSource, "A", indexedSource));

        // The index is on A, but the filter is on X: the data-index context's constructor rejects a filter source it
        // has no index column for. WhereListener never pairs them this way, so this is the broken-invariant path.
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(table, "A");
        final WhereFilter filter = WhereFilter.of(RawString.of("X > 1"));
        filter.init(table.getDefinition());

        final PushdownFilterMatcher matcher = DataIndexPushdownManager.wrap(dataIndex, filterSource);
        assertThrows(IllegalStateException.class,
                () -> matcher.makePushdownFilterContext(filter, List.of(filterSource)));

        assertNotNull("sanity: the wrapped matcher built a context", filterSource.lastContext);
        assertTrue("the wrapped context must be closed when the outer construction fails",
                filterSource.lastContext.closed);
    }
}
