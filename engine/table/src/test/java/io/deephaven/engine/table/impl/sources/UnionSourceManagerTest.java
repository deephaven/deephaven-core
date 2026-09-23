//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.api.RawString;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link UnionSourceManager}.
 */
public class UnionSourceManagerTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    /**
     * A pushdown context builds its per-constituent contexts in {@code initialize()}, which runs only when the filter
     * is estimated or pushed down. Filter execution can close a context that never got that far, for instance when a
     * later filter's context construction fails, so closing an uninitialized context must succeed.
     */
    @Test
    public void testCloseUninitializedPushdownContext() {
        final Table merged = TableTools.merge(
                TableTools.emptyTable(3).update("X = (int) ii"),
                TableTools.emptyTable(3).update("X = (int) ii"));
        final ColumnSource<?> source = merged.getColumnSource("X");
        assertTrue("sanity: merge() produces a UnionColumnSource, was " + source.getClass(),
                source instanceof UnionColumnSource);

        final WhereFilter filter = WhereFilter.of(RawString.of("X > 1"));
        filter.init(merged.getDefinition());

        final PushdownFilterContext context =
                ((UnionColumnSource<?>) source).makePushdownFilterContext(filter, List.of(source));
        context.close();
    }
}
