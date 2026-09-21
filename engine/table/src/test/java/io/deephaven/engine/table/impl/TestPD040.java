//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.RawString;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

/**
 * Reproducer for <b>PD-040 (P2)</b> — a failure while constructing a filter's pushdown context aborted the entire
 * {@code where()}.
 * <p>
 * {@code AbstractFilterExecution.filterStatelessCollection} built every filter's {@link PushdownFilterMatcher} and
 * {@link PushdownFilterContext} inside a single {@code try} block whose {@code catch} calls
 * {@code informAndCloseAll(collectionNec, ex, ...)} — failing the whole operation. Pushdown is purely an optimization,
 * so a fault there could take down a query that plain filtering would have satisfied.
 * <p>
 * No <i>reachable</i> trigger exists in this tree: {@code DataIndexPushdownManager.makePushdownFilterContext}'s "No
 * associated source" {@code IllegalStateException} cannot fire because {@code WhereListener.extractFilterDataIndexMap}
 * derives the index from the same column sources that manager later looks up, and
 * {@code PushdownPredicateManager.computeRenameMap}'s {@code IllegalArgumentException} needs the
 * reinterpreted-union-source case that PD-048 classifies as unreachable. This test therefore injects the failure with a
 * column source whose {@code makePushdownFilterContext} throws, which is the shape those two throwers would have.
 * <p>
 * <b>Expected after a fix:</b> the {@code where()} succeeds with the correct rows, having logged the failure and
 * degraded to no pushdown for that filter. <b>Before the fix</b> it failed the query.
 */
public class TestPD040 {

    private static final int TABLE_SIZE = 100;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    /**
     * An {@link AbstractColumnSource} — so it resolves as its own pushdown matcher — whose pushdown context
     * construction fails.
     */
    private static final class ThrowingPushdownContextSource extends IntegerArraySource {
        @Override
        public PushdownFilterContext makePushdownFilterContext(
                final WhereFilter filter,
                final List<ColumnSource<?>> filterSources) {
            throw new IllegalStateException("PD-040: injected pushdown context construction failure");
        }
    }

    private static QueryTable makeTable(final IntegerArraySource source) {
        source.ensureCapacity(TABLE_SIZE, false);
        for (int ii = 0; ii < TABLE_SIZE; ii++) {
            source.set(ii, ii);
        }
        return new QueryTable(RowSetFactory.flat(TABLE_SIZE).toTracking(), Map.of("X", source));
    }

    /** Sanity check: the injected source really does fail to build a context. */
    @Test
    public void injectedSourceFailsToMakeContext() {
        final ThrowingPushdownContextSource source = new ThrowingPushdownContextSource();
        final QueryTable table = makeTable(source);

        final WhereFilter filter = WhereFilter.of(RawString.of("X >= 50"));
        filter.init(table.getDefinition());

        assertThrows(IllegalStateException.class,
                () -> source.makePushdownFilterContext(filter, List.of(source)));
    }

    /**
     * The operation must survive that failure and return the rows plain filtering would have returned.
     */
    @Test
    public void contextConstructionFailureDegradesToPlainFiltering() {
        final Table oracle = makeTable(new IntegerArraySource()).where("X >= 50");
        final Table actual = makeTable(new ThrowingPushdownContextSource()).where("X >= 50");

        assertArrayEquals(
                "a pushdown-context construction failure must degrade to plain filtering, not fail the where()",
                ColumnVectors.ofInt(oracle, "X").toArray(),
                ColumnVectors.ofInt(actual, "X").toArray());
    }
}
