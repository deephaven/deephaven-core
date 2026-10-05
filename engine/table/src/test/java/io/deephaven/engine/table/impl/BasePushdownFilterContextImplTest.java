//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BasePushdownFilterContext.FilterNullBehavior;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.table.impl.select.WhereFilterImpl;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link BasePushdownFilterContextImpl}.
 */
public class BasePushdownFilterContextImplTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static FilterNullBehavior nullBehaviorOf(final WhereFilter filter, final ColumnSource<?> source) {
        try (final BasePushdownFilterContextImpl context = new BasePushdownFilterContextImpl(filter, List.of(source))) {
            return context.filterNullBehavior();
        }
    }

    /**
     * The null-behavior probe evaluates a copy of the filter. For a {@link MatchFilter} that failed over to a
     * {@code ConditionFilter}, the copy must still evaluate correctly; otherwise the probe throws and reports
     * {@link FilterNullBehavior#FAILS_ON_NULLS}, disabling null-aware pushdown for the filter.
     */
    @Test
    public void testFilterNullBehaviorOfFailoverMatchFilter() {
        final IntegerArraySource source = new IntegerArraySource();
        source.ensureCapacity(1, false);
        source.set(0, 0);
        final QueryTable table = new QueryTable(RowSetFactory.flat(1).toTracking(), Map.of("X", source));

        for (final Map.Entry<String, FilterNullBehavior> entry : Map.of(
                "X == i", FilterNullBehavior.EXCLUDES_NULLS,
                "X != i", FilterNullBehavior.INCLUDES_NULLS).entrySet()) {
            final String expression = entry.getKey();
            final WhereFilter filter = WhereFilterFactory.getExpression(expression);
            filter.init(table.getDefinition());
            assertTrue("sanity: " + expression + " parses to a MatchFilter", filter instanceof MatchFilter);
            assertNotNull("sanity: " + expression + " fails over",
                    ((MatchFilter) filter).getFailoverFilterIfCached());

            assertEquals(expression, entry.getValue(), nullBehaviorOf(filter, source));
        }
    }

    /**
     * A filter that fails when probed against a null reports {@link FilterNullBehavior#FAILS_ON_NULLS}.
     */
    @Test
    public void testFilterNullBehaviorOfFailingFilter() {
        assertEquals(FilterNullBehavior.FAILS_ON_NULLS, nullBehaviorOf(
                new ThrowingFilter("X", () -> new IllegalStateException("cannot filter a null")),
                new IntegerArraySource()));
    }

    /**
     * A cancellation during the null-behavior probe escapes rather than being reported as
     * {@link FilterNullBehavior#FAILS_ON_NULLS}, including when an interrupt arrives wrapped, as the query compiler
     * wraps one.
     */
    @Test
    public void testFilterNullBehaviorPropagatesCancellation() {
        for (final RuntimeException failure : List.of(
                new CancellationException("cancelled while probing"),
                new UncheckedDeephavenException("Interrupted while compiling class", new InterruptedException()))) {
            final RuntimeException thrown = assertThrows(RuntimeException.class,
                    () -> nullBehaviorOf(new ThrowingFilter("X", () -> failure), new IntegerArraySource()));
            assertSame(failure, thrown);
        }
    }

    /**
     * A filter on one column whose {@link #filter} throws the supplied exception.
     */
    private static final class ThrowingFilter extends WhereFilterImpl {
        private final String column;
        private final Supplier<RuntimeException> failure;

        private ThrowingFilter(final String column, final Supplier<RuntimeException> failure) {
            this.column = column;
            this.failure = failure;
        }

        @Override
        public List<String> getColumns() {
            return List.of(column);
        }

        @Override
        public List<String> getColumnArrays() {
            return List.of();
        }

        @Override
        public void init(@NotNull final TableDefinition tableDefinition) {}

        @NotNull
        @Override
        public WritableRowSet filter(
                @NotNull final RowSet selection, @NotNull final RowSet fullSet, @NotNull final Table table,
                final boolean usePrev) {
            throw failure.get();
        }

        @Override
        public boolean isSimpleFilter() {
            return true;
        }

        @Override
        public void setRecomputeListener(final RecomputeListener result) {}

        @Override
        public WhereFilter copy() {
            return new ThrowingFilter(column, failure);
        }
    }
}
