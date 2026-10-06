//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.testing.JMockRule;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BasePushdownFilterContextImpl;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterImpl;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

/**
 * Base class for testing {@link ColumnRegion} implementations.
 */

abstract class TstColumnRegionPrimitive<REGION_TYPE extends ColumnRegion<Values>> {

    @Rule
    public final JMockRule jmock = new JMockRule();

    REGION_TYPE SUT;

    @Test
    public abstract void testGet();

    static abstract class Deferred<REGION_TYPE extends ColumnRegion<Values>>
            extends TstColumnRegionPrimitive<REGION_TYPE> {

        Supplier<REGION_TYPE> regionSupplier;
    }

    static abstract class Constant<REGION_TYPE extends ColumnRegion<Values>>
            extends TstColumnRegionPrimitive<REGION_TYPE> {

        @Rule
        public final EngineCleanup framework = new EngineCleanup();

        /**
         * A filter the pushdown context cannot evaluate as a chunk filter, so the constant region must evaluate it
         * against a table and hand it the {@code usePrev} it was given.
         */
        private static final class RecordingFilter extends WhereFilterImpl {

            private final List<Boolean> usePrevValues = new ArrayList<>();

            @Override
            public List<String> getColumns() {
                return List.of("X");
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
                usePrevValues.add(usePrev);
                return selection.copy();
            }

            @Override
            public boolean isSimpleFilter() {
                return true;
            }

            @Override
            public void setRecomputeListener(final RecomputeListener result) {}

            @Override
            public WhereFilter copy() {
                return new RecordingFilter();
            }
        }

        @Test
        public void testPushdownForwardsUsePrev() {
            final RecordingFilter filter = new RecordingFilter();
            try (final BasePushdownFilterContextImpl context = new BasePushdownFilterContextImpl(filter, List.of());
                    final RowSet selection = RowSetFactory.fromRange(0, 9);
                    final PushdownResult input = PushdownResult.allMaybeMatch(selection);
                    final PushdownResult result = SUT.performPushdownAction(SUT.supportedActions().get(0), filter,
                            selection, input, true, context, null)) {
                assertEquals(List.of(true), filter.usePrevValues);
                assertEquals(selection, result.match());
            }
        }
    }
}
