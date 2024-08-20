//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.vectors;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TimeTable;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TestClock;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link ColumnVectors}.
 */
public class TestColumnVectors {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testSmallTable() {
        testTable(10);
    }

    @Test
    public void testLargeTable() {
        testTable(1_000_000);
    }

    private void testTable(final int size) {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final TestClock clock = new TestClock();
        final TimeTable timeTable = new TimeTable(updateGraph, clock, null, 1, false);
        final Table source = timeTable
                .updateView(
                        "B   = ii % 3 == 0 ? NULL_BYTE   : (byte)  ii",
                        "C   = ii % 3 == 0 ? NULL_CHAR   : (char)  ('A' + ii % 27)",
                        "S   = ii % 3 == 0 ? NULL_SHORT  : (short) ii",
                        "I   = ii % 3 == 0 ? NULL_INT    : (int)   ii",
                        "L   = ii % 3 == 0 ? NULL_LONG   :         ii",
                        "F   = ii % 3 == 0 ? NULL_FLOAT  : (float) (ii * 0.25)",
                        "D   = ii % 3 == 0 ? NULL_DOUBLE :         ii * 1.25",
                        "Bl  = ii % 3 == 0 ? null        :         ii % 3 == 1",
                        "Str = ii % 3 == 0 ? null        :         Long.toString(ii)")
                .tail(size);
        TestCase.assertTrue(source.isEmpty());
        TstUtils.assertTableEquals(source, copyTable(source, false));

        for (int si = 0; si < 10; ++si) {
            final Table prevSourceSnapshot = source.snapshot();

            updateGraph.startCycleForUnitTests();
            try {
                clock.now += 1_000_000_000L;
                timeTable.run();
                updateGraph.flushAllNormalNotificationsForUnitTests();

                TstUtils.assertTableEquals(prevSourceSnapshot, copyTable(source, true));
                TstUtils.assertTableEquals(source, copyTable(source, false));
            } finally {
                updateGraph.completeCycleForUnitTests();
            }
        }
    }

    private static Table copyTable(@NotNull final Table source, final boolean prev) {
        final Table coalesced = source.coalesce();
        return new QueryTable(
                coalesced.getDefinition(),
                RowSetFactory.flat(prev ? coalesced.getRowSet().sizePrev() : coalesced.size()).toTracking(),
                (Map<String, ? extends ColumnSource<?>>) coalesced.getDefinition().getColumnStream()
                        .collect(Collectors.toMap(
                                ColumnDefinition::getName,
                                cd -> InMemoryColumnSource.getImmutableMemoryColumnSource(
                                        ColumnVectors.of(coalesced, cd.getName(), prev).copyToArray(),
                                        cd.getDataType(),
                                        cd.getComponentType()),
                                Assert::neverInvoked,
                                LinkedHashMap::new)));
    }
}
