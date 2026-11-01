//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import java.util.stream.LongStream;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.removeRows;
import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestBooleanAdaptiveSparseArraySource extends AbstractBooleanColumnSourceTest {
    @NotNull
    @Override
    BooleanAdaptiveSparseArraySource makeTestSource() {
        return new BooleanAdaptiveSparseArraySource();
    }

    @Test
    public void testReinterpret() {
        final BooleanAdaptiveSparseArraySource source = makeTestSource();
        source.set(3, true);
        source.set(5, false);
        final ColumnSource<Byte> bytes = source.reinterpret(byte.class);
        assertTrue(bytes instanceof ByteAdaptiveSparseArraySource);
        assertSame(bytes, ReinterpretUtils.maybeConvertToPrimitive(source));
        assertSame(Boolean.TRUE, source.get(3));
        assertSame(Boolean.FALSE, source.get(5));
        assertSame(null, source.get(4));
    }

    @Test
    public void testUpdate() {
        final boolean startAdaptive = SparseMemoryColumnSourceFactory.USE_ADAPTIVE;
        try {
            SparseMemoryColumnSourceFactory.USE_ADAPTIVE = true;
            final QueryTable table = TstUtils.testRefreshingTable(
                    RowSetFactory.fromKeys(LongStream.range(0, 3000).map(key -> key * 3).toArray()).toTracking());
            final Table updated = table.update("B = k % 7 == 0 ? null : k % 7 < 4");
            final ColumnSource<Boolean> column = updated.getColumnSource("B", Boolean.class);
            assertTrue(column instanceof BooleanAdaptiveSparseArraySource);
            assertTableEquals(table.view("B = k % 7 == 0 ? null : k % 7 < 4"), updated);

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            for (int cycle = 0; cycle < 4; ++cycle) {
                final long firstKey = 9000 + cycle * 1000L;
                updateGraph.runWithinUnitTestCycle(() -> {
                    final RowSet added = RowSetFactory.fromRange(firstKey, firstKey + 999);
                    final RowSet removed = RowSetFactory.fromRange(firstKey - 9000, firstKey - 8001);
                    removeRows(table, removed);
                    addToTable(table, added);
                    table.notifyListeners(added, removed.intersect(table.getRowSet().prev()), i());
                });
                assertTableEquals(table.view("B = k % 7 == 0 ? null : k % 7 < 4"), updated);
            }
        } finally {
            SparseMemoryColumnSourceFactory.USE_ADAPTIVE = startAdaptive;
        }
    }
}
