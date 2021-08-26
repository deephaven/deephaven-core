package io.deephaven.db.v2.sources.chunk;

import static io.deephaven.db.v2.TstUtils.getTable;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.TstUtils;
import joptsimple.internal.Strings;
import org.junit.Test;

import java.util.Random;
import java.util.function.Consumer;

import static io.deephaven.db.v2.TstUtils.initColumnInfos;
import static org.junit.Assert.*;

public class TestSharedContext {

    private static final class TestSharedContextKey
        implements SharedContext.Key<TestResettableContext> {
    }

    private static final class TestResettableContext implements ResettableContext {

        private boolean reset = false;
        private boolean closed = false;

        @Override
        public void reset() {
            reset = true;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    @Test
    public void testBasic() {
        final TestSharedContextKey k1 = new TestSharedContextKey();
        final TestSharedContextKey k2 = new TestSharedContextKey();
        final TestResettableContext v1 = new TestResettableContext();
        final TestResettableContext v2 = new TestResettableContext();
        final TestResettableContext v3 = new TestResettableContext();

        try (final SharedContext sharedContext = SharedContext.makeSharedContext()) {
            ResettableContext result = sharedContext.getOrCreate(k1, () -> v1);
            assertEquals(v1, result);
            result = sharedContext.getOrCreate(k2, () -> v2);
            assertEquals(v2, result);
            result = sharedContext.getOrCreate(k1, () -> v3);
            assertEquals(v1, result);
            result = sharedContext.getOrCreate(k2, () -> v3);
            assertEquals(v2, result);

            assertFalse(v1.reset);
            assertFalse(v2.reset);
            assertFalse(v3.reset);
            sharedContext.reset();

            assertTrue(v1.reset);
            assertTrue(v2.reset);
            assertFalse(v3.reset);
            assertFalse(v1.closed);
            assertFalse(v2.closed);
            assertFalse(v3.closed);
        }

        assertTrue(v1.closed);
        assertTrue(v2.closed);
        assertFalse(v3.closed);
    }

    @Test
    public void testConditionFilterWithSimpleRedirections() {
        final int size = 16 * 1024; // hopefully bigger that twice our chunk size.
        final Random random = new Random(1);
        final int nCols = 4;
        final TstUtils.Generator[] gs = new TstUtils.Generator[nCols];
        final String[] cols = new String[nCols];
        final int imin = 1;
        final int imax = 100000;
        final String[] conditions = new String[nCols];
        for (int i = 0; i < nCols; ++i) {
            cols[i] = "I" + i;
            gs[i] = new TstUtils.IntGenerator(imin, imax);
            conditions[i] = cols[i] + " <= " + (nCols - 1) * (imax + imin) / nCols;
        }
        final String condition = Strings.join(conditions, " && ");
        final QueryTable t0 = getTable(size, random, initColumnInfos(cols, gs));
        final String sortCol = "TS";
        LiveTableMonitor.DEFAULT.exclusiveLock().doLocked(() -> {
            final Table t1 = t0.update(sortCol + "=i").reverse();
            final Table t1Filtered = t1.where(condition);
            final Table t2 = t1.sort(sortCol);
            final Table t2Filtered = t2.where(condition).reverse();
            assertEquals(t2.size(), t1.size());
            final Consumer<String> columnChecker = (final String col) -> {
                final int[] t2fcs = (int[]) t2Filtered.getColumn(col).getDirect();
                assertEquals(t2Filtered.size(), t2fcs.length);
                final int[] t1fcs = (int[]) t1Filtered.getColumn(col).getDirect();
                assertEquals(t1Filtered.size(), t1fcs.length);
                assertArrayEquals(t1fcs, t2fcs);
            };
            for (String col : cols) {
                columnChecker.accept(col);
            }
            columnChecker.accept(sortCol);
        });
    }

    @Test
    public void testConditionFilterWithMoreComplexRedirections() {
        final int size = 16 * 1024; // hopefully bigger that twice our chunk size.
        final Random random = new Random(1);
        final int nCols = 4;
        final TstUtils.Generator[] gs = new TstUtils.Generator[nCols];
        final String[] cols = new String[nCols];
        final int imin = 1;
        final int imax = 100000;
        final int threshold = (nCols - 1) * (imax + imin) / nCols;
        final String[] conditions = new String[nCols];
        final String[] joinedConditions = new String[nCols];
        final String[] joinColumnsToAdd = new String[nCols];
        for (int i = 0; i < nCols; ++i) {
            cols[i] = "I" + i;
            gs[i] = new TstUtils.IntGenerator(imin, imax);
            conditions[i] = cols[i] + " <= " + threshold;
            final String joinRename = "J" + cols[i];
            joinColumnsToAdd[i] = joinRename + "=" + cols[i];
            joinedConditions[i] = joinRename + " <= " + threshold;
        }
        final String condition = Strings.join(conditions, " && ");
        final String joinedCondition = Strings.join(joinedConditions, " && ");
        final QueryTable t0 = getTable(size, random, initColumnInfos(cols, gs));
        final String sortCol = "TS";
        final String formulaCol = "F";
        LiveTableMonitor.DEFAULT.exclusiveLock().doLocked(() -> {
            final Table t1 =
                t0.update(sortCol + "=i", formulaCol + "=" + cols[0] + "+" + cols[1]).reverse();
            final Table t1Filtered = t1.where(condition);
            final Table t2 =
                t1.sort(sortCol).naturalJoin(t1, sortCol, Strings.join(joinColumnsToAdd, ","));
            final Table t2Filtered = t2.where(joinedCondition).reverse();
            assertEquals(t2.size(), t1.size());
            final Consumer<String> columnChecker = (final String col) -> {
                final int[] t2fcs = (int[]) t2Filtered.getColumn(col).getDirect();
                assertEquals(t2Filtered.size(), t2fcs.length);
                final int[] t1fcs = (int[]) t1Filtered.getColumn(col).getDirect();
                assertEquals(t1Filtered.size(), t1fcs.length);
                assertArrayEquals(t1fcs, t2fcs);
            };
            for (String col : cols) {
                columnChecker.accept(col);
            }
            columnChecker.accept(sortCol);
            columnChecker.accept(formulaCol);
        });
    }
}
