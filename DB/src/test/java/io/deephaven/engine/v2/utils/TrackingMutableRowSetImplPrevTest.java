package io.deephaven.engine.v2.utils;

import io.deephaven.engine.tables.live.LiveTableMonitor;
import io.deephaven.engine.v2.sources.LogicalClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TrackingMutableRowSetImplPrevTest {
    @Before
    public void setUp() throws Exception {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);
    }

    @After
    public void tearDown() throws Exception {
        LiveTableMonitor.DEFAULT.resetForUnitTests(true);
    }

    @Test
    public void testPrevWithEmptyConstruction() {
        final TrackingMutableRowSet ix = RowSetFactoryImpl.INSTANCE.getEmptyRowSet();
        assertTrue(ix.empty());
        assertTrue(ix.getPrevRowSet().empty());
    }

    @Test
    public void testPrevWithSingleRangeIxOnly() {
        final TrackingMutableRowSet ix = RowSetFactoryImpl.INSTANCE.getRowSetByValues(1L);
        assertEquals(1L, ix.size());
        assertEquals(1L, ix.firstRowKey());
        assertEquals(1L, ix.getPrevRowSet().size());
        assertEquals(1L, ix.getPrevRowSet().firstRowKey());
        ix.insert(2L);
        assertEquals(1L, ix.getPrevRowSet().size());
        assertEquals(1L, ix.getPrevRowSet().firstRowKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(2L, ix.getPrevRowSet().size());
        assertEquals(2L, ix.getPrevRowSet().lastRowKey());
        ix.insert(3L);
        assertEquals(3L, ix.size());
        assertEquals(2L, ix.getPrevRowSet().size());
        assertEquals(2L, ix.getPrevRowSet().lastRowKey());
        ix.insert(4L);
        assertEquals(4L, ix.size());
        assertEquals(2L, ix.getPrevRowSet().size());
        assertEquals(2L, ix.getPrevRowSet().lastRowKey());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertEquals(4L, ix.size());
        assertEquals(2L, ix.getPrevRowSet().size());
        assertEquals(2L, ix.getPrevRowSet().lastRowKey());
        ix.insert(5L);
        assertEquals(5L, ix.size());
        assertEquals(2L, ix.getPrevRowSet().size());
        assertEquals(2L, ix.getPrevRowSet().lastRowKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(5L, ix.size());
        assertEquals(5L, ix.getPrevRowSet().size());
        assertEquals(5L, ix.getPrevRowSet().lastRowKey());
    }

    @Test
    public void testPrevWithRspOnly() {
        final TrackingMutableRowSet ix = RowSetFactoryImpl.INSTANCE.getRowSetByValues(1, 3);
        assertEquals(2L, ix.size());
        assertEquals(1L, ix.firstRowKey());
        assertEquals(3L, ix.lastRowKey());
        assertEquals(2L, ix.getPrevRowSet().size());
        assertEquals(1L, ix.getPrevRowSet().firstRowKey());
        assertEquals(3L, ix.getPrevRowSet().lastRowKey());
        ix.insert(5L);
        assertEquals(3L, ix.size());
        assertEquals(1L, ix.firstRowKey());
        assertEquals(5L, ix.lastRowKey());
        assertEquals(2L, ix.getPrevRowSet().size());
        assertEquals(1L, ix.getPrevRowSet().firstRowKey());
        assertEquals(3L, ix.getPrevRowSet().lastRowKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(3L, ix.getPrevRowSet().size());
        assertEquals(1L, ix.getPrevRowSet().firstRowKey());
        assertEquals(5L, ix.getPrevRowSet().lastRowKey());
        ix.insert(7L);
        assertEquals(4L, ix.size());
        assertEquals(3L, ix.getPrevRowSet().size());
        assertEquals(1L, ix.getPrevRowSet().firstRowKey());
        assertEquals(5L, ix.getPrevRowSet().lastRowKey());
        ix.insert(9L);
        assertEquals(5L, ix.size());
        assertEquals(3L, ix.getPrevRowSet().size());
        assertEquals(1L, ix.getPrevRowSet().firstRowKey());
        assertEquals(5L, ix.getPrevRowSet().lastRowKey());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertEquals(5L, ix.size());
        assertEquals(3L, ix.getPrevRowSet().size());
        assertEquals(1L, ix.getPrevRowSet().firstRowKey());
        assertEquals(5L, ix.getPrevRowSet().lastRowKey());
        ix.insert(11L);
        assertEquals(6L, ix.size());
        assertEquals(3L, ix.getPrevRowSet().size());
        assertEquals(1L, ix.getPrevRowSet().firstRowKey());
        assertEquals(5L, ix.getPrevRowSet().lastRowKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(6L, ix.size());
        assertEquals(6L, ix.getPrevRowSet().size());
        assertEquals(11L, ix.getPrevRowSet().lastRowKey());
    }

    @Test
    public void testPrevWithSingleThenRspThenEmptyThenSingle() {
        LogicalClock.DEFAULT.resetForUnitTests();
        final TrackingMutableRowSet ix = RowSetFactoryImpl.INSTANCE.getRowSetByValues(1L);
        assertEquals(1L, ix.size());
        assertEquals(1L, ix.firstRowKey());
        assertEquals(1L, ix.getPrevRowSet().size());
        assertEquals(1L, ix.getPrevRowSet().firstRowKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        ix.insert(3L);
        assertEquals(2L, ix.size());
        assertEquals(3L, ix.lastRowKey());
        assertEquals(1L, ix.getPrevRowSet().size());
        assertEquals(1L, ix.getPrevRowSet().firstRowKey());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertEquals(1L, ix.getPrevRowSet().size());
        assertEquals(1L, ix.getPrevRowSet().lastRowKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(2L, ix.getPrevRowSet().size());
        assertEquals(3L, ix.getPrevRowSet().lastRowKey());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertEquals(2L, ix.getPrevRowSet().size());
        assertEquals(3L, ix.getPrevRowSet().lastRowKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        ix.removeRange(0, 4);
        assertEquals(2L, ix.getPrevRowSet().size());
        assertEquals(3L, ix.getPrevRowSet().lastRowKey());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertEquals(2L, ix.getPrevRowSet().size());
        assertEquals(3L, ix.getPrevRowSet().lastRowKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertTrue(ix.getPrevRowSet().empty());
        ix.insert(1L);
        assertTrue(ix.getPrevRowSet().empty());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertTrue(ix.getPrevRowSet().empty());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(1L, ix.getPrevRowSet().size());
        assertEquals(1L, ix.getPrevRowSet().lastRowKey());
    }
}
