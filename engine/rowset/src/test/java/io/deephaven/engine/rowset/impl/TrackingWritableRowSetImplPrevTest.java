package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TrackingWritableRowSetImplPrevTest {
    @Before
    public void setUp() throws Exception {
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false);
    }

    @After
    public void tearDown() throws Exception {
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(true);
    }

    @Test
    public void testPrevWithEmptyConstruction() {
        final TrackingRowSet ix = RowSetFactory.empty().toTracking();
        assertTrue(ix.isEmpty());
        assertTrue(ix.copyPrev().isEmpty());
    }

    @Test
    public void testPrevWithSingleRangeIxOnly() {
        final TrackingWritableRowSet ix = RowSetFactory.fromKeys(1L).toTracking();
        assertEquals(1L, ix.size());
        assertEquals(1L, ix.firstRowKey());
        assertEquals(1L, ix.sizePrev());
        assertEquals(1L, ix.firstRowKeyPrev());
        ix.insert(2L);
        assertEquals(1L, ix.sizePrev());
        assertEquals(1L, ix.firstRowKeyPrev());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(2L, ix.sizePrev());
        assertEquals(2L, ix.lastRowKeyPrev());
        ix.insert(3L);
        assertEquals(3L, ix.size());
        assertEquals(2L, ix.sizePrev());
        assertEquals(2L, ix.lastRowKeyPrev());
        ix.insert(4L);
        assertEquals(4L, ix.size());
        assertEquals(2L, ix.sizePrev());
        assertEquals(2L, ix.lastRowKeyPrev());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertEquals(4L, ix.size());
        assertEquals(2L, ix.sizePrev());
        assertEquals(2L, ix.lastRowKeyPrev());
        ix.insert(5L);
        assertEquals(5L, ix.size());
        assertEquals(2L, ix.sizePrev());
        assertEquals(2L, ix.lastRowKeyPrev());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(5L, ix.size());
        assertEquals(5L, ix.sizePrev());
        assertEquals(5L, ix.lastRowKeyPrev());
    }

    @Test
    public void testPrevWithRspOnly() {
        final TrackingWritableRowSet ix = RowSetFactory.fromKeys(1, 3).toTracking();
        assertEquals(2L, ix.size());
        assertEquals(1L, ix.firstRowKey());
        assertEquals(3L, ix.lastRowKey());
        assertEquals(2L, ix.sizePrev());
        assertEquals(1L, ix.firstRowKeyPrev());
        assertEquals(3L, ix.lastRowKeyPrev());
        ix.insert(5L);
        assertEquals(3L, ix.size());
        assertEquals(1L, ix.firstRowKey());
        assertEquals(5L, ix.lastRowKey());
        assertEquals(2L, ix.sizePrev());
        assertEquals(1L, ix.firstRowKeyPrev());
        assertEquals(3L, ix.lastRowKeyPrev());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(3L, ix.sizePrev());
        assertEquals(1L, ix.firstRowKeyPrev());
        assertEquals(5L, ix.lastRowKeyPrev());
        ix.insert(7L);
        assertEquals(4L, ix.size());
        assertEquals(3L, ix.sizePrev());
        assertEquals(1L, ix.firstRowKeyPrev());
        assertEquals(5L, ix.lastRowKeyPrev());
        ix.insert(9L);
        assertEquals(5L, ix.size());
        assertEquals(3L, ix.sizePrev());
        assertEquals(1L, ix.firstRowKeyPrev());
        assertEquals(5L, ix.lastRowKeyPrev());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertEquals(5L, ix.size());
        assertEquals(3L, ix.sizePrev());
        assertEquals(1L, ix.firstRowKeyPrev());
        assertEquals(5L, ix.lastRowKeyPrev());
        ix.insert(11L);
        assertEquals(6L, ix.size());
        assertEquals(3L, ix.sizePrev());
        assertEquals(1L, ix.firstRowKeyPrev());
        assertEquals(5L, ix.lastRowKeyPrev());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(6L, ix.size());
        assertEquals(6L, ix.sizePrev());
        assertEquals(11L, ix.lastRowKeyPrev());
    }

    @Test
    public void testPrevWithSingleThenRspThenEmptyThenSingle() {
        LogicalClock.DEFAULT.resetForUnitTests();
        final TrackingWritableRowSet ix = RowSetFactory.fromKeys(1L).toTracking();
        assertEquals(1L, ix.size());
        assertEquals(1L, ix.firstRowKey());
        assertEquals(1L, ix.sizePrev());
        assertEquals(1L, ix.firstRowKeyPrev());
        LogicalClock.DEFAULT.startUpdateCycle();
        ix.insert(3L);
        assertEquals(2L, ix.size());
        assertEquals(3L, ix.lastRowKey());
        assertEquals(1L, ix.sizePrev());
        assertEquals(1L, ix.firstRowKeyPrev());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertEquals(1L, ix.sizePrev());
        assertEquals(1L, ix.lastRowKeyPrev());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(2L, ix.sizePrev());
        assertEquals(3L, ix.lastRowKeyPrev());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertEquals(2L, ix.sizePrev());
        assertEquals(3L, ix.lastRowKeyPrev());
        LogicalClock.DEFAULT.startUpdateCycle();
        ix.removeRange(0, 4);
        assertEquals(2L, ix.sizePrev());
        assertEquals(3L, ix.lastRowKeyPrev());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertEquals(2L, ix.sizePrev());
        assertEquals(3L, ix.lastRowKeyPrev());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertTrue(ix.copyPrev().isEmpty());
        ix.insert(1L);
        assertTrue(ix.copyPrev().isEmpty());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertTrue(ix.copyPrev().isEmpty());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(1L, ix.sizePrev());
        assertEquals(1L, ix.lastRowKeyPrev());
    }
}
