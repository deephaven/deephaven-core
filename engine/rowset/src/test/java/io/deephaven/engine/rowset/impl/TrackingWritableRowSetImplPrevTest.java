/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.UpdateContext;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TrackingWritableRowSetImplPrevTest {
    @Rule
    final public EngineCleanup engineCleanup = new EngineCleanup();

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
        UpdateContext.logicalClock().startUpdateCycle();
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
        UpdateContext.logicalClock().completeUpdateCycle();
        assertEquals(4L, ix.size());
        assertEquals(2L, ix.sizePrev());
        assertEquals(2L, ix.lastRowKeyPrev());
        ix.insert(5L);
        assertEquals(5L, ix.size());
        assertEquals(2L, ix.sizePrev());
        assertEquals(2L, ix.lastRowKeyPrev());
        UpdateContext.logicalClock().startUpdateCycle();
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
        UpdateContext.logicalClock().startUpdateCycle();
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
        UpdateContext.logicalClock().completeUpdateCycle();
        assertEquals(5L, ix.size());
        assertEquals(3L, ix.sizePrev());
        assertEquals(1L, ix.firstRowKeyPrev());
        assertEquals(5L, ix.lastRowKeyPrev());
        ix.insert(11L);
        assertEquals(6L, ix.size());
        assertEquals(3L, ix.sizePrev());
        assertEquals(1L, ix.firstRowKeyPrev());
        assertEquals(5L, ix.lastRowKeyPrev());
        UpdateContext.logicalClock().startUpdateCycle();
        assertEquals(6L, ix.size());
        assertEquals(6L, ix.sizePrev());
        assertEquals(11L, ix.lastRowKeyPrev());
    }

    @Test
    public void testPrevWithSingleThenRspThenEmptyThenSingle() {
        UpdateContext.logicalClock().resetForUnitTests();
        final TrackingWritableRowSet ix = RowSetFactory.fromKeys(1L).toTracking();
        assertEquals(1L, ix.size());
        assertEquals(1L, ix.firstRowKey());
        assertEquals(1L, ix.sizePrev());
        assertEquals(1L, ix.firstRowKeyPrev());
        UpdateContext.logicalClock().startUpdateCycle();
        ix.insert(3L);
        assertEquals(2L, ix.size());
        assertEquals(3L, ix.lastRowKey());
        assertEquals(1L, ix.sizePrev());
        assertEquals(1L, ix.firstRowKeyPrev());
        UpdateContext.logicalClock().completeUpdateCycle();
        assertEquals(1L, ix.sizePrev());
        assertEquals(1L, ix.lastRowKeyPrev());
        UpdateContext.logicalClock().startUpdateCycle();
        assertEquals(2L, ix.sizePrev());
        assertEquals(3L, ix.lastRowKeyPrev());
        UpdateContext.logicalClock().completeUpdateCycle();
        assertEquals(2L, ix.sizePrev());
        assertEquals(3L, ix.lastRowKeyPrev());
        UpdateContext.logicalClock().startUpdateCycle();
        ix.removeRange(0, 4);
        assertEquals(2L, ix.sizePrev());
        assertEquals(3L, ix.lastRowKeyPrev());
        UpdateContext.logicalClock().completeUpdateCycle();
        assertEquals(2L, ix.sizePrev());
        assertEquals(3L, ix.lastRowKeyPrev());
        UpdateContext.logicalClock().startUpdateCycle();
        assertTrue(ix.copyPrev().isEmpty());
        ix.insert(1L);
        assertTrue(ix.copyPrev().isEmpty());
        UpdateContext.logicalClock().completeUpdateCycle();
        assertTrue(ix.copyPrev().isEmpty());
        UpdateContext.logicalClock().startUpdateCycle();
        assertEquals(1L, ix.sizePrev());
        assertEquals(1L, ix.lastRowKeyPrev());
    }
}
