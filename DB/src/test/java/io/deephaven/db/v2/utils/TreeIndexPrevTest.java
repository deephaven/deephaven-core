package io.deephaven.db.v2.utils;

import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.sources.LogicalClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TreeIndexPrevTest {
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
        final Index ix = Index.FACTORY.getEmptyIndex();
        assertTrue(ix.empty());
        assertTrue(ix.getPrevIndex().empty());
    }

    @Test
    public void testPrevWithSingleRangeIxOnly() {
        final Index ix = Index.FACTORY.getIndexByValues(1L);
        assertEquals(1L, ix.size());
        assertEquals(1L, ix.firstKey());
        assertEquals(1L, ix.getPrevIndex().size());
        assertEquals(1L, ix.getPrevIndex().firstKey());
        ix.insert(2L);
        assertEquals(1L, ix.getPrevIndex().size());
        assertEquals(1L, ix.getPrevIndex().firstKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(2L, ix.getPrevIndex().size());
        assertEquals(2L, ix.getPrevIndex().lastKey());
        ix.insert(3L);
        assertEquals(3L, ix.size());
        assertEquals(2L, ix.getPrevIndex().size());
        assertEquals(2L, ix.getPrevIndex().lastKey());
        ix.insert(4L);
        assertEquals(4L, ix.size());
        assertEquals(2L, ix.getPrevIndex().size());
        assertEquals(2L, ix.getPrevIndex().lastKey());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertEquals(4L, ix.size());
        assertEquals(2L, ix.getPrevIndex().size());
        assertEquals(2L, ix.getPrevIndex().lastKey());
        ix.insert(5L);
        assertEquals(5L, ix.size());
        assertEquals(2L, ix.getPrevIndex().size());
        assertEquals(2L, ix.getPrevIndex().lastKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(5L, ix.size());
        assertEquals(5L, ix.getPrevIndex().size());
        assertEquals(5L, ix.getPrevIndex().lastKey());
    }

    @Test
    public void testPrevWithRspOnly() {
        final Index ix = Index.FACTORY.getIndexByValues(1, 3);
        assertEquals(2L, ix.size());
        assertEquals(1L, ix.firstKey());
        assertEquals(3L, ix.lastKey());
        assertEquals(2L, ix.getPrevIndex().size());
        assertEquals(1L, ix.getPrevIndex().firstKey());
        assertEquals(3L, ix.getPrevIndex().lastKey());
        ix.insert(5L);
        assertEquals(3L, ix.size());
        assertEquals(1L, ix.firstKey());
        assertEquals(5L, ix.lastKey());
        assertEquals(2L, ix.getPrevIndex().size());
        assertEquals(1L, ix.getPrevIndex().firstKey());
        assertEquals(3L, ix.getPrevIndex().lastKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(3L, ix.getPrevIndex().size());
        assertEquals(1L, ix.getPrevIndex().firstKey());
        assertEquals(5L, ix.getPrevIndex().lastKey());
        ix.insert(7L);
        assertEquals(4L, ix.size());
        assertEquals(3L, ix.getPrevIndex().size());
        assertEquals(1L, ix.getPrevIndex().firstKey());
        assertEquals(5L, ix.getPrevIndex().lastKey());
        ix.insert(9L);
        assertEquals(5L, ix.size());
        assertEquals(3L, ix.getPrevIndex().size());
        assertEquals(1L, ix.getPrevIndex().firstKey());
        assertEquals(5L, ix.getPrevIndex().lastKey());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertEquals(5L, ix.size());
        assertEquals(3L, ix.getPrevIndex().size());
        assertEquals(1L, ix.getPrevIndex().firstKey());
        assertEquals(5L, ix.getPrevIndex().lastKey());
        ix.insert(11L);
        assertEquals(6L, ix.size());
        assertEquals(3L, ix.getPrevIndex().size());
        assertEquals(1L, ix.getPrevIndex().firstKey());
        assertEquals(5L, ix.getPrevIndex().lastKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(6L, ix.size());
        assertEquals(6L, ix.getPrevIndex().size());
        assertEquals(11L, ix.getPrevIndex().lastKey());
    }

    @Test
    public void testPrevWithSingleThenRspThenEmptyThenSingle() {
        LogicalClock.DEFAULT.resetForUnitTests();
        final Index ix = Index.FACTORY.getIndexByValues(1L);
        assertEquals(1L, ix.size());
        assertEquals(1L, ix.firstKey());
        assertEquals(1L, ix.getPrevIndex().size());
        assertEquals(1L, ix.getPrevIndex().firstKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        ix.insert(3L);
        assertEquals(2L, ix.size());
        assertEquals(3L, ix.lastKey());
        assertEquals(1L, ix.getPrevIndex().size());
        assertEquals(1L, ix.getPrevIndex().firstKey());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertEquals(1L, ix.getPrevIndex().size());
        assertEquals(1L, ix.getPrevIndex().lastKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(2L, ix.getPrevIndex().size());
        assertEquals(3L, ix.getPrevIndex().lastKey());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertEquals(2L, ix.getPrevIndex().size());
        assertEquals(3L, ix.getPrevIndex().lastKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        ix.removeRange(0, 4);
        assertEquals(2L, ix.getPrevIndex().size());
        assertEquals(3L, ix.getPrevIndex().lastKey());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertEquals(2L, ix.getPrevIndex().size());
        assertEquals(3L, ix.getPrevIndex().lastKey());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertTrue(ix.getPrevIndex().empty());
        ix.insert(1L);
        assertTrue(ix.getPrevIndex().empty());
        LogicalClock.DEFAULT.completeUpdateCycle();
        assertTrue(ix.getPrevIndex().empty());
        LogicalClock.DEFAULT.startUpdateCycle();
        assertEquals(1L, ix.getPrevIndex().size());
        assertEquals(1L, ix.getPrevIndex().lastKey());
    }
}
