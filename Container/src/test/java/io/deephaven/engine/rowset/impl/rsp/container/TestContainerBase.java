package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import java.util.function.Supplier;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public abstract class TestContainerBase {

    public abstract Supplier<Container> makeContainer();

    public abstract String containerName();

    protected static void assertSameContents(final Container c1, final Container c2) {
        assertTrue(c1.sameContents(c2));
    }

    @Test
    public void testFind() {
        ContainerTestCommon.doTestFind(makeContainer(), containerName());
    }

    @Test
    public void testShortRangeIterator() {
        ContainerTestCommon.doTestRangeIterator(makeContainer(), containerName());
    }

    @Test
    public void testShortRangeIteratorAdvance() {
        ContainerTestCommon.doTestRangeIteratorAdvance(makeContainer(), containerName());
    }

    @Test
    public void testShortRangeIteratorSearch() {
        ContainerTestCommon.doTestRangeIteratorSearch(makeContainer(), containerName());
    }

    @Test
    public void testShortRangeIteratorNextBuffer() {
        ContainerTestCommon.doTestRangeIteratorNextBuffer(makeContainer(), containerName());
    }

    @Test
    public void testRemoveRange() {
        ContainerTestCommon.doTestRemoveRange(makeContainer(), containerName());
    }

    @Test
    public void testSelect() {
        ContainerTestCommon.doTestSelect(makeContainer(), containerName());
    }

    @Test
    public void testSelectRanges() {
        ContainerTestCommon.doTestSelectRanges(makeContainer(), containerName());
    }

    @Test
    public void testSelectContainer() {
        ContainerTestCommon.doTestSelectContainer(makeContainer(), containerName());
    }

    @Test
    public void testFindRanges() {
        ContainerTestCommon.doTestFindRanges(makeContainer(), containerName());
    }

    @Test
    public void testFindRangesWithMaxPos() {
        ContainerTestCommon.doTestFindRangesWithMaxPos(makeContainer(), containerName());
    }

    @Test
    public void testContainerShortBatchIterator() {
        ContainerTestCommon.doTestContainerShortBatchIterator(makeContainer(), containerName());
    }

    @Test
    public void testCopyOnWrite() {
        ContainerTestCommon.doTestCopyOnWrite(makeContainer(), containerName());
    }

    @Test
    public void testForEachWithRankOffset() {
        ContainerTestCommon.doTestForEachWithRankOffset(makeContainer(), containerName());
    }

    @Test
    public void testForEachRange() {
        ContainerTestCommon.doTestForEachRange(makeContainer(), containerName());
    }

    @Test
    public void testOverlapsRange() {
        ContainerTestCommon.doTestOverlapsRange(makeContainer(), containerName());
    }

    @Test
    public void testContainsRange() {
        ContainerTestCommon.doTestContainsRange(makeContainer(), containerName());
    }

    @Test
    public void testAppend() {
        ContainerTestCommon.doTestAppend(makeContainer(), containerName());
    }

    @Test
    public void testOverlaps() {
        ContainerTestCommon.BoolContainerOp testOp = (Container c1, Container c2) -> c1.overlaps(c2);
        ContainerTestCommon.BoolContainerOp validateOp =
                (Container c1, Container c2) -> c1.and(c2).getCardinality() > 0;
        ContainerTestCommon.doTestBoolOp(testOp, validateOp);
    }

    @Test
    public void testAppendContiguousRanges() {
        Container c = makeContainer().get();
        c = c.iadd(1, 2 + 1);
        c = c.iappend(3, 3 + 1);
        c = c.iappend(4, 6 + 1);
        assertEquals(6, c.getCardinality());
    }

    @Test
    public void testReverseIteratorAdvance() {
        ContainerTestCommon.doTestReverseIteratorAdvance(makeContainer(), containerName());
    }

    @Test
    public void testAddRange() {
        ContainerTestCommon.doTestAddRange(makeContainer(), containerName());
    }

    @Test
    public void testAndRange() {
        ContainerTestCommon.doTestAndRange(makeContainer(), containerName());
    }
}
