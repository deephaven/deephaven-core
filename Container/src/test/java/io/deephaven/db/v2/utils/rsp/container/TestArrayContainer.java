package io.deephaven.db.v2.utils.rsp.container;

import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.function.Supplier;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

public class TestArrayContainer extends TestContainerBase {

    @Test
    public void testConst() {
        ArrayContainer ac1 = new ArrayContainer(5, 15);
        short[] data = {5, 6, 7, 8, 9, 10, 11, 12, 13, 14};
        ArrayContainer ac2 = new ArrayContainer(data);
        assertSameContents(ac1, ac2);
    }

    @Test
    public void testRemove() {
        ArrayContainer ac1 = new ArrayContainer(5, 15);
        ac1.iunset((short) 14);
        ArrayContainer ac2 = new ArrayContainer(5, 14);
        assertSameContents(ac1, ac2);
    }

    @Test
    public void testIandNot() {
        ArrayContainer ac1 = new ArrayContainer(5, 15);
        ArrayContainer ac2 = new ArrayContainer(10, 15);
        Container bc = Container.singleRange(5, 10);
        Container ac3 = ac1.iandNot(bc);
        assertSameContents(ac2, ac3);
    }

    @Test
    public void intersectsArray() {
        Container ac = new ArrayContainer();
        ac = ac.add(1, 10);
        Container ac2 = new ArrayContainer();
        ac2 = ac2.add(5, 25);
        assertTrue(ac.intersects(ac2));
    }

    @Test
    public void orFullToRunContainer() {
        ArrayContainer ac = new ArrayContainer(0, 1 << 12);
        Container half = Container.singleRange(1 << 12, 1 << 16);
        Container result = ac.or(half);
        assertEquals(1 << 16, result.getCardinality());
        if (ImmutableContainer.ENABLED) {
            assertThat(result, instanceOf(SingleRangeContainer.class));
        }
    }

    @Test
    public void orFullToRunContainer2() {
        ArrayContainer ac = new ArrayContainer(0, 1 << 15);
        ArrayContainer half = new ArrayContainer(1 << 15, 1 << 16);
        Container result = ac.or(half);
        assertEquals(1 << 16, result.getCardinality());
        if (ImmutableContainer.ENABLED) {
            assertThat(result, instanceOf(SingleRangeContainer.class));
        }
    }

    @Test
    public void iandBitmap() {
        Container ac = new ArrayContainer();
        ac = ac.add(1, 10);
        Container bc = new BitmapContainer();
        bc = bc.add(5, 25);
        ac.iand(bc);
        assertEquals(5, ac.getCardinality());
        for (int i = 5; i < 10; i++) {
            assertTrue(ac.contains((short) i));
        }
    }

    @Test
    public void iandRun() throws Exception {
        Container ac = new ArrayContainer();
        ac = ac.add(1, 10);
        Container rc = new RunContainer();
        rc = rc.add(5, 25);
        ac = ac.iand(rc);
        assertEquals(5, ac.getCardinality());
        for (int i = 5; i < 10; i++) {
            assertTrue(ac.contains((short) i));
        }
    }

    @Test
    public void addEmptyRange() {
        Container ac = new ArrayContainer();
        ac = ac.add(1, 1);
        assertEquals(0, ac.getCardinality());
    }

    @Test(expected = IllegalArgumentException.class)
    public void addInvalidRange() {
        Container ac = new ArrayContainer();
        ac.add(13, 1);
    }

    @Test
    public void iaddEmptyRange() {
        Container ac = new ArrayContainer();
        ac = ac.iadd(1, 1);
        assertEquals(0, ac.getCardinality());
    }

    @Test(expected = IllegalArgumentException.class)
    public void iaddInvalidRange() {
        Container ac = new ArrayContainer();
        ac.iadd(13, 1);
    }

    @Test
    public void iaddSanityTest() {
        Container ac = new ArrayContainer();
        ac = ac.iadd(10, 20);
        // insert disjoint at end
        ac = ac.iadd(30, 70);
        // insert disjoint between
        ac = ac.iadd(25, 26);
        // insert disjoint at start
        ac = ac.iadd(1, 2);
        // insert overlap at end
        ac = ac.iadd(60, 80);
        // insert overlap between
        ac = ac.iadd(10, 30);
        // insert overlap at start
        ac = ac.iadd(1, 20);
        assertEquals(79, ac.getCardinality());
    }

    @Test(expected = NoSuchElementException.class)
    public void testFirst_Empty() {
        new ArrayContainer().first();
    }

    @Test(expected = NoSuchElementException.class)
    public void testLast_Empty() {
        new ArrayContainer().last();
    }

    @Test
    public void testFirstLast() {
        Container rc = new ArrayContainer();
        final int firstInclusive = 1;
        int lastExclusive = firstInclusive;
        for (int i = 0; i < 1 << 16 - 10; ++i) {
            int newLastExclusive = lastExclusive + 10;
            rc = rc.add(lastExclusive, newLastExclusive);
            assertEquals(firstInclusive, rc.first());
            assertEquals(newLastExclusive - 1, rc.last());
            lastExclusive = newLastExclusive;
        }
    }

    @Test
    public void testContainsBitmapContainer_ExcludeShiftedSet() {
        Container ac = new ArrayContainer().add(0, 10);
        Container subset = new BitmapContainer().add(2, 12);
        assertFalse(ac.contains(subset));
    }

    @Test
    public void testContainsBitmapContainer_NotAlwaysFalse() {
        Container ac = new ArrayContainer().add(0, 10);
        Container subset = new BitmapContainer().add(0, 10);
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsBitmapContainer_ExcludeSuperSet() {
        Container ac = new ArrayContainer().add(0, 10);
        Container superset = new BitmapContainer().add(0, 20);
        assertFalse(ac.contains(superset));
    }

    @Test
    public void testContainsBitmapContainer_ExcludeDisJointSet() {
        Container ac = new ArrayContainer().add(0, 10);
        Container disjoint = new BitmapContainer().add(20, 40);
        assertFalse(ac.contains(disjoint));
        assertFalse(disjoint.contains(ac));
    }

    @Test
    public void testContainsRunContainer_EmptyContainsEmpty() {
        Container ac = new ArrayContainer();
        Container subset = new RunContainer();
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsRunContainer_IncludeProperSubset() {
        Container ac = new ArrayContainer().add(0, 10);
        Container subset = new RunContainer().add(0, 9);
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsRunContainer_IncludeSelf() {
        Container ac = new ArrayContainer().add(0, 10);
        Container subset = new RunContainer().add(0, 10);
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsRunContainer_ExcludeSuperSet() {
        Container ac = new ArrayContainer().add(0, 10);
        Container superset = new RunContainer().add(0, 20);
        assertFalse(ac.contains(superset));
    }

    @Test
    public void testContainsRunContainer_IncludeProperSubsetDifferentStart() {
        Container ac = new ArrayContainer().add(0, 10);
        Container subset = new RunContainer().add(1, 9);
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsRunContainer_ExcludeShiftedSet() {
        Container ac = new ArrayContainer().add(0, 10);
        Container subset = new RunContainer().add(2, 12);
        assertFalse(ac.contains(subset));
    }

    @Test
    public void testContainsRunContainer_ExcludeDisJointSet() {
        Container ac = new ArrayContainer().add(0, 10);
        Container disjoint = new RunContainer().add(20, 40);
        assertFalse(ac.contains(disjoint));
        assertFalse(disjoint.contains(ac));
    }

    @Test
    public void testContainsArrayContainer_EmptyContainsEmpty() {
        Container ac = new ArrayContainer();
        Container subset = new ArrayContainer();
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_IncludeProperSubset() {
        Container ac = new ArrayContainer().add(0, 10);
        Container subset = new ArrayContainer().add(0, 9);
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_IncludeProperSubsetDifferentStart() {
        Container ac = new ArrayContainer().add(0, 10);
        Container subset = new ArrayContainer().add(2, 9);
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_ExcludeShiftedSet() {
        Container ac = new ArrayContainer().add(0, 10);
        Container shifted = new ArrayContainer().add(2, 12);
        assertFalse(ac.contains(shifted));
    }

    @Test
    public void testContainsArrayContainer_IncludeSelf() {
        Container ac = new ArrayContainer().add(0, 10);
        Container subset = new ArrayContainer().add(0, 10);
        assertTrue(ac.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_ExcludeSuperSet() {
        Container ac = new ArrayContainer().add(0, 10);
        Container superset = new ArrayContainer().add(0, 20);
        assertFalse(ac.contains(superset));
    }

    @Test
    public void testContainsArrayContainer_ExcludeDisJointSet() {
        Container ac = new ArrayContainer().add(0, 10);
        Container disjoint = new ArrayContainer().add(20, 40);
        assertFalse(ac.contains(disjoint));
        assertFalse(disjoint.contains(ac));
    }

    @Test
    public void iorNotIncreaseCapacity() {
        Container ac1 = new ArrayContainer();
        Container ac2 = new ArrayContainer();
        ac1.iset((short) 128);
        ac1.iset((short) 256);
        ac2.iset((short) 1024);

        ac1 = ac1.ior(ac2);
        assertTrue(ac1.contains((short) 128));
        assertTrue(ac1.contains((short) 256));
        assertTrue(ac1.contains((short) 1024));
    }

    @Test
    public void iorIncreaseCapacity() {
        Container ac1 = new ArrayContainer();
        Container ac2 = new ArrayContainer();
        ac1.iset((short) 128);
        ac1.iset((short) 256);
        ac1.iset((short) 512);
        ac1.iset((short) 513);
        ac2.iset((short) 1024);

        ac1 = ac1.ior(ac2);
        assertTrue(ac1.contains((short) 128));
        assertTrue(ac1.contains((short) 256));
        assertTrue(ac1.contains((short) 512));
        assertTrue(ac1.contains((short) 513));
        assertTrue(ac1.contains((short) 1024));
    }

    @Test
    public void iorSanityCheck() {
        Container ac = new ArrayContainer().add(0, 10);
        Container disjoint = new ArrayContainer().add(20, 40);
        ac = ac.ior(disjoint);
        assertTrue(ac.contains(disjoint));
    }

    @Test
    public void testIntersectsWithRange() {
        Container container = new ArrayContainer().add(0, 10);
        assertTrue(container.intersects(0, 1));
        assertTrue(container.intersects(0, 101));
        assertTrue(container.intersects(0, lower16Bits(-1)));
        assertFalse(container.intersects(11, lower16Bits(-1)));
    }


    @Test
    public void testIntersectsWithRange2() {
        Container container = new ArrayContainer().add(lower16Bits(-50), lower16Bits(-10));
        assertFalse(container.intersects(0, 1));
        assertTrue(container.intersects(0, lower16Bits(-40)));
        assertFalse(container.intersects(lower16Bits(-100), lower16Bits(-55)));
        assertFalse(container.intersects(lower16Bits(-9), lower16Bits(-1)));
        assertTrue(container.intersects(11, 1 << 16));
    }


    @Test
    public void testIntersectsWithRange3() {
        Container container = new ArrayContainer()
                .iset((short) 1)
                .iset((short) 300)
                .iset((short) 1024);
        assertTrue(container.intersects(0, 300));
        assertTrue(container.intersects(1, 300));
        assertFalse(container.intersects(2, 300));
        assertFalse(container.intersects(2, 299));
        assertTrue(container.intersects(0, lower16Bits(-1)));
        assertFalse(container.intersects(1025, 1 << 16));
    }


    @Test
    public void testContainsRange() {
        Container ac = new ArrayContainer().add(20, 100);
        assertFalse(ac.contains(1, 21));
        assertFalse(ac.contains(1, 19));
        assertTrue(ac.contains(20, 100));
        assertTrue(ac.contains(20, 99));
        assertTrue(ac.contains(21, 100));
        assertFalse(ac.contains(21, 101));
        assertFalse(ac.contains(19, 99));
        assertFalse(ac.contains(190, 9999));
    }

    @Test
    public void testContainsRange2() {
        Container ac = new ArrayContainer()
                .iset((short) 1).iset((short) 10)
                .add(20, 100);
        assertFalse(ac.contains(1, 21));
        assertFalse(ac.contains(1, 20));
        assertTrue(ac.contains(1, 2));
    }

    @Test
    public void testContainsRangeUnsigned() {
        Container ac = new ArrayContainer().add(1 << 15, 1 << 8 | 1 << 15);
        assertTrue(ac.contains(1 << 15, 1 << 8 | 1 << 15));
        assertTrue(ac.contains(1 + (1 << 15), (1 << 8 | 1 << 15) - 1));
        assertFalse(ac.contains(1 + (1 << 15), (1 << 8 | 1 << 15) + 1));
        assertFalse(ac.contains((1 << 15) - 1, (1 << 8 | 1 << 15) - 1));
        assertFalse(ac.contains(0, 1 << 15));
        assertFalse(ac.contains(1 << 8 | 1 << 15 | 1, 1 << 16));
    }

    @Test
    public void testNextValueBeforeStart() {
        ArrayContainer container = new ArrayContainer(new short[] {10, 20, 30});
        assertEquals(10, container.nextValue((short) 5));
    }

    @Test
    public void testNextValue() {
        ArrayContainer container = new ArrayContainer(new short[] {10, 20, 30});
        assertEquals(10, container.nextValue((short) 10));
        assertEquals(20, container.nextValue((short) 11));
        assertEquals(30, container.nextValue((short) 30));
    }

    @Test
    public void testNextValueAfterEnd() {
        ArrayContainer container = new ArrayContainer(new short[] {10, 20, 30});
        assertEquals(-1, container.nextValue((short) 31));
    }

    @Test
    public void testShortRangeIteratorSearchRegression0() {
        Container c = new ArrayContainer();
        c = c.iset((short) 0);
        c = c.add(3, 6);
        assertEquals("array", c.getContainerName());
        SearchRangeIterator it = c.getShortRangeIterator(0);
        assertTrue(it.hasNext());
        it.next();
        assertTrue(it.hasNext());
        it.next();
        final ContainerUtil.TargetComparator comp = (k) -> {
            if (k <= 3) {
                return 1;
            }
            return -1;
        };
        it.search(comp);
        assertEquals(3, it.start());
    }

    public Supplier<Container> makeContainer() {
        return ArrayContainer::new;
    }

    public String containerName() {
        return "array";
    }

    private static int lower16Bits(int x) {
        return ((short) x) & Container.MAX_VALUE;
    }

    @Test
    public void testIaddWhenShared() {
        for (boolean b : new boolean[] {false, true}) {
            Container c = b ? new ArrayContainer() : new ArrayContainer(13);
            c = c.iadd(3, 6 + 1);
            c = c.iadd(11, 12 + 1);
            c = c.iadd(15, 21 + 1);
            assertEquals("array", c.getContainerName());
            c.setCopyOnWrite();
            final Container pre = c.deepCopy();
            final Container ans = c.iadd(7, 14 + 1);
            assertEquals(pre.getCardinality(), c.getCardinality());
            assertEquals(0, c.andNot(pre).getCardinality());
            assertEquals(21 - 3 + 1, ans.getCardinality());
            assertEquals(0, ans.andNot(Container.rangeOfOnes(3, 21 + 1)).getCardinality());
        }
    }

    @Test
    public void testInotWhenSharedContraction() {
        Container c = new ArrayContainer();
        c = c.iadd(3, 6 + 1);
        c = c.iadd(11, 12 + 1);
        c = c.iadd(15, 21 + 1);
        assertEquals("array", c.getContainerName());
        c.setCopyOnWrite();
        final Container pre = c.deepCopy();
        final Container ans = c.inot(10, 12 + 1);
        assertEquals(pre.getCardinality(), c.getCardinality());
        assertEquals(0, c.andNot(pre).getCardinality());
        assertEquals(pre.getCardinality() - 2 + 1, ans.getCardinality());
        Container e = new ArrayContainer();
        e = e.iadd(3, 6 + 1);
        e = e.iset((short) 10);
        e = e.iadd(15, 21 + 1);
        assertEquals(e.getCardinality(), ans.getCardinality());
        assertEquals(0, ans.andNot(e).getCardinality());
    }

    @Test
    public void testInotWhenSharedExpansion() {
        Container c = new ArrayContainer();
        c = c.iadd(3, 6 + 1);
        c = c.iadd(11, 12 + 1);
        c = c.iadd(15, 21 + 1);
        assertEquals("array", c.getContainerName());
        c.setCopyOnWrite();
        final Container pre = c.deepCopy();
        final Container ans = c.inot(7, 14 + 1);
        assertEquals(pre.getCardinality(), c.getCardinality());
        assertEquals(0, c.andNot(pre).getCardinality());
        assertEquals(21 - 3 + 1 - 2, ans.getCardinality());
        assertEquals(0,
                ans.andNot(Container.rangeOfOnes(3, 10 + 1)).andNot(Container.rangeOfOnes(13, 21 + 1))
                        .getCardinality());
    }

    @Test
    public void testIorWhenShared() {
        for (boolean b : new boolean[] {false, true}) {
            Container c = b ? new ArrayContainer(100) : new ArrayContainer(10);
            c = c.iadd(3, 6 + 1);
            c = c.iadd(20, 25 + 1);
            assertEquals("array", c.getContainerName());
            c.setCopyOnWrite();
            Container c2 = new ArrayContainer();
            c2 = c2.iadd(13, 21 + 1);
            final Container pre = c.deepCopy();
            final Container ans = c.ior(c2);
            assertEquals(pre.getCardinality(), c.getCardinality());
            assertEquals(0, pre.andNot(c).getCardinality());
            assertEquals(pre.getCardinality() + 19 - 13 + 1, ans.getCardinality());
            Container e = new ArrayContainer();
            e = e.iadd(3, 6 + 1);
            e = e.iadd(13, 25 + 1);
            assertEquals(e.getCardinality(), ans.getCardinality());
            assertEquals(0, e.andNot(ans).getCardinality());
        }
    }

    @Test
    public void testReverseIteratorAdvanceRegression0() {
        Container c = new ArrayContainer(16);
        c = c.iadd(0xFFFA, Container.MAX_RANGE);
        assertEquals("array", c.getContainerName());
        final ShortAdvanceIterator reverse = c.getReverseShortIterator();
        assertTrue(reverse.advance(0xFFFA));
        assertFalse(reverse.hasNext());
    }

    @Test
    public void testSetWithGuess() {
        Container c = new ArrayContainer();
        c = c.iadd(10, 16);
        c = c.iadd(20, 26);
        assertEquals("array", c.getContainerName());
        final PositionHint hint = new PositionHint();
        c = c.iset((short) 8, hint).iset((short) 9, hint).iset((short) 10, hint).iset((short) 11, hint)
                .iset((short) 12, hint).iset((short) 13, hint).iset((short) 14, hint).iset((short) 15, hint)
                .iset((short) 16, hint).iset((short) 17, hint).iset((short) 18, hint).iset((short) 19, hint)
                .iset((short) 20, hint).iset((short) 21, hint).iset((short) 22, hint).iset((short) 23, hint)
                .iset((short) 24, hint).iset((short) 25, hint).iset((short) 26, hint).iset((short) 27, hint)
                .iset((short) 28, hint).iset((short) 29, hint).iset((short) 30, hint);
        assertEquals(23, c.getCardinality());
        assertTrue(c.contains(8, 23));
    }

    @Test
    public void testUnsetWithHint() {
        Container c = new ArrayContainer();
        c = c.iadd(10, 15);
        c = c.iadd(20, 25);
        c = c.iadd(30, 35);
        final PositionHint hint = new PositionHint();
        c = c.iunset((short) 10, hint).iunset((short) 12, hint).iunset((short) 14, hint).iunset((short) 20, hint)
                .iunset((short) 21, hint).iunset((short) 22, hint).iunset((short) 23, hint).iunset((short) 24, hint)
                .iunset((short) 34, hint).iset((short) 35, hint);
        assertEquals(7, c.getCardinality());
        assertTrue(c.contains(11, 12));
        assertTrue(c.contains(13, 14));
        assertTrue(c.contains(30, 34));
        assertTrue(c.contains(35, 36));
    }

    @Test
    public void testEveryOtherThenFull() {
        Container c = new ArrayContainer();
        for (int i = 1; i < 400; i += 2) {
            c = c.iset((short) i);
        }
        for (int i = 0; i < 400; i += 2) {
            c = c.iset((short) i);
        }
        assertEquals(400, c.getCardinality());
        assertTrue(c.sameContents(new SingleRangeContainer(0, 400)));
    }
}
