/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */

package io.deephaven.db.v2.utils.rsp.container;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;
import static io.deephaven.db.v2.utils.rsp.container.Container.MAX_RANGE;

public class TestBitmapContainer extends TestContainerBase {
    private static BitmapContainer emptyContainer() {
        return new BitmapContainer(new long[1 << 10], 0);
    }

    static BitmapContainer generateContainer(short min, short max, int sample) {
        BitmapContainer bc = new BitmapContainer();
        for (int i = min; i < max; i++) {
            if (i % sample != 0)
                bc.iset((short) i);
        }
        return bc;
    }

    @Test
    public void testXOR() {
        Container bc = Container.singleRange(100, 10000);
        BitmapContainer bc2 = new BitmapContainer();
        BitmapContainer bc3 = new BitmapContainer();

        for (int i = 100; i < 10000; ++i) {
            if ((i % 2) == 0)
                bc2 = (BitmapContainer) bc2.iset((short) i);
            else
                bc3 = (BitmapContainer) bc3.iset((short) i);
        }
        bc = bc.ixor(bc2);
        assertTrue(bc.ixor(bc3).getCardinality() == 0);
    }

    @Test
    public void testANDNOT() {
        Container bc = Container.singleRange(100, 10000);
        Container bc2 = new BitmapContainer();
        Container bc3 = new BitmapContainer();

        for (int i = 100; i < 10000; ++i) {
            if ((i % 2) == 0)
                bc2 = bc2.iset((short) i);
            else
                bc3 = bc3.iset((short) i);
        }
        RunContainer rc = new RunContainer();
        rc.iadd(0, 1 << 16);
        bc = bc.iand(rc);
        bc = bc.iandNot(bc2);
        assertTrue(bc.sameContents(bc3));
        assertTrue(bc.iandNot(bc3).getCardinality() == 0);
    }


    @Test
    public void testAND() {
        Container bc = Container.singleRange(100, 10000);
        BitmapContainer bc2 = new BitmapContainer();
        BitmapContainer bc3 = new BitmapContainer();

        for (int i = 100; i < 10000; ++i) {
            if ((i % 2) == 0)
                bc2 = (BitmapContainer) bc2.iset((short) i);
            else
                bc3 = (BitmapContainer) bc3.iset((short) i);
        }
        bc = bc.iand(bc2);
        assertTrue(bc.sameContents(bc2));
        assertTrue(bc.iand(bc3).getCardinality() == 0);
    }


    @Test
    public void testOR() {
        Container bc = Container.singleRange(100, 10000);
        Container bc2 = new BitmapContainer();
        Container bc3 = new BitmapContainer();

        for (int i = 100; i < 10000; ++i) {
            if ((i % 2) == 0)
                bc2 = bc2.iset((short) i);
            else
                bc3 = bc3.iset((short) i);
        }
        bc2 = bc2.ior(bc3);
        assertTrue(bc.sameContents(bc2));
        bc2 = bc2.ior(bc);
        assertTrue(bc.sameContents(bc2));
        Container rc = new RunContainer();
        rc = rc.iadd(0, 1 << 16);
        assertEquals(0, bc.iandNot(rc).getCardinality());
    }

    @Test
    public void runConstructorForBitmap() {
        System.out.println("runConstructorForBitmap");
        for (int start = 0; start <= (1 << 16); start += 4096) {
            for (int end = start; end <= (1 << 16); end += 4096) {
                BitmapContainer bc = BitmapContainer.singleRange(start, end);
                Container bc2 = new BitmapContainer();
                Container bc3 = bc2.add(start, end);
                bc2 = bc2.iadd(start, end);
                assertEquals(bc.getCardinality(), end - start);
                assertEquals(bc2.getCardinality(), end - start);
                assertSameContents(bc, bc2);
                assertSameContents(bc, bc3);
                assertEquals(0, bc2.remove(start, end).getCardinality());
                assertEquals(bc2.getCardinality(), end - start);
                assertEquals(0, bc2.not(start, end).getCardinality());
            }
        }
    }

    @Test
    public void runConstructorForBitmap2() {
        System.out.println("runConstructorForBitmap2");
        for (int start = 0; start <= (1 << 16); start += 63) {
            for (int end = start; end <= (1 << 16); end += 63) {
                BitmapContainer bc = BitmapContainer.singleRange(start, end);
                BitmapContainer bc2 = new BitmapContainer();
                BitmapContainer bc3 = (BitmapContainer) bc2.add(start, end);
                bc2 = (BitmapContainer) bc2.iadd(start, end);
                assertEquals(bc.getCardinality(), end - start);
                assertEquals(bc2.getCardinality(), end - start);
                assertSameContents(bc, bc2);
                assertSameContents(bc, bc3);
                assertEquals(0, bc2.remove(start, end).getCardinality());
                assertEquals(bc2.getCardinality(), end - start);
                assertEquals(0, bc2.not(start, end).getCardinality());
            }
        }
    }

    @Test
    public void testRangeCardinality() {
        BitmapContainer bc = generateContainer((short) 100, (short) 10000, 5);
        bc = (BitmapContainer) bc.add(200, 2000);
        assertEquals(8280, bc.cardinality);
    }

    @Test
    public void testRangeCardinality2() {
        BitmapContainer bc = generateContainer((short) 100, (short) 10000, 5);
        bc.iadd(200, 2000);
        assertEquals(8280, bc.cardinality);
    }

    @Test
    public void testRangeCardinality3() {
        BitmapContainer bc = generateContainer((short) 100, (short) 10000, 5);
        RunContainer rc = new RunContainer(new short[] {7, 300, 400, 900, 1400, 2200}, 3);
        bc.ior(rc);
        assertEquals(8677, bc.cardinality);
    }

    @Test
    public void testRangeCardinality4() {
        BitmapContainer bc = generateContainer((short) 100, (short) 10000, 5);
        RunContainer rc = new RunContainer(new short[] {7, 300, 400, 900, 1400, 2200}, 3);
        bc = (BitmapContainer) bc.andNot(rc);
        assertEquals(5274, bc.cardinality);
    }

    @Test
    public void testRangeCardinality5() {
        BitmapContainer bc = generateContainer((short) 100, (short) 10000, 5);
        RunContainer rc = new RunContainer(new short[] {7, 300, 400, 900, 1400, 2200}, 3);
        bc.iandNot(rc);
        assertEquals(5274, bc.cardinality);
    }

    @Test
    public void testRangeCardinality6() {
        BitmapContainer bc = generateContainer((short) 100, (short) 10000, 5);
        RunContainer rc = new RunContainer(new short[] {7, 300, 400, 900, 1400, 5200}, 3);
        bc = (BitmapContainer) bc.iand(rc);
        assertEquals(5046, bc.cardinality);
    }

    @Test
    public void testRangeCardinality7() {
        BitmapContainer bc = generateContainer((short) 100, (short) 10000, 5);
        RunContainer rc = new RunContainer(new short[] {7, 300, 400, 900, 1400, 2200}, 3);
        bc.ixor(rc);
        assertEquals(6031, bc.cardinality);
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testNextTooLarge() {
        emptyContainer().nextSetBit(1 << 16);
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testNextTooSmall() {
        emptyContainer().nextSetBit(-1);
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testPreviousTooLarge() {
        emptyContainer().prevSetBit(1 << 16);
    }


    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testPreviousTooSmall() {
        emptyContainer().prevSetBit(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void addInvalidRange() {
        Container bc = new BitmapContainer();
        bc.add(13, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void iaddInvalidRange() {
        Container bc = new BitmapContainer();
        bc.iadd(13, 1);
    }

    @Test
    public void iorRun() {
        Container bc = new BitmapContainer();
        bc = bc.add(1, 5);
        Container rc = new RunContainer();
        rc = rc.add(4, 10);
        bc.ior(rc);
        assertEquals(9, bc.getCardinality());
        for (int i = 1; i < 10; i++) {
            assertTrue(bc.contains((short) i));
        }
    }

    @Test
    public void orFullToRunContainer() {
        BitmapContainer bc = BitmapContainer.singleRange(0, 1 << 15);
        BitmapContainer half = BitmapContainer.singleRange(1 << 15, 1 << 16);
        Container result = bc.or(half);
        assertEquals(1 << 16, result.getCardinality());
        if (ImmutableContainer.ENABLED) {
            assertThat(result, instanceOf(SingleRangeContainer.class));
        }
    }

    @Test
    public void orFullToRunContainer2() {
        BitmapContainer bc = BitmapContainer.singleRange(0, 1 << 15);
        ArrayContainer half = new ArrayContainer(1 << 15, 1 << 16);
        Container result = bc.or(half);
        assertEquals(1 << 16, result.getCardinality());
        if (ImmutableContainer.ENABLED) {
            assertThat(result, instanceOf(SingleRangeContainer.class));
        }
    }

    @Test
    public void orFullToRunContainer3() {
        BitmapContainer bc = BitmapContainer.singleRange(0, 1 << 15);
        BitmapContainer bc2 = BitmapContainer.singleRange(3210, 1 << 16);
        Container result = bc.or(bc2);
        Container iresult = bc.ior(bc2);
        assertEquals(1 << 16, result.getCardinality());
        assertEquals(1 << 16, iresult.getCardinality());
        if (ImmutableContainer.ENABLED) {
            assertThat(result, instanceOf(SingleRangeContainer.class));
            assertThat(iresult, instanceOf(SingleRangeContainer.class));
        }
    }

    @Test
    public void orFullToRunContainer4() {
        BitmapContainer bc = BitmapContainer.singleRange(0, 1 << 15);
        Container bc2 = Container.rangeOfOnes(3210, 1 << 16);
        Container iresult = bc.ior(bc2);
        assertEquals(1 << 16, iresult.getCardinality());
        if (ImmutableContainer.ENABLED) {
            assertThat(iresult, instanceOf(SingleRangeContainer.class));
        }
    }

    @Test
    public void iremoveEmptyRange() {
        Container bc = new BitmapContainer();
        bc = bc.iremove(1, 1);
        assertEquals(0, bc.getCardinality());
    }

    @Test(expected = IllegalArgumentException.class)
    public void iremoveInvalidRange() {
        Container ac = new BitmapContainer();
        ac.iremove(13, 1);
    }

    @Test
    public void iremove() {
        Container bc = new BitmapContainer();
        bc = bc.add(1, 10);
        bc = bc.iremove(5, 10);
        assertEquals(4, bc.getCardinality());
        for (int i = 1; i < 5; i++) {
            assertTrue(bc.contains((short) i));
        }
    }

    @Test
    public void iremove2() {
        Container bc = new BitmapContainer();
        bc = bc.add(1, 8092);
        bc = bc.iremove(1, 10);
        assertEquals(8082, bc.getCardinality());
        for (int i = 10; i < 8092; i++) {
            assertTrue(bc.contains((short) i));
        }
    }

    @Test
    public void ixorRun() {
        Container bc = new BitmapContainer();
        bc = bc.add(1, 10);
        Container rc = new RunContainer();
        rc = rc.add(5, 15);
        bc = bc.ixor(rc);
        assertEquals(9, bc.getCardinality());
        for (int i = 1; i < 5; i++) {
            assertTrue(bc.contains((short) i));
        }
        for (int i = 10; i < 15; i++) {
            assertTrue(bc.contains((short) i));
        }
    }

    @Test
    public void ixorRun2() {
        Container bc = new BitmapContainer();
        bc = bc.add(1, 8092);
        Container rc = new RunContainer();
        rc = rc.add(1, 10);
        bc = bc.ixor(rc);
        assertEquals(8082, bc.getCardinality());
        for (int i = 10; i < 8092; i++) {
            assertTrue(bc.contains((short) i));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void selectInvalidPosition() {
        Container bc = new BitmapContainer();
        bc = bc.add(1, 13);
        bc.select(100);
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeInvalidRange() {
        Container ac = new BitmapContainer();
        ac.remove(13, 1);
    }

    @Test
    public void remove() {
        Container bc = new BitmapContainer();
        bc = bc.add(1, 8092);
        bc = bc.remove(1, 10);
        assertEquals(8082, bc.getCardinality());
        for (int i = 10; i < 8092; i++) {
            assertTrue(bc.contains((short) i));
        }
    }

    @Test
    public void iandRun() {
        Container bc = new BitmapContainer();
        bc = bc.add(0, 8092);
        Container rc = new RunContainer();
        rc = rc.add(1, 10);
        bc = bc.iand(rc);
        assertEquals(9, bc.getCardinality());
        for (int i = 1; i < 10; i++) {
            assertTrue(bc.contains((short) i));
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void testFirst_Empty() {
        new BitmapContainer().first();
    }

    @Test(expected = NoSuchElementException.class)
    public void testLast_Empty() {
        new BitmapContainer().last();
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
    public void testContainsBitmapContainer_EmptyContainsEmpty() {
        Container bc = new BitmapContainer();
        Container subset = new BitmapContainer();
        assertTrue(bc.contains(subset));
    }

    @Test
    public void testContainsBitmapContainer_IncludeProperSubset() {
        Container bc = new BitmapContainer().add(0, 10);
        Container subset = new BitmapContainer().add(0, 9);
        assertTrue(bc.contains(subset));
    }

    @Test
    public void testContainsBitmapContainer_IncludeSelf() {
        Container bc = new BitmapContainer().add(0, 10);
        Container subset = new BitmapContainer().add(0, 10);
        assertTrue(bc.contains(subset));
    }

    @Test
    public void testContainsBitmapContainer_ExcludeSuperSet() {
        Container bc = new BitmapContainer().add(0, 10);
        Container superset = new BitmapContainer().add(0, 20);
        assertFalse(bc.contains(superset));
    }

    @Test
    public void testContainsBitmapContainer_IncludeProperSubsetDifferentStart() {
        Container bc = new BitmapContainer().add(0, 10);
        Container subset = new RunContainer().add(2, 9);
        assertTrue(bc.contains(subset));
    }

    @Test
    public void testContainsBitmapContainer_ExcludeShiftedSet() {
        Container bc = new BitmapContainer().add(0, 10);
        Container shifted = new BitmapContainer().add(2, 12);
        assertFalse(bc.contains(shifted));
    }

    @Test
    public void testContainsBitmapContainer_ExcludeDisJointSet() {
        Container bc = new BitmapContainer().add(0, 10);
        Container disjoint = new BitmapContainer().add(20, 40);
        assertFalse(bc.contains(disjoint));
        assertFalse(disjoint.contains(bc));
    }

    @Test
    public void testContainsRunContainer_EmptyContainsEmpty() {
        Container bc = new BitmapContainer();
        Container subset = new BitmapContainer();
        assertTrue(bc.contains(subset));
    }

    @Test
    public void testContainsRunContainer_IncludeProperSubset() {
        Container bc = new BitmapContainer().add(0, 10);
        Container subset = new RunContainer().add(0, 9);
        assertTrue(bc.contains(subset));
    }

    @Test
    public void testContainsRunContainer_IncludeSelf() {
        Container bc = new BitmapContainer().add(0, 10);
        Container subset = new RunContainer().add(0, 10);
        assertTrue(bc.contains(subset));
    }

    @Test
    public void testContainsRunContainer_ExcludeSuperSet() {
        Container bc = new BitmapContainer().add(0, 10);
        Container superset = new RunContainer().add(0, 20);
        assertFalse(bc.contains(superset));
    }

    @Test
    public void testContainsRunContainer_IncludeProperSubsetDifferentStart() {
        Container bc = new BitmapContainer().add(0, 10);
        Container subset = new RunContainer().add(2, 9);
        assertTrue(bc.contains(subset));
    }

    @Test
    public void testContainsRunContainer_ExcludeShiftedSet() {
        Container bc = new BitmapContainer().add(0, 10);
        Container shifted = new RunContainer().add(2, 12);
        assertFalse(bc.contains(shifted));
    }

    @Test
    public void testContainsRunContainer_ExcludeDisJointSet() {
        Container bc = new BitmapContainer().add(0, 10);
        Container disjoint = new RunContainer().add(20, 40);
        assertFalse(bc.contains(disjoint));
        assertFalse(disjoint.contains(bc));
    }

    @Test
    public void testContainsArrayContainer_EmptyContainsEmpty() {
        Container bc = new BitmapContainer();
        Container subset = new ArrayContainer();
        assertTrue(bc.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_IncludeProperSubset() {
        Container bc = new BitmapContainer().add(0, 10);
        Container subset = new ArrayContainer().add(0, 9);
        assertTrue(bc.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_IncludeSelf() {
        Container bc = new BitmapContainer().add(0, 10);
        Container subset = new ArrayContainer().add(0, 10);
        assertTrue(bc.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_ExcludeSuperSet() {
        Container bc = new BitmapContainer().add(0, 10);
        Container superset = new ArrayContainer().add(0, 20);
        assertFalse(bc.contains(superset));
    }

    @Test
    public void testContainsArrayContainer_IncludeProperSubsetDifferentStart() {
        Container bc = new BitmapContainer().add(0, 10);
        Container subset = new ArrayContainer().add(2, 9);
        assertTrue(bc.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_ExcludeShiftedSet() {
        Container bc = new BitmapContainer().add(0, 10);
        Container shifted = new ArrayContainer().add(2, 12);
        assertFalse(bc.contains(shifted));
    }

    @Test
    public void testContainsArrayContainer_ExcludeDisJointSet() {
        Container bc = new BitmapContainer().add(0, 10);
        Container disjoint = new ArrayContainer().add(20, 40);
        assertFalse(bc.contains(disjoint));
        assertFalse(disjoint.contains(bc));
    }

    @Test
    public void testIntersectsWithRange() {
        Container container = new BitmapContainer().add(0, 10);
        assertTrue(container.intersects(0, 1));
        assertTrue(container.intersects(0, 101));
        assertTrue(container.intersects(0, 1 << 16));
        assertFalse(container.intersects(11, lower16Bits(-1)));
    }


    @Test
    public void testIntersectsWithRangeHitScan() {
        Container container = new BitmapContainer().add(0, 10)
                .add(500, 512).add(lower16Bits(-50), lower16Bits(-10));
        assertTrue(container.intersects(0, 1));
        assertTrue(container.intersects(0, 101));
        assertTrue(container.intersects(0, 1 << 16));
        assertTrue(container.intersects(11, 1 << 16));
        assertTrue(container.intersects(501, 511));
    }


    @Test
    public void testIntersectsWithRangeUnsigned() {
        Container container = new BitmapContainer().add(lower16Bits(-50), lower16Bits(-10));
        assertFalse(container.intersects(0, 1));
        assertTrue(container.intersects(0, lower16Bits(-40)));
        assertFalse(container.intersects(lower16Bits(-100), lower16Bits(-55)));
        assertFalse(container.intersects(lower16Bits(-9), lower16Bits(-1)));
        assertTrue(container.intersects(11, (short) -1));
    }

    @Test
    public void testIntersectsAtEndWord() {
        Container container = new BitmapContainer().add(lower16Bits(-500), lower16Bits(-10));
        assertTrue(container.intersects(lower16Bits(-50), lower16Bits(-10)));
        assertTrue(container.intersects(lower16Bits(-400), lower16Bits(-11)));
        assertTrue(container.intersects(lower16Bits(-11), lower16Bits(-1)));
        assertFalse(container.intersects(lower16Bits(-10), lower16Bits(-1)));
    }


    @Test
    public void testIntersectsAtEndWord2() {
        Container container = new BitmapContainer().add(lower16Bits(500), lower16Bits(-500));
        assertTrue(container.intersects(lower16Bits(-650), lower16Bits(-500)));
        assertTrue(container.intersects(lower16Bits(-501), lower16Bits(-1)));
        assertFalse(container.intersects(lower16Bits(-500), lower16Bits(-1)));
        assertFalse(container.intersects(lower16Bits(-499), 1 << 16));
    }

    @Test
    public void testContainsRangeSingleWord() {
        long[] bitmap = oddBits();
        bitmap[10] = -1L;
        int cardinality = 32 + (1 << 15);
        BitmapContainer container = new BitmapContainer(bitmap, cardinality);
        assertTrue(container.contains(0, 1));
        assertTrue(container.contains(64 * 10, 64 * 11));
        assertFalse(container.contains(64 * 10, 2 + 64 * 11));
        assertTrue(container.contains(1 + 64 * 10, (64 * 11) - 1));
    }

    @Test
    public void testContainsRangeMultiWord() {
        long[] bitmap = oddBits();
        bitmap[10] = -1L;
        bitmap[11] = -1L;
        bitmap[12] |= ((1L << 32) - 1);
        int cardinality = 32 + 32 + 16 + (1 << 15);
        BitmapContainer container = new BitmapContainer(bitmap, cardinality);
        assertTrue(container.contains(0, 1));
        assertFalse(container.contains(64 * 10, (64 * 13) - 30));
        assertTrue(container.contains(64 * 10, (64 * 13) - 31));
        assertTrue(container.contains(1 + 64 * 10, (64 * 13) - 32));
        assertTrue(container.contains(64 * 10, 64 * 12));
        assertFalse(container.contains(64 * 10, 2 + 64 * 13));
    }


    @Test
    public void testContainsRangeSubWord() {
        long[] bitmap = oddBits();
        bitmap[bitmap.length - 1] = ~((1L << 63) | 1L);
        int cardinality = 32 + 32 + 16 + (1 << 15);
        BitmapContainer container = new BitmapContainer(bitmap, cardinality);
        assertFalse(container.contains(64 * 1023, 64 * 1024));
        assertFalse(container.contains(64 * 1023, 64 * 1024 - 1));
        assertTrue(container.contains(1 + 64 * 1023, 64 * 1024 - 1));
        assertTrue(container.contains(1 + 64 * 1023, 64 * 1024 - 2));
        assertFalse(container.contains(64 * 1023, 64 * 1023 + 2));
        assertTrue(container.contains(64 * 1023 + 1, 64 * 1023 + 2));
    }

    @Test
    public void testNextSetBit() {
        BitmapContainer container = new BitmapContainer(oddBits(), 1 << 15);
        assertEquals(0, container.nextSetBit(0));
        assertEquals(2, container.nextSetBit(1));
        assertEquals(2, container.nextSetBit(2));
        assertEquals(4, container.nextSetBit(3));
    }

    @Test
    public void testNextSetBitAfterEnd() {
        BitmapContainer container = new BitmapContainer(oddBits(), 1 << 15);
        container.bitmap[1023] = 0L;
        container.cardinality -= 32;
        assertEquals(-1, container.nextSetBit((64 * 1023) + 5));
    }

    @Test
    public void testNextSetBitBeforeStart() {
        BitmapContainer container = new BitmapContainer(oddBits(), 1 << 15);
        container.bitmap[0] = 0L;
        container.cardinality -= 32;
        assertEquals(64, container.nextSetBit(1));
    }

    @Test
    public void testFindRangesWithMaxRegression() {
        Container c = new BitmapContainer();
        c = c.iadd(5, 132);
        assertEquals("bitmap", c.getContainerName());
        final ArrayList<Integer> vs = new ArrayList<>();
        RangeConsumer rc = (int begin, int end) -> {
            for (int i = begin; i < end; ++i) {
                vs.add(i);
            }
        };
        final boolean r = c.findRanges(rc, new RangeIterator.Single(7, 100), 3);
        assertTrue(r);
        assertEquals(vs.size(), 2);
        assertTrue(vs.contains(2));
        assertTrue(vs.contains(3));
    }

    @Test
    public void testIsAllOnes() {
        Container c = new BitmapContainer();
        c = c.iadd(0, (1 << 16) - 1);
        if (ImmutableContainer.ENABLED) {
            assertEquals("singlerange", c.getContainerName());
        }
        assertFalse(c.isAllOnes());
        c = c.iadd((1 << 16) - 1, 1 << 16);
        if (ImmutableContainer.ENABLED) {
            assertEquals("singlerange", c.getContainerName());
        }
        assertTrue(c.isAllOnes());
    }

    private static long[] oddBits() {
        long[] bitmap = new long[1 << 10];
        Arrays.fill(bitmap, 0x5555555555555555L);
        return bitmap;
    }

    @Test
    public void testRangeIteratorRegression() {
        Container c = new BitmapContainer();
        final int end = Container.MAX_VALUE + 1;
        final int start = end - 66;
        c = c.iadd(start, end);
        assertEquals("bitmap", c.getContainerName());
        final RangeIterator it = c.getShortRangeIterator(0);
        assertTrue(it.hasNext());
        it.next();
        assertEquals(start, it.start());
        assertEquals(end, it.end());
        assertFalse(it.hasNext());
    }

    @Test
    public void testSelectBitmapContainer() {
        Container c = new BitmapContainer();
        final int max = Container.MAX_VALUE + 1;
        for (int i = 0; i < max; i += 2) {
            c = c.iset((short) i);
        }
        final int card = max / 2;
        assertEquals(card, c.getCardinality());
        assertEquals("bitmap", c.getContainerName());
        final int len = ArrayContainer.DEFAULT_MAX_SIZE + 1;
        for (int sr = 0; sr < card - len; ++sr) {
            final String m = "sr==" + sr;
            final int er = sr + len;
            final String m2 = m + " && er==" + er;
            final Container sc = c.select(sr, er);
            assertEquals(m2, er - sr, sc.getCardinality());
            assertEquals(m2, ContainerUtil.toIntUnsigned(c.select(sr)), sc.first());
            assertEquals(m2, ContainerUtil.toIntUnsigned(c.select(er - 1)), sc.last());
        }
    }

    @Test
    public void testSearchRegression0() {
        Container c = new BitmapContainer();
        for (int i = 0; i < 5000; ++i) {
            c = c.iset((short) (i * 2));
        }
        c = c.add((short) 10069, (short) (10070 + 1));
        assertEquals("bitmap", c.getContainerName());
        final SearchRangeIterator it = c.getShortRangeIterator(0);
        final ContainerUtil.TargetComparator comp = (final int v) -> Integer.compare(10070, v);
        final boolean searchResult = it.search(comp);
        assertTrue(searchResult);
        assertFalse(it.hasNext());
    }

    @Test
    public void testReverseIteratorAdvanceRegression0() {
        Container c = new BitmapContainer();
        for (int i = 0; i < 5000; ++i) {
            c = c.iset((short) (i * 2));
        }
        assertEquals("bitmap", c.getContainerName());
        final ShortAdvanceIterator it = c.getReverseShortIterator();
        assertTrue(it.advance(4));
        assertEquals(4, it.currAsInt());
        assertTrue(it.hasNext());
        assertEquals(2, it.nextAsInt());
    }

    @Test
    public void testContainsRange() {
        Container bc = new BitmapContainer();
        bc = bc.add(65534, 65536);
        assertEquals("bitmap", bc.getContainerName());
        assertTrue(bc.contains(65534, 65536));
        assertFalse(bc.contains(65533, 65536));
        bc = new BitmapContainer();
        bc = bc.add(0, 65);
        assertEquals("bitmap", bc.getContainerName());
        assertTrue(bc.contains(0, 65));
        bc = new BitmapContainer();
        bc = bc.add(65525, 65535);
        assertEquals("bitmap", bc.getContainerName());
        assertTrue(bc.contains(65534, 65535));
        final int min = 64 - 2;
        final int max = 128 + 2;
        for (int first = min; first <= max; ++first) {
            final String m = "first==" + first;
            for (int last = first; last <= max; ++last) {
                final String m2 = m + " && last==" + last;
                bc = new BitmapContainer();
                bc = bc.add(first, last + 1);
                assertEquals(m2, "bitmap", bc.getContainerName());
                assertTrue(m2, bc.contains(first, last + 1));
                assertFalse(m2, bc.contains(first - 1, last + 1));
                assertFalse(m2, bc.contains(first, last + 2));
            }
        }
    }

    @Test
    public void testValuesInRangeIter() {
        Container bc = new BitmapContainer();
        bc = bc.iadd(67, 70);
        BitmapContainer.ValuesInRangeIter it =
                new BitmapContainer.ValuesInRangeIter(((BitmapContainer) bc).bitmap, 68, 2001);
        assertTrue(it.hasNext());
        assertEquals(68, it.next());
        assertTrue(it.hasNext());
        assertEquals(69, it.next());
        assertFalse(it.hasNext());

        bc = bc.iadd(1099, 3507);
        it = new BitmapContainer.ValuesInRangeIter(((BitmapContainer) bc).bitmap, 68, 2001);
        assertTrue(it.hasNext());
        assertEquals((short) 68, it.next());
        assertTrue(it.hasNext());
        assertEquals((short) 69, it.next());
        for (int i = 1099; i < 2001; ++i) {
            final String m = "i==" + i;
            assertTrue(m, it.hasNext());
            assertEquals(m, (short) i, it.next());
        }
        assertFalse(it.hasNext());
    }

    @Test
    public void testValuesInRangeIterRegression0() {
        Container bc = new BitmapContainer();
        bc = bc.add(10, 11);
        bc = bc.add(20, 21);
        bc = bc.add(30, 31);
        bc = bc.add(40, 41);
        bc = bc.add(50, 51);
        bc = bc.add(32767, 32769);
        bc = bc.add(65535, 65536);
        final BitmapContainer.ValuesInRangeContext ctx =
                new BitmapContainer.ValuesInRangeContext(29566, 44970);
        final long[] bitmap = ((BitmapContainer) bc).bitmap;
        BitmapContainer.ValuesInRangeIter it =
                new BitmapContainer.ValuesInRangeIter(bitmap, ctx);
        assertEquals(2, ctx.cardinalityInRange(bitmap));
        assertTrue(it.hasNext());
        assertEquals((short) 32767, it.next());
        assertTrue(it.hasNext());
        assertEquals((short) 32768, it.next());
        assertFalse(it.hasNext());
    }

    @Test
    public void testValuesInRangeIterRegression1() {
        Container bc = new BitmapContainer();
        final BitmapContainer.ValuesInRangeContext ctx =
                new BitmapContainer.ValuesInRangeContext(0, 65535);
        final long[] bitmap = ((BitmapContainer) bc).bitmap;
        final BitmapContainer.ValuesInRangeIter it =
                new BitmapContainer.ValuesInRangeIter(bitmap, ctx);
        assertFalse(it.hasNext());
    }

    @Test
    public void testvaluesInRangeIterRandom() {
        final Random rand = new Random(1);
        final int bitsPerWord = Long.BYTES * 8;
        final int runs = 100;
        for (int run = 0; run < runs; ++run) {
            Container bc = new BitmapContainer();
            for (int i = 0; i < BitmapContainer.BITMAP_CAPACITY; ++i) {
                if (rand.nextBoolean()) {
                    // we'll have around half zero words.
                    continue;
                }
                for (int j = 0; j < bitsPerWord; ++j) {
                    if (rand.nextBoolean()) {
                        // we'll have around half the bits set in a non-zero word.
                        continue;
                    }
                    final int v = i * bitsPerWord + j;
                    bc = bc.iset((short) v);
                }
            }
            final long[] bitmap = ((BitmapContainer) bc).bitmap;
            // we finished populating bc, now we roll.
            final int nranges = 600;
            for (int range = 0; range < nranges; ++range) {
                final int start = rand.nextInt(65535);
                final int end = start + rand.nextInt(MAX_RANGE - 1 - start) + 1;
                final Container onesInRange = Container.singleRange(start, end);
                final Container bcRestricted = bc.and(onesInRange);
                final ShortIterator sit = bcRestricted.getShortIterator();
                final BitmapContainer.ValuesInRangeIter vit = new BitmapContainer.ValuesInRangeIter(bitmap, start, end);
                while (sit.hasNext()) {
                    assertTrue(vit.hasNext());
                    assertEquals(sit.next(), vit.next());
                }
                assertFalse(vit.hasNext());
            }
        }
    }

    public Supplier<Container> makeContainer() {
        return BitmapContainer::new;
    }

    public String containerName() {
        return "bitmap";
    }

    private static int lower16Bits(int x) {
        return ((short) x) & Container.MAX_VALUE;
    }
}
