package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import java.util.*;
import java.util.function.Supplier;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;
import static io.deephaven.engine.rowset.impl.rsp.container.ArrayContainer.DEFAULT_MAX_SIZE;
import static io.deephaven.engine.rowset.impl.rsp.container.Container.MAX_VALUE;

public class TestRunContainer extends TestContainerBase {

    public static Container fillMeUp(Container c, int[] values) {
        if (values.length == 0) {
            throw new RuntimeException("You are trying to create an empty bitmap! ");
        }
        for (int k = 0; k < values.length; ++k) {
            c = c.iset((short) values[k]);
        }
        if (c.getCardinality() != values.length) {
            throw new RuntimeException("add failure");
        }
        return c;
    }

    /**
     * generates randomly N distinct integers from 0 to Max.
     */
    static int[] generateUniformHash(Random rand, int N, int Max) {

        if (N > Max) {
            throw new RuntimeException("not possible");
        }
        if (N > Max / 2) {
            return negate(generateUniformHash(rand, Max - N, Max), Max);
        }
        int[] ans = new int[N];
        HashSet<Integer> s = new HashSet<Integer>();
        while (s.size() < N) {
            s.add(new Integer(rand.nextInt(Max)));
        }
        Iterator<Integer> i = s.iterator();
        for (int k = 0; k < N; ++k) {
            ans[k] = i.next().intValue();
        }
        Arrays.sort(ans);
        return ans;
    }

    private static void getSetOfContainers(ArrayList<Container> set,
            ArrayList<Container> setb) {
        Container r1 = new RunContainer();
        r1 = r1.iadd(0, (1 << 16));
        Container b1 = new ArrayContainer();
        b1 = b1.iadd(0, 1 << 16);
        assertTrue(r1.sameContents(b1));

        set.add(r1);
        setb.add(b1);

        Container r2 = new RunContainer();
        r2 = r2.iadd(0, 4096);
        Container b2 = new ArrayContainer();
        b2 = b2.iadd(0, 4096);
        set.add(r2);
        setb.add(b2);
        assertTrue(r2.sameContents(b2));

        Container r3 = new RunContainer();
        Container b3 = new ArrayContainer();

        // mayhaps some of the 655536s were intended to be 65536s?? And later...
        for (int k = 0; k < 655536; k += 2) {
            r3 = r3.iset((short) k);
            b3 = b3.iset((short) k);
        }
        assertTrue(r3.sameContents(b3));
        set.add(r3);
        setb.add(b3);

        Container r4 = new RunContainer();
        Container b4 = new ArrayContainer();
        for (int k = 0; k < 655536; k += 256) {
            r4 = r4.iset((short) k);
            b4 = b4.iset((short) k);
        }
        assertTrue(r4.sameContents(b4));
        set.add(r4);
        setb.add(b4);

        Container r5 = new RunContainer();
        Container b5 = new ArrayContainer();
        for (int k = 0; k + 4096 < 65536; k += 4096) {
            r5 = r5.iadd(k, k + 256);
            b5 = b5.iadd(k, k + 256);
        }
        assertTrue(r5.sameContents(b5));
        set.add(r5);
        setb.add(b5);

        Container r6 = new RunContainer();
        Container b6 = new ArrayContainer();
        for (int k = 0; k + 1 < 65536; k += 7) {
            r6 = r6.iadd(k, k + 1);
            b6 = b6.iadd(k, k + 1);
        }
        assertTrue(r6.sameContents(b6));
        set.add(r6);
        setb.add(b6);

        Container r7 = new RunContainer();
        Container b7 = new ArrayContainer();
        for (int k = 0; k + 1 < 65536; k += 11) {
            r7 = r7.iadd(k, k + 1);
            b7 = b7.iadd(k, k + 1);
        }
        assertTrue(r7.sameContents(b7));
        set.add(r7);
        setb.add(b7);

    }

    /**
     * output all integers from the range [0,Max) that are not in the array
     */
    static int[] negate(int[] x, int Max) {
        int[] ans = new int[Max - x.length];
        int i = 0;
        int c = 0;
        for (int j = 0; j < x.length; ++j) {
            int v = x[j];
            for (; i < v; ++i) {
                ans[c++] = i;
            }
            ++i;
        }
        while (c < ans.length) {
            ans[c++] = i++;
        }
        return ans;
    }

    @Test
    public void addOutOfOrder() {
        RunContainer container = new RunContainer();
        container.iset((short) 0);
        container.iset((short) 2);
        container.iset((short) 55);
        container.iset((short) 1);
        assertEquals(4, container.getCardinality());
    }

    @Test
    public void addRange() {
        for (int i = 0; i < 100; ++i) {
            for (int j = 0; j < 100; ++j) {
                for (int k = 0; k < 50; ++k) {
                    BitSet bs = new BitSet();
                    Container container = new RunContainer();
                    for (int p = 0; p < i; ++p) {
                        container = container.iset((short) p);
                        bs.set(p);
                    }
                    for (int p = 0; p < j; ++p) {
                        container = container.iset((short) (99 - p));
                        bs.set(99 - p);
                    }
                    container = container.add(49 - k, 50 + k);
                    bs.set(49 - k, 50 + k);
                    final String s = "i==" + i + " && j==" + j + " && k==" + k;
                    assertEquals(s, bs.cardinality(), container.getCardinality());

                    for (int p = bs.nextSetBit(0); p >= 0; p = bs.nextSetBit(p + 1)) {
                        assertTrue(s, container.contains((short) p));
                    }
                }
            }
        }
    }

    @Test
    public void addRangeAndFuseWithNextValueLength() {
        RunContainer container = new RunContainer();
        for (short i = 10; i < 20; ++i) {
            container.iset(i);
        }
        for (short i = 21; i < 30; ++i) {
            container.iset(i);
        }
        Container newContainer = container.add(15, 21);
        assertNotSame(container, newContainer);
        assertEquals(20, newContainer.getCardinality());
        for (short i = 10; i < 30; ++i) {
            assertTrue(newContainer.contains(i));
        }
    }

    @Test
    public void addRangeAndFuseWithPreviousValueLength() {
        RunContainer container = new RunContainer();
        for (short i = 10; i < 20; ++i) {
            container.iset(i);
        }
        Container newContainer = container.add(20, 30);
        assertNotSame(container, newContainer);
        assertEquals(20, newContainer.getCardinality());
        for (short i = 10; i < 30; ++i) {
            assertTrue(newContainer.contains(i));
        }
    }

    @Test
    public void addRangeOnEmptyContainer() {
        RunContainer container = new RunContainer();
        Container newContainer = container.add(10, 100);
        assertNotSame(container, newContainer);
        assertEquals(90, newContainer.getCardinality());
        for (short i = 10; i < 100; ++i) {
            assertTrue(newContainer.contains(i));
        }
    }

    @Test
    public void addRangeOnNonEmptyContainer() {
        RunContainer container = new RunContainer();
        container.iset((short) 1);
        container.iset((short) 256);
        Container newContainer = container.add(10, 100);
        assertNotSame(container, newContainer);
        assertEquals(92, newContainer.getCardinality());
        assertTrue(newContainer.contains((short) 1));
        assertTrue(newContainer.contains((short) 256));
        for (short i = 10; i < 100; ++i) {
            assertTrue(newContainer.contains(i));
        }
    }

    @Test
    public void addRangeOnNonEmptyContainerAndFuse() {
        RunContainer container = new RunContainer();
        for (short i = 1; i < 20; ++i) {
            container.iset(i);
        }
        for (short i = 90; i < 120; ++i) {
            container.iset(i);
        }
        Container newContainer = container.add(10, 100);
        assertNotSame(container, newContainer);
        assertEquals(119, newContainer.getCardinality());
        for (short i = 1; i < 120; ++i) {
            assertTrue(newContainer.contains(i));
        }
    }

    @Test
    public void addRangeWithinSetBounds() {
        RunContainer container = new RunContainer();
        container.iset((short) 10);
        container.iset((short) 99);
        assertEquals(2, container.getCardinality());
        Container newContainer = container.add(10, 100);
        assertNotSame(container, newContainer);
        assertEquals(90, newContainer.getCardinality());
        for (short i = 10; i < 100; ++i) {
            assertTrue(newContainer.contains(i));
        }
    }

    @Test
    public void addRangeWithinSetBoundsAndFuse() {
        RunContainer container = new RunContainer();
        container.iset((short) 1);
        container.iset((short) 10);
        container.iset((short) 55);
        container.iset((short) 99);
        container.iset((short) 150);
        Container newContainer = container.add(10, 100);
        assertNotSame(container, newContainer);
        assertEquals(92, newContainer.getCardinality());
        for (short i = 10; i < 100; ++i) {
            assertTrue(newContainer.contains(i));
        }
    }

    @Test
    public void andNot() {
        Container bc = new BitmapContainer();
        Container rc = new RunContainer();
        for (int k = 0; k < 2 * DEFAULT_MAX_SIZE; ++k) {
            bc = bc.iset((short) (k * 10));
            rc = rc.iset((short) (k * 10 + 3));
        }
        Container result = rc.andNot(bc);
        assertTrue(rc.sameContents(result));
    }

    @Test
    public void andNot1() {
        Container bc = new BitmapContainer();
        Container rc = new RunContainer();
        rc.iset((short) 1);
        Container result = rc.andNot(bc);
        assertEquals(1, result.getCardinality());
        assertTrue(result.contains((short) 1));
    }

    @Test
    public void andNot2() {
        Container bc = new BitmapContainer();
        Container rc = new RunContainer();
        bc.iset((short) 1);
        Container result = rc.andNot(bc);
        assertEquals(0, result.getCardinality());
    }

    @Test
    public void andNotTest1() {
        // this test uses a bitmap container that will be too sparse- okay?
        Container bc = new BitmapContainer();
        Container rc = new RunContainer();
        for (int k = 0; k < 100; ++k) {
            bc = bc.iset((short) (k * 10));
            bc = bc.iset((short) (k * 10 + 3));

            rc = rc.iset((short) (k * 10 + 5));
            rc = rc.iset((short) (k * 10 + 3));
        }
        Container intersectionNOT = rc.andNot(bc);
        assertEquals(100, intersectionNOT.getCardinality());
        for (int k = 0; k < 100; ++k) {
            assertTrue(" missing k=" + k, intersectionNOT.contains((short) (k * 10 + 5)));
        }
        assertEquals(200, bc.getCardinality());
        assertEquals(200, rc.getCardinality());
    }

    @Test
    public void andNotTest2() {
        System.out.println("andNotTest2");
        Container ac = new ArrayContainer();
        Container rc = new RunContainer();
        for (int k = 0; k < 100; ++k) {
            ac = ac.iset((short) (k * 10));
            ac = ac.iset((short) (k * 10 + 3));

            rc = rc.iset((short) (k * 10 + 5));
            rc = rc.iset((short) (k * 10 + 3));
        }
        Container intersectionNOT = rc.andNot(ac);
        assertEquals(100, intersectionNOT.getCardinality());
        for (int k = 0; k < 100; ++k) {
            assertTrue(" missing k=" + k, intersectionNOT.contains((short) (k * 10 + 5)));
        }
        assertEquals(200, ac.getCardinality());
        assertEquals(200, rc.getCardinality());
    }

    @Test
    public void clear() {
        Container rc = new RunContainer();
        rc = rc.iset((short) 1);
        assertEquals(1, rc.getCardinality());
        ((RunContainer) rc).clear();
        assertEquals(0, rc.getCardinality());
    }

    @Test
    public void equalTest1() {
        Container ac = new ArrayContainer();
        Container ar = new RunContainer();
        for (int k = 0; k < 100; ++k) {
            ac = ac.iset((short) (k * 10));
            ar = ar.iset((short) (k * 10));
        }
        assertSameContents(ac, ar);
    }

    @Test
    public void equalTest2() {
        Container ac = new ArrayContainer();
        Container ar = new RunContainer();
        for (int k = 0; k < 10000; ++k) {
            ac = ac.iset((short) k);
            ar = ar.iset((short) k);
        }
        assertSameContents(ac, ar);
    }

    @Test
    public void flip() {
        RunContainer rc = new RunContainer();
        rc.iflip((short) 1);
        assertTrue(rc.contains((short) 1));
        rc.iflip((short) 1);
        assertFalse(rc.contains((short) 1));
    }


    @Test(expected = IllegalArgumentException.class)
    public void iaddInvalidRange1() {
        Container rc = new RunContainer();
        rc.iadd(10, 9);
    }


    @Test(expected = IllegalArgumentException.class)
    public void iaddInvalidRange2() {
        Container rc = new RunContainer();
        rc.iadd(0, 1 << 20);
    }

    @Test
    public void iaddRange() {
        for (int i = 0; i < 100; ++i) {
            for (int j = 0; j < 100; ++j) {
                for (int k = 0; k < 50; ++k) {
                    BitSet bs = new BitSet();
                    Container container = new RunContainer();
                    for (int p = 0; p < i; ++p) {
                        container = container.iset((short) p);
                        bs.set(p);
                    }
                    for (int p = 0; p < j; ++p) {
                        container = container.iset((short) (99 - p));
                        bs.set(99 - p);
                    }
                    container = container.iadd(49 - k, 50 + k);
                    bs.set(49 - k, 50 + k);
                    assertEquals(bs.cardinality(), container.getCardinality());

                    for (int p = bs.nextSetBit(0); p >= 0; p = bs.nextSetBit(p + 1)) {
                        assertTrue(container.contains((short) p));
                    }
                }
            }
        }
    }

    @Test
    public void iaddRange1() {
        Container rc = new RunContainer();
        for (short k = 0; k < 10; ++k) {
            rc.iset(k);
        }
        for (short k = 20; k < 30; ++k) {
            rc.iset(k);
        }
        for (short k = 40; k < 50; ++k) {
            rc.iset(k);
        }
        rc.iadd(5, 21);
        assertEquals(40, rc.getCardinality());
        for (short k = 0; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 40; k < 50; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iaddRange10() {
        Container rc = new RunContainer();
        for (short k = 0; k < 10; ++k) {
            rc.iset(k);
        }
        for (short k = 20; k < 30; ++k) {
            rc.iset(k);
        }
        rc.iadd(15, 35);
        assertEquals(30, rc.getCardinality());
        for (short k = 0; k < 10; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 15; k < 35; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iaddRange11() {
        Container rc = new RunContainer();
        for (short k = 5; k < 10; ++k) {
            rc.iset(k);
        }
        for (short k = 20; k < 30; ++k) {
            rc.iset(k);
        }
        rc.iadd(0, 20);
        assertEquals(30, rc.getCardinality());
        for (short k = 0; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iaddRange12() {
        Container rc = new RunContainer();
        for (short k = 5; k < 10; ++k) {
            rc.iset(k);
        }
        for (short k = 20; k < 30; ++k) {
            rc.iset(k);
        }
        rc.iadd(0, 35);
        assertEquals(35, rc.getCardinality());
        for (short k = 0; k < 35; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iaddRange2() {
        Container rc = new RunContainer();
        for (short k = 0; k < 10; ++k) {
            rc.iset(k);
        }
        for (short k = 20; k < 30; ++k) {
            rc.iset(k);
        }
        for (short k = 40; k < 50; ++k) {
            rc.iset(k);
        }
        rc.iadd(0, 26);
        assertEquals(40, rc.getCardinality());
        for (short k = 0; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 40; k < 50; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iaddRange3() {
        Container rc = new RunContainer();
        for (short k = 0; k < 10; ++k) {
            rc.iset(k);
        }
        for (short k = 20; k < 30; ++k) {
            rc.iset(k);
        }
        for (short k = 40; k < 50; ++k) {
            rc.iset(k);
        }
        rc.iadd(0, 20);
        assertEquals(40, rc.getCardinality());
        for (short k = 0; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 40; k < 50; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iaddRange4() {
        Container rc = new RunContainer();
        for (short k = 0; k < 10; ++k) {
            rc.iset(k);
        }
        for (short k = 20; k < 30; ++k) {
            rc.iset(k);
        }
        for (short k = 40; k < 50; ++k) {
            rc.iset(k);
        }
        rc.iadd(10, 21);
        assertEquals(40, rc.getCardinality());
        for (short k = 0; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 40; k < 50; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iaddRange5() {
        Container rc = new RunContainer();
        for (short k = 0; k < 10; ++k) {
            rc.iset(k);
        }
        for (short k = 20; k < 30; ++k) {
            rc.iset(k);
        }
        for (short k = 40; k < 50; ++k) {
            rc.iset(k);
        }
        rc.iadd(15, 21);
        assertEquals(35, rc.getCardinality());
        for (short k = 0; k < 10; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 15; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 40; k < 50; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iaddRange6() {
        Container rc = new RunContainer();
        for (short k = 5; k < 10; ++k) {
            rc.iset(k);
        }
        for (short k = 20; k < 30; ++k) {
            rc.iset(k);
        }
        for (short k = 40; k < 50; ++k) {
            rc.iset(k);
        }
        rc.iadd(0, 21);
        assertEquals(40, rc.getCardinality());
        for (short k = 0; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 40; k < 50; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iaddRange7() {
        Container rc = new RunContainer();
        for (short k = 0; k < 10; ++k) {
            rc.iset(k);
        }
        for (short k = 20; k < 30; ++k) {
            rc.iset(k);
        }
        for (short k = 40; k < 50; ++k) {
            rc.iset(k);
        }
        rc.iadd(15, 25);
        assertEquals(35, rc.getCardinality());
        for (short k = 0; k < 10; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 15; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 40; k < 50; ++k) {
            assertTrue(rc.contains(k));
        }
    }


    @Test
    public void iaddRange8() {
        Container rc = new RunContainer();
        for (short k = 0; k < 10; ++k) {
            rc.iset(k);
        }
        for (short k = 20; k < 30; ++k) {
            rc.iset(k);
        }
        for (short k = 40; k < 50; ++k) {
            rc.iset(k);
        }
        rc.iadd(15, 40);
        assertEquals(45, rc.getCardinality());
        for (short k = 0; k < 10; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 15; k < 50; ++k) {
            assertTrue(rc.contains(k));
        }
    }


    @Test
    public void iaddRangeAndFuseWithPreviousValueLength() {
        RunContainer container = new RunContainer();
        for (short i = 10; i < 20; ++i) {
            container.iset(i);
        }
        container.iadd(20, 30);
        assertEquals(20, container.getCardinality());
        for (short i = 10; i < 30; ++i) {
            assertTrue(container.contains(i));
        }
    }


    @Test
    public void iaddRangeOnNonEmptyContainerAndFuse() {
        RunContainer container = new RunContainer();
        for (short i = 1; i < 20; ++i) {
            container.iset(i);
        }
        for (short i = 90; i < 120; ++i) {
            container.iset(i);
        }
        container.iadd(10, 100);
        assertEquals(119, container.getCardinality());
        for (short i = 1; i < 120; ++i) {
            assertTrue(container.contains(i));
        }
    }


    @Test
    public void iaddRangeWithinSetBounds() {
        RunContainer container = new RunContainer();
        container.iset((short) 10);
        container.iset((short) 99);
        container.iadd(10, 100);
        assertEquals(90, container.getCardinality());
        for (short i = 10; i < 100; ++i) {
            assertTrue(container.contains(i));
        }
    }

    @Test
    public void inot1() {
        RunContainer container = new RunContainer();
        container.iset((short) 0);
        container.iset((short) 2);
        container.iset((short) 55);
        container.iset((short) 64);
        container.iset((short) 256);

        Container result = container.inot(64, 64); // empty range
        assertSame(container, result);
        assertEquals(5, container.getCardinality());
    }

    @Test
    public void inot10() {
        RunContainer container = new RunContainer();
        container.iset((short) 300);
        container.iset((short) 500);
        container.iset((short) 501);
        container.iset((short) 502);
        container.iset((short) 503);
        container.iset((short) 504);
        container.iset((short) 505);

        // second run begins inside the range but extends outside
        Container result = container.inot(498, 504);

        assertEquals(5, result.getCardinality());
        for (short i : new short[] {300, 498, 499, 504, 505}) {
            assertTrue(result.contains(i));
        }
    }

    @Test
    public void inot11() {
        RunContainer container = new RunContainer();
        container.iset((short) 300);

        container.iset((short) 500);
        container.iset((short) 501);
        container.iset((short) 502);

        container.iset((short) 504);

        container.iset((short) 510);

        // second run entirely inside range, third run entirely inside range, 4th run entirely outside
        Container result = container.inot(498, 507);

        assertEquals(7, result.getCardinality());
        for (short i : new short[] {300, 498, 499, 503, 505, 506, 510}) {
            assertTrue(result.contains(i));
        }
    }

    @Test
    public void inot12() {
        RunContainer container = new RunContainer();
        container.iset((short) 300);

        container.iset((short) 500);
        container.iset((short) 501);
        container.iset((short) 502);

        container.iset((short) 504);

        container.iset((short) 510);
        container.iset((short) 511);

        // second run crosses into range, third run entirely inside range, 4th crosses outside
        Container result = container.inot(501, 511);

        assertEquals(9, result.getCardinality());
        for (short i : new short[] {300, 500, 503, 505, 506, 507, 508, 509, 511}) {
            assertTrue(result.contains(i));
        }
    }

    @Test
    public void inot12A() {
        RunContainer container = new RunContainer();
        container.iset((short) 300);
        container.iset((short) 301);

        // first run crosses into range
        Container result = container.inot(301, 303);

        assertEquals(2, result.getCardinality());
        for (short i : new short[] {300, 302}) {
            assertTrue(result.contains(i));
        }
    }


    @Test
    public void inot13() {
        RunContainer container = new RunContainer();
        // check for off-by-1 errors that might affect length 1 runs

        for (int i = 100; i < 120; i += 3) {
            container.iset((short) i);
        }

        // second run crosses into range, third run entirely inside range, 4th crosses outside
        Container result = container.inot(110, 115);

        assertEquals(10, result.getCardinality());
        for (short i : new short[] {100, 103, 106, 109, 110, 111, 113, 114, 115, 118}) {
            assertTrue(result.contains(i));
        }
    }

    @Test
    public void inot14() {
        inot14once(10, 1);
        inot14once(10, 10);
        inot14once(1000, 100);
        for (int i = 1; i <= 100; ++i) {
            if (i % 10 == 0) {
                System.out.println("inot 14 attempt " + i);
            }
            inot14once(50000, 100);
        }
    }

    private void inot14once(int num, int rangeSize) {
        final String m = "num==" + num + " && rangeSize==" + rangeSize;
        Container container = new RunContainer();
        BitSet checker = new BitSet();
        for (int i = 0; i < num; ++i) {
            int val = (int) (Math.random() * 65536);
            checker.set(val);
            container = container.iset((short) val);
        }

        int rangeStart = (int) Math.random() * (65536 - rangeSize);
        int rangeEnd = rangeStart + rangeSize;

        Container result = container.inot(rangeStart, rangeEnd);
        checker.flip(rangeStart, rangeEnd);

        // ensure they agree on each possible bit
        for (int i = 0; i < 65536; ++i) {
            assertFalse(m + " && i==" + i, result.contains((short) i) ^ checker.get(i));
        }

    }

    @Test
    public void inot15() {
        RunContainer container = new RunContainer();
        for (int i = 0; i < 20000; ++i) {
            container.iset((short) i);
        }

        for (int i = 40000; i < 60000; ++i) {
            container.iset((short) i);
        }

        Container result = container.inot(15000, 25000);

        // this result should stay as a run container (same one)
        assertSame(container, result);
    }

    @Test
    public void inot2() {
        RunContainer container = new RunContainer();
        container.iset((short) 0);
        container.iset((short) 2);
        container.iset((short) 55);
        container.iset((short) 64);
        container.iset((short) 256);

        Container result = container.inot(64, 66);
        assertEquals(5, result.getCardinality());
        for (short i : new short[] {0, 2, 55, 65, 256}) {
            assertTrue(result.contains(i));
        }
    }

    @Test
    public void inot3() {
        RunContainer container = new RunContainer();
        // applied to a run-less container
        Container result = container.inot(64, 68);
        assertEquals(4, result.getCardinality());
        for (short i : new short[] {64, 65, 66, 67}) {
            assertTrue(result.contains(i));
        }
    }


    @Test
    public void inot4() {
        RunContainer container = new RunContainer();
        container.iset((short) 0);
        container.iset((short) 2);
        container.iset((short) 55);
        container.iset((short) 64);
        container.iset((short) 256);

        // all runs are before the range
        Container result = container.inot(300, 303);
        assertEquals(8, result.getCardinality());
        for (short i : new short[] {0, 2, 55, 64, 256, 300, 301, 302}) {
            assertTrue(result.contains(i));
        }
    }


    @Test
    public void inot5() {
        RunContainer container = new RunContainer();
        container.iset((short) 500);
        container.iset((short) 502);
        container.iset((short) 555);
        container.iset((short) 564);
        container.iset((short) 756);

        // all runs are after the range
        Container result = container.inot(300, 303);
        assertEquals(8, result.getCardinality());
        for (short i : new short[] {500, 502, 555, 564, 756, 300, 301, 302}) {
            assertTrue(result.contains(i));
        }
    }

    @Test
    public void inot6() {
        RunContainer container = new RunContainer();
        container.iset((short) 500);
        container.iset((short) 501);
        container.iset((short) 502);
        container.iset((short) 503);

        // one run is strictly within the range
        Container result = container.inot(499, 505);
        assertEquals(2, result.getCardinality());
        for (short i : new short[] {499, 504}) {
            assertTrue(result.contains(i));
        }
    }


    @Test
    public void inot7() {
        RunContainer container = new RunContainer();
        container.iset((short) 500);
        container.iset((short) 501);
        container.iset((short) 502);
        container.iset((short) 503);
        container.iset((short) 504);
        container.iset((short) 505);


        // one run, spans the range
        Container result = container.inot(502, 504);

        assertEquals(4, result.getCardinality());
        for (short i : new short[] {500, 501, 504, 505}) {
            assertTrue(result.contains(i));
        }
    }

    @Test
    public void inot8() {
        RunContainer container = new RunContainer();
        container.iset((short) 300);
        container.iset((short) 500);
        container.iset((short) 501);
        container.iset((short) 502);
        container.iset((short) 503);
        container.iset((short) 504);
        container.iset((short) 505);

        // second run, spans the range
        Container result = container.inot(502, 504);

        assertEquals(5, result.getCardinality());
        for (short i : new short[] {300, 500, 501, 504, 505}) {
            assertTrue(result.contains(i));
        }
    }

    @Test
    public void inot9() {
        RunContainer container = new RunContainer();
        container.iset((short) 500);
        container.iset((short) 501);
        container.iset((short) 502);
        container.iset((short) 503);
        container.iset((short) 504);
        container.iset((short) 505);

        // first run, begins inside the range but extends outside
        Container result = container.inot(498, 504);

        assertEquals(4, result.getCardinality());
        for (short i : new short[] {498, 499, 504, 505}) {
            assertTrue(result.contains(i));
        }
    }

    @Test
    public void intersectionTest1() {
        Container ac = new ArrayContainer();
        Container rc = new RunContainer();
        for (int k = 0; k < 100; ++k) {
            ac = ac.iset((short) (k * 10));
            rc = rc.iset((short) (k * 10));
        }
        assertSameContents(ac, ac.and(rc));
        assertSameContents(ac, rc.and(ac));
    }

    @Test
    public void intersectionTest2() {
        Container ac = new ArrayContainer();
        Container rc = new RunContainer();
        for (int k = 0; k < 10000; ++k) {
            ac = ac.iset((short) k);
            rc = rc.iset((short) k);
        }
        assertSameContents(ac, ac.and(rc));
        assertSameContents(ac, rc.and(ac));
    }

    @Test
    public void intersectionTest3() {
        Container ac = new ArrayContainer();
        Container rc = new RunContainer();
        for (int k = 0; k < 100; ++k) {
            ac = ac.iset((short) k);
            rc = rc.iset((short) (k + 100));
        }
        assertEquals(0, rc.and(ac).getCardinality());
    }

    @Test
    public void intersectionTest4() {
        Container bc = new BitmapContainer();
        Container rc = new RunContainer();
        for (int k = 0; k < 100; ++k) {
            bc = bc.iset((short) (k * 10));
            bc = bc.iset((short) (k * 10 + 3));

            rc = rc.iset((short) (k * 10 + 5));
            rc = rc.iset((short) (k * 10 + 3));
        }
        Container intersection = rc.and(bc);
        assertEquals(100, intersection.getCardinality());
        for (int k = 0; k < 100; ++k) {
            assertTrue(intersection.contains((short) (k * 10 + 3)));
        }
        assertEquals(200, bc.getCardinality());
        assertEquals(200, rc.getCardinality());
    }

    @Test
    public void ior() {
        Container rc1 = new RunContainer();
        Container rc2 = new RunContainer();
        rc1 = rc1.iadd(0, 128);
        rc2 = rc2.iadd(128, 256);
        rc1 = rc1.ior(rc2);
        assertEquals(256, rc1.getCardinality());
    }

    @Test
    public void ior2() {
        Container rc = new RunContainer();
        Container ac = new ArrayContainer();
        rc = rc.iadd(0, 128);
        rc = rc.iadd(256, 512);
        ac = ac.iadd(128, 256);
        rc = rc.ior(ac);
        assertEquals(512, rc.getCardinality());
    }

    @Test
    public void ior3() {
        Container rc = new RunContainer();
        Container ac = new ArrayContainer();
        rc = rc.iadd(0, 128);
        rc = rc.iadd(256, 512);
        final int origCard = rc.getCardinality();
        ac = ac.iset((short) 130);
        rc = rc.ior(ac);
        assertEquals(origCard + 1, rc.getCardinality());
        assertTrue(rc.contains(0, 128));
        assertTrue(rc.contains((short) 130));
        assertTrue(rc.contains(256, 512));
    }

    @Test
    public void ior4() {
        final int left = 65000;
        final int right = 65020;
        int i = 0;
        for (int leftDelta0 = -1; leftDelta0 <= 1; ++leftDelta0) {
            for (int rightDelta0 = -1; rightDelta0 <= 1; ++rightDelta0) {
                for (int leftDelta1 = -1; leftDelta1 <= 1; ++leftDelta1) {
                    for (int rightDelta1 = -1; rightDelta1 <= 1; ++rightDelta1) {
                        for (boolean asArray : new boolean[] {false, true}) {
                            for (boolean prevRun : new boolean[] {false, true}) {
                                Container c0 = new RunContainer();
                                Container c1 = asArray ? new ArrayContainer() : new RunContainer();
                                c0 = c0.iadd(left + leftDelta0, right + rightDelta0 + 1);
                                final int expectedMin;
                                if (prevRun) {
                                    expectedMin = left - 5;
                                    c0 = c0.iadd(expectedMin, left - 3);
                                } else {
                                    expectedMin = left + Math.min(leftDelta0, leftDelta1);
                                }
                                c1 = c1.iadd(left + leftDelta1, right + rightDelta1 + 1);
                                final Container expected = c0.or(c1);
                                c0 = c0.ior(c1);
                                final int expectedMax = right + Math.max(rightDelta0, rightDelta1);
                                final String s = "i==" + i;
                                assertEquals(s, c0.first(), expectedMin);
                                assertEquals(s, c0.last(), expectedMax);
                                assertTrue(s, c0.sameContents(expected));
                                ++i;
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void iremove1() {
        Container rc = new RunContainer();
        rc = rc.iset((short) 1);
        rc = rc.iremove(1, 2);
        assertEquals(0, rc.getCardinality());
    }

    @Test
    public void iremove10() {
        Container rc = new RunContainer();
        rc = rc.iadd(5, 10);
        rc = rc.iadd(20, 30);
        rc = rc.iremove(0, 25);
        assertEquals(5, rc.getCardinality());
        for (short k = 25; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iremove11() {
        Container rc = new RunContainer();
        rc = rc.iadd(5, 10);
        rc = rc.iadd(20, 30);
        rc = rc.iremove(0, 35);
        assertEquals(0, rc.getCardinality());
    }

    @Test
    public void iremove12() {
        Container rc = new RunContainer();
        rc = rc.iset((short) 0);
        rc = rc.iset((short) 10);
        rc = rc.iremove(0, 11);
        assertEquals(0, rc.getCardinality());
    }

    @Test
    public void iremove13() {
        Container rc = new RunContainer();
        rc = rc.iadd(0, 10);
        rc = rc.iadd(20, 30);
        rc = rc.iremove(5, 25);
        assertEquals(10, rc.getCardinality());
        for (short k = 0; k < 5; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 25; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iremove14() {
        Container rc = new RunContainer();
        rc = rc.iadd(0, 10);
        rc = rc.iadd(20, 30);
        rc = rc.iremove(5, 31);
        assertEquals(5, rc.getCardinality());
        for (short k = 0; k < 5; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iremove15() {
        Container rc = new RunContainer();
        rc = rc.iadd(0, 5);
        rc = rc.iadd(20, 30);
        rc = rc.iremove(5, 25);
        assertEquals(10, rc.getCardinality());
        for (short k = 0; k < 5; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 25; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iremove16() {
        Container rc = new RunContainer();
        rc = rc.iadd(0, 5);
        rc = rc.iadd(20, 30);
        rc = rc.iremove(5, 31);
        assertEquals(5, rc.getCardinality());
        for (short k = 0; k < 5; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iremove17() {
        Container rc = new RunContainer();
        rc = rc.iadd(37543, 65536);
        rc = rc.iremove(9795, 65536);
        assertEquals(0, rc.getCardinality());
    }

    @Test
    public void iremove18() {
        Container rc = new RunContainer();
        rc = rc.iadd(100, 200);
        rc = rc.iadd(300, 400);
        rc = rc.iadd(37543, 65000);
        rc = rc.iremove(300, 65000); // start at beginning of run, end at end of another run
        assertEquals(100, rc.getCardinality());
    }

    @Test
    public void iremove19() {
        Container rc = new RunContainer();
        rc = rc.iadd(100, 200);
        rc = rc.iadd(300, 400);
        rc = rc.iadd(64000, 65000);
        rc = rc.iremove(350, 64000); // start midway through run, end at the start of next
        // got 100..199, 300..349, 64000..64999
        assertEquals(1150, rc.getCardinality());
    }

    @Test
    public void iremove2() {
        Container rc = new RunContainer();
        rc = rc.iadd(0, 10);
        rc = rc.iadd(20, 30);
        rc = rc.iremove(0, 21);
        assertEquals(9, rc.getCardinality());
        for (short k = 21; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iremove20() {
        Container rc = new RunContainer();
        rc = rc.iadd(100, 200);
        rc = rc.iadd(300, 400);
        rc = rc.iadd(64000, 65000);
        rc = rc.iremove(350, 64001); // start midway through run, end at the start of next
        // got 100..199, 300..349, 64001..64999
        assertEquals(1149, rc.getCardinality());
    }

    @Test
    public void iremove3() {
        Container rc = new RunContainer();
        rc = rc.iadd(0, 10);
        rc = rc.iadd(20, 30);
        rc = rc.iadd(40, 50);
        rc = rc.iremove(0, 21);
        assertEquals(19, rc.getCardinality());
        for (short k = 21; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 40; k < 50; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iremove4() {
        Container rc = new RunContainer();
        rc = rc.iadd(0, 10);
        rc = rc.iremove(0, 5);
        assertEquals(5, rc.getCardinality());
        for (short k = 5; k < 10; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iremove5() {
        Container rc = new RunContainer();
        rc = rc.iadd(0, 10);
        rc = rc.iadd(20, 30);
        rc = rc.iremove(0, 31);
        assertEquals(0, rc.getCardinality());
    }

    @Test
    public void iremove6() {
        Container rc = new RunContainer();
        rc = rc.iadd(0, 10);
        rc = rc.iadd(20, 30);
        rc = rc.iremove(0, 25);
        assertEquals(5, rc.getCardinality());
        for (short k = 25; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iremove7() {
        Container rc = new RunContainer();
        rc = rc.iadd(0, 10);
        rc = rc.iremove(0, 15);
        assertEquals(0, rc.getCardinality());
    }

    @Test
    public void iremove8() {
        Container rc = new RunContainer();
        rc = rc.iadd(0, 10);
        rc = rc.iadd(20, 30);
        rc = rc.iremove(5, 21);
        assertEquals(14, rc.getCardinality());
        for (short k = 0; k < 5; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 21; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test
    public void iremove9() {
        Container rc = new RunContainer();
        rc = rc.iadd(0, 10);
        rc = rc.iadd(20, 30);
        rc = rc.iremove(15, 21);
        assertEquals(19, rc.getCardinality());
        for (short k = 0; k < 10; ++k) {
            assertTrue(rc.contains(k));
        }
        for (short k = 21; k < 30; ++k) {
            assertTrue(rc.contains(k));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void iremoveInvalidRange1() {
        Container rc = new RunContainer();
        rc.iremove(10, 9);
    }

    @Test(expected = IllegalArgumentException.class)
    public void iremoveInvalidRange2() {
        Container rc = new RunContainer();
        rc.remove(0, 1 << 20);
    }

    @Test
    public void iremoveRange() {
        for (int i = 0; i < 100; ++i) {
            for (int j = 0; j < 100; ++j) {
                for (int k = 0; k < 50; ++k) {
                    BitSet bs = new BitSet();
                    Container container = new RunContainer();
                    for (int p = 0; p < i; ++p) {
                        container = container.iset((short) p);
                        container.validate();
                        bs.set(p);
                    }
                    for (int p = 0; p < j; ++p) {
                        container = container.iset((short) (99 - p));
                        container.validate();
                        bs.set(99 - p);
                    }
                    container = container.iremove(49 - k, 50 + k);
                    container.validate();
                    bs.clear(49 - k, 50 + k);
                    assertEquals(bs.cardinality(), container.getCardinality());

                    for (int p = bs.nextSetBit(0); p >= 0; p = bs.nextSetBit(p + 1)) {
                        assertTrue(container.contains((short) p));
                    }
                }
            }
        }
    }

    @Test
    public void iremoveEmptyRange() {
        Container container = new RunContainer();
        assertEquals(0, container.getCardinality());
        container = container.iremove(0, 0);
        assertEquals(0, container.getCardinality());
    }

    @Test
    public void iterator() {
        RunContainer x = new RunContainer();
        for (int k = 0; k < 100; ++k) {
            for (int j = 0; j < k; ++j) {
                x = (RunContainer) x.iset((short) (k * 100 + j));
            }
        }
        ShortIterator i = x.getShortIterator();
        for (int k = 0; k < 100; ++k) {
            for (int j = 0; j < k; ++j) {
                assertTrue(i.hasNext());
                assertEquals(i.next(), (short) (k * 100 + j));
            }
        }
        assertFalse(i.hasNext());
    }

    @Test
    public void longbacksimpleIterator() {
        RunContainer x = new RunContainer();
        for (int k = 0; k < (1 << 16); ++k) {
            x = (RunContainer) x.iset((short) k);
        }
        ShortIterator i = x.getReverseShortIterator();
        for (int k = (1 << 16) - 1; k >= 0; --k) {
            assertTrue(i.hasNext());
            assertEquals(i.next(), (short) k);
        }
        assertFalse(i.hasNext());
    }

    @Test
    public void longcsimpleIterator() {
        RunContainer x = new RunContainer();
        for (int k = 0; k < (1 << 16); ++k) {
            x = (RunContainer) x.iset((short) k);
        }
        ShortIterator i = x.getShortIterator();
        for (int k = 0; k < (1 << 16); ++k) {
            assertTrue(i.hasNext());
            assertEquals(i.next(), (short) k);
        }
        assertFalse(i.hasNext());
    }

    @Test
    public void longsimpleIterator() {
        RunContainer x = new RunContainer();
        for (int k = 0; k < (1 << 16); ++k) {
            x = (RunContainer) x.iset((short) (k));
        }
        ShortIterator i = x.getShortIterator();
        for (int k = 0; k < (1 << 16); ++k) {
            assertTrue(i.hasNext());
            assertEquals(i.next(), (short) k);
        }
        assertFalse(i.hasNext());
    }

    @Test
    public void not10() {
        Container rc = new RunContainer();
        rc = rc.iset((short) 300);
        rc = rc.iset((short) 500);
        rc = rc.iset((short) 501);
        rc = rc.iset((short) 502);
        rc = rc.iset((short) 503);
        rc = rc.iset((short) 504);
        rc = rc.iset((short) 505);

        // second run begins inside the range but extends outside
        Container result = rc.not(498, 504);

        assertEquals(5, result.getCardinality());
        for (short i : new short[] {300, 498, 499, 504, 505}) {
            assertTrue(result.contains(i));
        }
    }

    @Test
    public void not11() {
        RunContainer container = new RunContainer();
        container.iset((short) 300);

        container.iset((short) 500);
        container.iset((short) 501);
        container.iset((short) 502);

        container.iset((short) 504);

        container.iset((short) 510);

        // second run entirely inside range, third run entirely inside range, 4th run entirely outside
        Container result = container.not(498, 507);

        assertEquals(7, result.getCardinality());
        for (short i : new short[] {300, 498, 499, 503, 505, 506, 510}) {
            assertTrue(result.contains(i));
        }
    }


    @Test
    public void not12() {
        RunContainer container = new RunContainer();
        container.iset((short) 300);

        container.iset((short) 500);
        container.iset((short) 501);
        container.iset((short) 502);

        container.iset((short) 504);

        container.iset((short) 510);
        container.iset((short) 511);

        // second run crosses into range, third run entirely inside range, 4th crosses outside
        Container result = container.not(501, 511);

        assertEquals(9, result.getCardinality());
        for (short i : new short[] {300, 500, 503, 505, 506, 507, 508, 509, 511}) {
            assertTrue(result.contains(i));
        }
    }


    @Test
    public void not12A() {
        RunContainer container = new RunContainer();
        container.iset((short) 300);
        container.iset((short) 301);

        // first run crosses into range
        Container result = container.not(301, 303);

        assertEquals(2, result.getCardinality());
        for (short i : new short[] {300, 302}) {
            assertTrue(result.contains(i));
        }
    }

    @Test
    public void not13() {
        RunContainer container = new RunContainer();
        // check for off-by-1 errors that might affect length 1 runs

        for (int i = 100; i < 120; i += 3) {
            container.iset((short) i);
        }

        // second run crosses into range, third run entirely inside range, 4th crosses outside
        Container result = container.not(110, 115);

        assertEquals(10, result.getCardinality());
        for (short i : new short[] {100, 103, 106, 109, 110, 111, 113, 114, 115, 118}) {
            assertTrue(result.contains(i));
        }
    }


    @Test
    public void not14() {
        not14once(10, 1);
        not14once(10, 10);
        not14once(1000, 100);

        for (int i = 1; i <= 100; ++i) {
            if (i % 10 == 0) {
                System.out.println("not 14 attempt " + i);
            }
            not14once(50000, 100);
        }
    }


    private void not14once(int num, int rangeSize) {
        Container container = new RunContainer();
        BitSet checker = new BitSet();
        for (int i = 0; i < num; ++i) {
            int val = (int) (Math.random() * 65536);
            checker.set(val);
            container = container.iset((short) val);
        }

        int rangeStart = (int) Math.random() * (65536 - rangeSize);
        int rangeEnd = rangeStart + rangeSize;

        Container result = container.not(rangeStart, rangeEnd);
        checker.flip(rangeStart, rangeEnd);

        // ensure they agree on each possible bit
        for (int i = 0; i < 65536; ++i) {
            assertFalse(result.contains((short) i) ^ checker.get(i));
        }
    }


    @Test
    public void not15() {
        RunContainer container = new RunContainer();
        for (int i = 0; i < 20000; ++i) {
            container.iset((short) i);
        }

        for (int i = 40000; i < 60000; ++i) {
            container.iset((short) i);
        }

        Container result = container.not(15000, 25000);

        // this result should stay as a run container.
        assertTrue(result instanceof RunContainer);
    }


    @Test
    public void not2() {
        RunContainer container = new RunContainer();
        container.iset((short) 0);
        container.iset((short) 2);
        container.iset((short) 55);
        container.iset((short) 64);
        container.iset((short) 256);

        Container result = container.not(64, 66);
        assertEquals(5, result.getCardinality());
        for (short i : new short[] {0, 2, 55, 65, 256}) {
            assertTrue(result.contains(i));
        }
    }


    @Test
    public void not3() {
        RunContainer container = new RunContainer();
        // applied to a run-less container
        Container result = container.not(64, 68);
        assertEquals(4, result.getCardinality());
        for (short i : new short[] {64, 65, 66, 67}) {
            assertTrue(result.contains(i));
        }
    }

    @Test
    public void not4() {
        RunContainer container = new RunContainer();
        container.iset((short) 0);
        container.iset((short) 2);
        container.iset((short) 55);
        container.iset((short) 64);
        container.iset((short) 256);

        // all runs are before the range
        Container result = container.not(300, 303);
        assertEquals(8, result.getCardinality());
        for (short i : new short[] {0, 2, 55, 64, 256, 300, 301, 302}) {
            assertTrue(result.contains(i));
        }
    }


    @Test
    public void not5() {
        RunContainer container = new RunContainer();
        container.iset((short) 500);
        container.iset((short) 502);
        container.iset((short) 555);
        container.iset((short) 564);
        container.iset((short) 756);

        // all runs are after the range
        Container result = container.not(300, 303);
        assertEquals(8, result.getCardinality());
        for (short i : new short[] {500, 502, 555, 564, 756, 300, 301, 302}) {
            assertTrue(result.contains(i));
        }
    }


    @Test
    public void not6() {
        RunContainer container = new RunContainer();
        container.iset((short) 500);
        container.iset((short) 501);
        container.iset((short) 502);
        container.iset((short) 503);

        // one run is strictly within the range
        Container result = container.not(499, 505);
        assertEquals(2, result.getCardinality());
        for (short i : new short[] {499, 504}) {
            assertTrue(result.contains(i));
        }
    }

    @Test
    public void not7() {
        RunContainer container = new RunContainer();
        container.iset((short) 500);
        container.iset((short) 501);
        container.iset((short) 502);
        container.iset((short) 503);
        container.iset((short) 504);
        container.iset((short) 505);


        // one run, spans the range
        Container result = container.not(502, 504);

        assertEquals(4, result.getCardinality());
        for (short i : new short[] {500, 501, 504, 505}) {
            assertTrue(result.contains(i));
        }
    }


    @Test
    public void not8() {
        RunContainer container = new RunContainer();
        container.iset((short) 300);
        container.iset((short) 500);
        container.iset((short) 501);
        container.iset((short) 502);
        container.iset((short) 503);
        container.iset((short) 504);
        container.iset((short) 505);

        // second run, spans the range
        Container result = container.not(502, 504);

        assertEquals(5, result.getCardinality());
        for (short i : new short[] {300, 500, 501, 504, 505}) {
            assertTrue(result.contains(i));
        }
    }


    @Test
    public void not9() {
        RunContainer container = new RunContainer();
        container.iset((short) 500);
        container.iset((short) 501);
        container.iset((short) 502);
        container.iset((short) 503);
        container.iset((short) 504);
        container.iset((short) 505);

        // first run, begins inside the range but extends outside
        Container result = container.not(498, 504);

        assertEquals(4, result.getCardinality());
        for (short i : new short[] {498, 499, 504, 505}) {
            assertTrue(result.contains(i));
        }
    }


    @Test
    public void randomFun() {
        final int bitsetperword1 = 32;
        final int bitsetperword2 = 63;

        Container rc1, rc2, ac1, ac2;
        Random rand = new Random(0);
        final int max = 1 << 16;
        final int howmanywords = (1 << 16) / 64;
        int[] values1 = generateUniformHash(rand, bitsetperword1 * howmanywords, max);
        int[] values2 = generateUniformHash(rand, bitsetperword2 * howmanywords, max);


        rc1 = new RunContainer();
        rc1 = fillMeUp(rc1, values1);

        rc2 = new RunContainer();
        rc2 = fillMeUp(rc2, values2);

        ac1 = new ArrayContainer();
        ac1 = fillMeUp(ac1, values1);

        ac2 = new ArrayContainer();
        ac2 = fillMeUp(ac2, values2);

        if (!rc1.sameContents(ac1)) {
            throw new RuntimeException("first containers do not match");
        }

        if (!rc2.sameContents(ac2)) {
            throw new RuntimeException("second containers do not match");
        }

        if (!rc1.or(rc2).sameContents(ac1.or(ac2))) {
            throw new RuntimeException("ors do not match");
        }
        if (!rc1.and(rc2).sameContents(ac1.and(ac2))) {
            throw new RuntimeException("ands do not match");
        }
        if (!rc1.andNot(rc2).sameContents(ac1.andNot(ac2))) {
            throw new RuntimeException("andnots do not match");
        }
        if (!rc2.andNot(rc1).sameContents(ac2.andNot(ac1))) {
            throw new RuntimeException("andnots do not match");
        }
        if (!rc1.xor(rc2).sameContents(ac1.xor(ac2))) {
            throw new RuntimeException("xors do not match");
        }

    }

    @Test
    public void rank() {
        RunContainer container = new RunContainer();
        container.iset((short) 0);
        container.iset((short) 2);
        container.iset((short) 55);
        container.iset((short) 64);
        container.iset((short) 256);
        assertEquals(1, container.rank((short) 0));
        assertEquals(2, container.rank((short) 10));
        assertEquals(4, container.rank((short) 128));
        assertEquals(5, container.rank((short) 1024));
    }

    @Test
    public void shortRangeRank() {
        Container container = new RunContainer();
        container = container.add(16, 32);
        assertTrue(container instanceof RunContainer);
        // results in correct value: 16
        // assertEquals(16, container.toBitmapContainer().rank((short) 32));
        assertEquals(16, container.rank((short) 32));
    }


    @Test
    public void remove() {
        Container rc = new RunContainer();
        rc.iset((short) 1);
        Container newContainer = rc.remove(1, 2);
        assertEquals(0, newContainer.getCardinality());
    }


    @Test
    public void RunContainerArg_ArrayAND() {
        boolean atLeastOneArray = false;
        ArrayList<Container> set = new ArrayList<Container>();
        ArrayList<Container> setb = new ArrayList<Container>();
        getSetOfContainers(set, setb);
        for (int k = 0; k < set.size(); ++k) {
            for (int l = 0; l < set.size(); ++l) {
                final String m = "k==" + k + " && l==" + l;
                assertTrue(m, set.get(k).sameContents(setb.get(k)));
                assertTrue(m, set.get(l).sameContents(setb.get(l)));
                Container thisContainer = setb.get(k);
                if (thisContainer instanceof BitmapContainer) {
                    // continue;
                } else {
                    atLeastOneArray = true;
                }
                Container c1 = thisContainer.and(set.get(l));
                Container c2 = setb.get(k).and(setb.get(l));
                assertTrue(m, c1.sameContents(c2));
            }
        }
        assertTrue(atLeastOneArray);
    }


    @Test
    public void RunContainerArg_ArrayANDNOT() {
        boolean atLeastOneArray = false;
        ArrayList<Container> set = new ArrayList<Container>();
        ArrayList<Container> setb = new ArrayList<Container>();
        getSetOfContainers(set, setb);
        for (int k = 0; k < set.size(); ++k) {
            for (int l = 0; l < set.size(); ++l) {
                final String m = "k==" + k + " && l==" + l;
                boolean same = set.get(k).sameContents(setb.get(k));
                assertTrue(m, same);
                same = set.get(l).sameContents(setb.get(l));
                assertTrue(m, same);
                Container thisContainer = setb.get(k);
                if (thisContainer instanceof BitmapContainer) {
                    // continue;
                } else {
                    atLeastOneArray = true;
                }

                Container c1 = thisContainer.andNot(set.get(l));
                c1.validate();
                Container c2 = setb.get(k).andNot(setb.get(l));
                c2.validate();

                assertTrue(m, c1.sameContents(c2));
            }
        }
        assertTrue(atLeastOneArray);
    }

    @Test
    public void RunContainerArg_ArrayANDNOT2() {
        ArrayContainer ac = new ArrayContainer(new short[] {0, 2, 4, 8, 10, 15, 16, 48, 50, 61, 80, -2});
        RunContainer rc = new RunContainer(new short[] {7, 3, 17, 2, 20, 3, 30, 3, 36, 6, 60, 5, -3, 2}, 7);
        assertSameContents(new ArrayContainer(new short[] {0, 2, 4, 15, 16, 48, 50, 80}), ac.andNot(rc));
    }

    @Test
    public void FullRunContainerArg_ArrayANDNOT2() {
        ArrayContainer ac = new ArrayContainer(new short[] {3});
        Container rc = Container.full();
        assertTrue(ac.andNot(rc).isEmpty());
    }

    @Test
    public void RunContainerArg_ArrayANDNOT3() {
        ArrayContainer ac = new ArrayContainer(new short[] {5});
        Container rc = new RunContainer(new short[] {3, 10}, 1);
        assertTrue(ac.andNot(rc).isEmpty());
    }

    @Test
    public void RunContainerArg_ArrayOR() {
        boolean atLeastOneArray = false;
        ArrayList<Container> set = new ArrayList<Container>();
        ArrayList<Container> setb = new ArrayList<Container>();
        getSetOfContainers(set, setb);
        for (int k = 0; k < set.size(); ++k) {
            for (int l = 0; l < set.size(); ++l) {
                final String m = "k==" + k + " && l==" + l;
                assertTrue(m, set.get(k).sameContents(setb.get(k)));
                assertTrue(m, set.get(l).sameContents(setb.get(l)));
                Container thisContainer = setb.get(k);
                // BitmapContainers are tested separately, but why not test some more?
                if (thisContainer instanceof BitmapContainer) {
                    // continue;
                } else {
                    atLeastOneArray = true;
                }

                Container c1 = thisContainer.or(set.get(l));
                Container c2 = setb.get(k).or(setb.get(l));
                assertTrue(m, c1.sameContents(c2));
            }
        }
        assertTrue(atLeastOneArray);
    }

    @Test
    public void RunContainerArg_ArrayXOR() {
        boolean atLeastOneArray = false;
        ArrayList<Container> set = new ArrayList<Container>();
        ArrayList<Container> setb = new ArrayList<Container>();
        getSetOfContainers(set, setb);
        for (int k = 0; k < set.size(); ++k) {
            for (int l = 0; l < set.size(); ++l) {
                final String m = "k==" + k + " && l==" + l;
                assertTrue(m, set.get(k).sameContents(setb.get(k)));
                assertTrue(m, set.get(l).sameContents(setb.get(l)));
                Container thisContainer = setb.get(k);
                if (thisContainer instanceof BitmapContainer) {
                    // continue;
                } else {
                    atLeastOneArray = true;
                }

                Container c1 = thisContainer.xor(set.get(l));
                c1.validate();
                Container c2 = setb.get(k).xor(setb.get(l));
                c2.validate();
                assertTrue(m, c1.sameContents(c2));
            }
        }
        assertTrue(atLeastOneArray);
    }

    @Test
    public void RunContainerFromBitmap() {
        Container rc = new RunContainer();
        Container bc = new BitmapContainer();

        rc = rc.iset((short) 2);
        bc = bc.iset((short) 2);
        rc = rc.iset((short) 3);
        bc = bc.iset((short) 3);
        rc = rc.iset((short) 4);
        bc = bc.iset((short) 4);
        rc = rc.iset((short) 17);
        bc = bc.iset((short) 17);
        for (int i = 192; i < 500; ++i) {
            rc = rc.iset((short) i);
            bc = bc.iset((short) i);
        }
        rc = rc.iset((short) 1700);
        bc = bc.iset((short) 1700);
        rc = rc.iset((short) 1701);
        bc = bc.iset((short) 1701);

        // cases depending on whether we have largest item.
        // this test: no, we don't get near largest word

        RunContainer rc2 = new RunContainer((BitmapContainer) bc);
        assertTrue(rc.sameContents(rc2));
    }

    @Test
    public void RunContainerFromBitmap1() {
        Container rc = new RunContainer();
        Container bc = new BitmapContainer();


        rc = rc.iset((short) 2);
        bc = bc.iset((short) 2);
        rc = rc.iset((short) 3);
        bc = bc.iset((short) 3);
        rc = rc.iset((short) 4);
        bc = bc.iset((short) 4);
        rc = rc.iset((short) 17);
        bc = bc.iset((short) 17);
        for (int i = 192; i < 500; ++i) {
            rc = rc.iset((short) i);
            bc = bc.iset((short) i);
        }
        rc = rc.iset((short) 1700);
        bc = bc.iset((short) 1700);
        rc = rc.iset((short) 1701);
        bc = bc.iset((short) 1701);

        // cases depending on whether we have largest item.
        // this test: we have a 1 in the largest word but not at end
        rc = rc.iset((short) 65530);
        bc = bc.iset((short) 65530);

        RunContainer rc2 = new RunContainer((BitmapContainer) bc);
        assertSameContents(rc, rc2);
    }


    @Test
    public void RunContainerFromBitmap2() {
        Container rc = new RunContainer();
        Container bc = new BitmapContainer();

        rc = rc.iset((short) 2);
        bc = bc.iset((short) 2);
        rc = rc.iset((short) 3);
        bc = bc.iset((short) 3);
        rc = rc.iset((short) 4);
        bc = bc.iset((short) 4);
        rc = rc.iset((short) 17);
        bc = bc.iset((short) 17);
        for (int i = 192; i < 500; ++i) {
            rc = rc.iset((short) i);
            bc = bc.iset((short) i);
        }
        rc = rc.iset((short) 1700);
        bc = bc.iset((short) 1700);
        rc = rc.iset((short) 1701);
        bc = bc.iset((short) 1701);

        // cases depending on whether we have largest item.
        // this test: we have a 1 in the largest word and at end
        rc = rc.iset((short) 65530);
        bc = bc.iset((short) 65530);
        rc = rc.iset((short) 65535);
        bc = bc.iset((short) 65535);


        RunContainer rc2 = new RunContainer((BitmapContainer) bc);
        assertSameContents(rc, rc2);
    }


    @Test
    public void RunContainerFromBitmap3() {
        Container rc = new RunContainer();
        Container bc = new BitmapContainer();

        rc = rc.iset((short) 2);
        bc = bc.iset((short) 2);
        rc = rc.iset((short) 3);
        bc = bc.iset((short) 3);
        rc = rc.iset((short) 4);
        bc = bc.iset((short) 4);
        rc = rc.iset((short) 17);
        bc = bc.iset((short) 17);
        for (int i = 192; i < 500; ++i) {
            rc = rc.iset((short) i);
            bc = bc.iset((short) i);
        }
        rc = rc.iset((short) 1700);
        bc = bc.iset((short) 1700);
        rc = rc.iset((short) 1701);
        bc = bc.iset((short) 1701);
        // cases depending on whether we have largest item.
        // this test: we have a lot of 1s in a run at the end

        for (int i = 65000; i < 65535; ++i) {
            rc = rc.iset((short) i);
            bc = bc.iset((short) i);
        }

        RunContainer rc2 = new RunContainer((BitmapContainer) bc);
        assertSameContents(rc, rc2);
    }

    @Test
    public void RunContainerVSRunContainerAND() {
        ArrayList<Container> set = new ArrayList<Container>();
        ArrayList<Container> setb = new ArrayList<Container>();
        getSetOfContainers(set, setb);
        for (int k = 0; k < set.size(); ++k) {
            for (int l = 0; l < set.size(); ++l) {
                final String m = "k==" + k + " && l==" + l;
                assertTrue(m, set.get(k).sameContents(setb.get(k)));
                assertTrue(m, set.get(l).sameContents(setb.get(l)));
                Container c1 = set.get(k).and(set.get(l));
                Container c2 = setb.get(k).and(setb.get(l));
                assertTrue(m, c1.sameContents(c2));
            }
        }
    }


    @Test
    public void RunContainerVSRunContainerANDNOT() {
        ArrayList<Container> set = new ArrayList<Container>();
        ArrayList<Container> setb = new ArrayList<Container>();
        getSetOfContainers(set, setb);
        for (int k = 0; k < set.size(); ++k) {
            for (int l = 0; l < set.size(); ++l) {
                final String m = "k==" + k + " && l==" + l;
                assertTrue(m, set.get(k).sameContents(setb.get(k)));
                assertTrue(m, set.get(l).sameContents(setb.get(l)));
                Container c1 = set.get(k).andNot(set.get(l));
                c1.validate();
                Container c2 = setb.get(k).andNot(setb.get(l));
                c2.validate();
                assertTrue(m, c1.sameContents(c2));
            }
        }
    }


    @Test
    public void RunContainerVSRunContainerOR() {
        ArrayList<Container> set = new ArrayList<Container>();
        ArrayList<Container> setb = new ArrayList<Container>();
        getSetOfContainers(set, setb);
        for (int k = 0; k < set.size(); ++k) {
            for (int l = 0; l < set.size(); ++l) {
                final String m = "k==" + k + " && l==" + l;
                assertTrue(m, set.get(k).sameContents(setb.get(k)));
                assertTrue(m, set.get(l).sameContents(setb.get(l)));
                Container c1 = set.get(k).or(set.get(l));
                Container c2 = setb.get(k).or(setb.get(l));
                assertTrue(m, c1.sameContents(c2));
            }
        }
    }

    @Test
    public void RunContainerVSRunContainerXOR() {
        ArrayList<Container> set = new ArrayList<Container>();
        ArrayList<Container> setb = new ArrayList<Container>();
        getSetOfContainers(set, setb);
        for (int k = 0; k < set.size(); ++k) {
            for (int l = 0; l < set.size(); ++l) {
                final String m = "k==" + k + " && l==" + l;
                assertTrue(m, set.get(k).sameContents(setb.get(k)));
                assertTrue(m, set.get(l).sameContents(setb.get(l)));
                Container c1 = set.get(k).xor(set.get(l));
                Container c2 = setb.get(k).xor(setb.get(l));
                assertTrue(m, c1.sameContents(c2));
            }
        }
    }


    @Test
    public void safeor() {
        Container rc1 = new RunContainer();
        Container rc2 = new RunContainer();
        for (int i = 0; i < 100; ++i) {
            rc1 = rc1.iadd(i * 4, (i + 1) * 4 - 1);
            rc2 = rc2.iadd(i * 4 + 10000, (i + 1) * 4 - 1 + 10000);
        }
        Container x1 = rc1.or(rc2);
        Container x2 = rc1.ior(rc2);
        assertTrue(x1.sameContents(x2));
    }

    @Test
    public void orFullToRunContainer() {
        Container rc = Container.rangeOfOnes(0, 1 << 15);
        Container half = Container.singleRange(1 << 15, 1 << 16);
        Container result = rc.or(half);
        assertEquals(1 << 16, result.getCardinality());
        if (ImmutableContainer.ENABLED) {
            assertTrue(result instanceof SingleRangeContainer);
        }
    }

    @Test
    public void orFullToRunContainer2() {
        Container rc = Container.rangeOfOnes((1 << 10) - 200, 1 << 16);
        Container half = new ArrayContainer(0, 1 << 10);
        Container result = rc.or(half);
        assertEquals(1 << 16, result.getCardinality());
        if (ImmutableContainer.ENABLED) {
            assertTrue(result instanceof SingleRangeContainer);
        }
    }

    @Test
    public void orFullToRunContainer3() {
        Container rc = Container.rangeOfOnes(0, 1 << 15);
        Container half = Container.rangeOfOnes((1 << 15) - 200, 1 << 16);
        Container result = rc.or(half);
        assertEquals(1 << 16, result.getCardinality());
        if (ImmutableContainer.ENABLED) {
            assertTrue(result instanceof SingleRangeContainer);
        }
    }

    @Test
    public void select() {
        RunContainer container = new RunContainer();
        container.iset((short) 0);
        container.iset((short) 2);
        container.iset((short) 55);
        container.iset((short) 64);
        container.iset((short) 256);
        assertEquals(0, container.select(0));
        assertEquals(2, container.select(1));
        assertEquals(55, container.select(2));
        assertEquals(64, container.select(3));
        assertEquals(256, container.select(4));
    }

    @Test(expected = IllegalArgumentException.class)
    public void select2() {
        RunContainer container = new RunContainer();
        container.iset((short) 0);
        container.iset((short) 3);
        container.iset((short) 118);
        container.select(666);
    }

    @Test
    public void simpleIterator() {
        RunContainer x = new RunContainer();
        for (int k = 0; k < 100; ++k) {
            x = (RunContainer) x.iset((short) (k));
        }
        ShortIterator i = x.getShortIterator();
        for (int k = 0; k < 100; ++k) {
            assertTrue(i.hasNext());
            assertEquals(i.next(), (short) k);
        }
        assertFalse(i.hasNext());
    }

    @Test
    public void toBitmapOrArrayContainer() {
        RunContainer rc = new RunContainer();
        rc.iadd(0, DEFAULT_MAX_SIZE / 2);
        Container ac = rc.toBitmapOrArrayContainer(rc.getCardinality());
        assertTrue(ac instanceof ArrayContainer);
        assertEquals(DEFAULT_MAX_SIZE / 2, ac.getCardinality());
        for (short k = 0; k < DEFAULT_MAX_SIZE / 2; ++k) {
            assertTrue(ac.contains(k));
        }
        rc.iadd(DEFAULT_MAX_SIZE / 2, 2 * DEFAULT_MAX_SIZE);
        Container bc = rc.toBitmapOrArrayContainer(rc.getCardinality());
        assertTrue(bc instanceof BitmapContainer);
        assertEquals(2 * DEFAULT_MAX_SIZE, bc.getCardinality());
        for (short k = 0; k < 2 * DEFAULT_MAX_SIZE; ++k) {
            assertTrue(bc.contains(k));
        }
    }

    @Test
    public void union() {
        Container bc = new BitmapContainer();
        Container rc = new RunContainer();
        for (int k = 0; k < 100; ++k) {
            bc = bc.iset((short) (k * 10));
            rc = rc.iset((short) (k * 10 + 3));
        }
        Container union = rc.or(bc);
        assertEquals(200, union.getCardinality());
        for (int k = 0; k < 100; ++k) {
            assertTrue(union.contains((short) (k * 10)));
            assertTrue(union.contains((short) (k * 10 + 3)));
        }
        assertEquals(100, bc.getCardinality());
        assertEquals(100, rc.getCardinality());
    }


    @Test
    public void union2() {
        System.out.println("union2");
        ArrayContainer ac = new ArrayContainer();
        RunContainer rc = new RunContainer();
        for (int k = 0; k < 100; ++k) {
            ac = (ArrayContainer) ac.iset((short) (k * 10));
            rc = (RunContainer) rc.iset((short) (k * 10 + 3));
        }
        Container union = rc.or(ac);
        assertEquals(200, union.getCardinality());
        for (int k = 0; k < 100; ++k) {
            assertTrue(union.contains((short) (k * 10)));
            assertTrue(union.contains((short) (k * 10 + 3)));
        }
        assertEquals(100, ac.getCardinality());
        assertEquals(100, rc.getCardinality());
    }


    @Test
    public void xor() {
        Container bc = new BitmapContainer();
        Container rc = new RunContainer();
        for (int k = 0; k < 2 * DEFAULT_MAX_SIZE; ++k) {
            bc = bc.iset((short) (k * 10));
            bc = bc.iset((short) (k * 10 + 1));
            rc = rc.iset((short) (k * 10));
            rc = rc.iset((short) (k * 10 + 3));
        }
        Container result = rc.xor(bc);
        assertEquals(4 * DEFAULT_MAX_SIZE, result.getCardinality());
        for (int k = 0; k < 2 * DEFAULT_MAX_SIZE; ++k) {
            assertTrue(result.contains((short) (k * 10 + 1)));
            assertTrue(result.contains((short) (k * 10 + 3)));
        }
        assertEquals(4 * DEFAULT_MAX_SIZE, bc.getCardinality());
        assertEquals(4 * DEFAULT_MAX_SIZE, rc.getCardinality());
    }


    @Test
    public void xor_array() {
        Container bc = new ArrayContainer();
        Container rc = new RunContainer();
        for (int k = 0; k < 2 * DEFAULT_MAX_SIZE; ++k) {
            bc = bc.iset((short) (k * 10));
            bc = bc.iset((short) (k * 10 + 1));
            rc = rc.iset((short) (k * 10));
            rc = rc.iset((short) (k * 10 + 3));
        }
        Container result = rc.xor(bc);
        assertEquals(4 * DEFAULT_MAX_SIZE, result.getCardinality());
        for (int k = 0; k < 2 * DEFAULT_MAX_SIZE; ++k) {
            assertTrue(result.contains((short) (k * 10 + 1)));
            assertTrue(result.contains((short) (k * 10 + 3)));
        }
        assertEquals(4 * DEFAULT_MAX_SIZE, bc.getCardinality());
        assertEquals(4 * DEFAULT_MAX_SIZE, rc.getCardinality());
    }


    @Test
    public void xor_array_largecase_runcontainer_best() {
        Container bc = new ArrayContainer();
        Container rc = new RunContainer();
        for (int k = 0; k < 60; ++k) {
            for (int j = 0; j < 99; ++j) {
                rc = rc.iset((short) (k * 100 + j)); // most efficiently stored as runs
                bc = bc.iset((short) (k * 100 + 98)).iset((short) (k * 100 + 99));
            }
        }

        // size ordering preference for rc: run, bitmap, array

        assertTrue(bc instanceof ArrayContainer);
        assertTrue(rc instanceof RunContainer);
        int rcSize = rc.getCardinality();
        int bcSize = bc.getCardinality();

        Container result = rc.xor(bc);

        // input containers should not change (just check card)
        assertEquals(rcSize, rc.getCardinality());
        assertEquals(bcSize, bc.getCardinality());

        // each group of 60, we gain the missing 99th value but lose the 98th. Net wash
        assertEquals(rcSize, result.getCardinality());

        // a runcontainer would be, space-wise, best
        // but the code may (and does) opt to produce a bitmap

        // assertTrue( result instanceof RunContainer);

        for (int k = 0; k < 60; ++k) {
            for (int j = 0; j < 98; ++j) {
                assertTrue(result.contains((short) (k * 100 + j)));
            }
            assertTrue(result.contains((short) (k * 100 + 99)));
        }
    }


    @Test
    public void xor_array_mediumcase() {
        Container bc = new ArrayContainer();
        Container rc = new RunContainer();
        for (int k = 0; k < DEFAULT_MAX_SIZE / 6; ++k) {
            rc = rc.iset((short) (k * 10)); // most efficiently stored as runs
            rc = rc.iset((short) (k * 10 + 1));
            rc = rc.iset((short) (k * 10 + 2));
        }

        for (int k = 0; k < DEFAULT_MAX_SIZE / 12; ++k) {
            bc = bc.iset((short) (k * 10));
        }

        // size ordering preference for rc: run, array, bitmap

        assertTrue(bc instanceof ArrayContainer);
        assertTrue(rc instanceof RunContainer);
        int rcSize = rc.getCardinality();
        int bcSize = bc.getCardinality();

        Container result = rc.xor(bc);

        // input containers should not change (just check card)
        assertEquals(rcSize, rc.getCardinality());
        assertEquals(bcSize, bc.getCardinality());

        assertEquals(rcSize - bcSize, result.getCardinality());

        // The result really ought to be a runcontainer, by its size
        // however, as of test writing, the implementation
        // will have converted the result to an array container.
        // This is suboptimal, storagewise, but arguably not an error

        // assertTrue( result instanceof RunContainer);

        for (int k = 0; k < DEFAULT_MAX_SIZE / 12; ++k) {
            assertTrue(result.contains((short) (k * 10 + 1)));
            assertTrue(result.contains((short) (k * 10 + 2)));
        }

        for (int k = DEFAULT_MAX_SIZE / 12; k < DEFAULT_MAX_SIZE / 6; ++k) {
            assertTrue(result.contains((short) (k * 10 + 1)));
            assertTrue(result.contains((short) (k * 10 + 2)));
        }
    }


    @Test
    public void xor_array_smallcase() {
        Container bc = new ArrayContainer();
        Container rc = new RunContainer();
        for (int k = 0; k < DEFAULT_MAX_SIZE / 3; ++k) {
            rc = rc.iset((short) (k * 10)); // most efficiently stored as runs
            rc = rc.iset((short) (k * 10 + 1));
            rc = rc.iset((short) (k * 10 + 2));
            rc = rc.iset((short) (k * 10 + 3));
            rc = rc.iset((short) (k * 10 + 4));
        }

        // very small array.
        bc = bc.iset((short) 1).iset((short) 2).iset((short) 3).iset((short) 4).iset((short) 5);

        assertTrue(bc instanceof ArrayContainer);
        assertTrue(rc instanceof RunContainer);
        int rcSize = rc.getCardinality();
        int bcSize = bc.getCardinality();


        Container result = rc.xor(bc);

        // input containers should not change (just check card)
        assertEquals(rcSize, rc.getCardinality());
        assertEquals(bcSize, bc.getCardinality());

        assertEquals(rcSize - 3, result.getCardinality());
        assertTrue(result.contains((short) 5));
        assertTrue(result.contains((short) 0));


        for (int k = 1; k < DEFAULT_MAX_SIZE / 3; ++k) {
            for (int i = 0; i < 5; ++i) {
                assertTrue(result.contains((short) (k * 10 + i)));
            }
        }
    }

    @Test
    public void xor1() {
        Container bc = new BitmapContainer();
        Container rc = new RunContainer();
        rc.iset((short) 1);
        Container result = rc.xor(bc);
        assertEquals(1, result.getCardinality());
        assertTrue(result.contains((short) 1));
    }

    @Test
    public void xor1a() {
        Container bc = new ArrayContainer();
        Container rc = new RunContainer();
        rc.iset((short) 1);
        Container result = rc.xor(bc);
        assertEquals(1, result.getCardinality());
        assertTrue(result.contains((short) 1));
    }


    @Test
    public void xor2() {
        Container bc = new BitmapContainer();
        Container rc = new RunContainer();
        bc.iset((short) 1);
        Container result = rc.xor(bc);
        assertEquals(1, result.getCardinality());
        assertTrue(result.contains((short) 1));
    }


    @Test
    public void xor2a() {
        Container bc = new ArrayContainer();
        Container rc = new RunContainer();
        bc.iset((short) 1);
        Container result = rc.xor(bc);
        assertEquals(1, result.getCardinality());
        assertTrue(result.contains((short) 1));
    }


    @Test
    public void xor3() {
        Container bc = new BitmapContainer();
        Container rc = new RunContainer();
        rc.iset((short) 1);
        bc.iset((short) 1);
        Container result = rc.xor(bc);
        assertEquals(0, result.getCardinality());
    }

    @Test
    public void xor3a() {
        Container bc = new ArrayContainer();
        Container rc = new RunContainer();
        rc.iset((short) 1);
        bc.iset((short) 1);
        Container result = rc.xor(bc);
        assertEquals(0, result.getCardinality());
    }

    @Test
    public void xor4() {
        Container bc = new ArrayContainer();
        Container rc = new RunContainer();
        Container answer = new ArrayContainer();
        answer = answer.add(28203, 28214);
        rc = rc.add(28203, 28214);
        int[] data = {17739, 17740, 17945, 19077, 19278, 19407};
        for (int x : data) {
            answer = answer.iset((short) x);
            bc = bc.iset((short) x);
        }
        Container result = rc.xor(bc);
        assertSameContents(answer, result);
    }

    @Test
    public void xor5() {
        Container rc1 = new RunContainer();
        Container rc2 = new RunContainer();
        rc2.iadd(1, 13);
        assertEquals(rc2, rc1.xor(rc2));
        assertEquals(rc2, rc2.xor(rc1));
    }

    @Test
    public void intersects1() {
        Container ac = new ArrayContainer();
        ac = ac.iset((short) 1);
        ac = ac.iset((short) 7);
        ac = ac.iset((short) 13);
        ac = ac.iset((short) 666);

        Container rc = new RunContainer();

        assertFalse(rc.intersects(ac));
        assertFalse(ac.intersects(rc));

        rc = rc.iset((short) 1000);
        assertFalse(rc.intersects(ac));
        assertFalse(ac.intersects(rc));

        rc = rc.iunset((short) 1000);
        rc = rc.add(100, 200);
        rc = rc.add(300, 500);
        assertFalse(rc.intersects(ac));
        assertFalse(ac.intersects(rc));

        rc = rc.add(500, 1000);
        assertTrue(rc.intersects(ac));
        assertTrue(ac.intersects(rc));
    }

    @Test
    public void intersects2() {
        Container rc1 = new RunContainer();
        Container rc2 = new RunContainer();

        assertFalse(rc1.intersects(rc2));

        rc1 = rc1.add(10, 50);
        rc2 = rc2.add(100, 500);
        assertFalse(rc1.intersects(rc2));

        rc1 = rc1.add(60, 70);
        assertFalse(rc1.intersects(rc2));

        rc1 = rc1.add(600, 700);
        rc2 = rc2.add(800, 900);
        assertFalse(rc1.intersects(rc2));

        rc2 = rc2.add(30, 40);
        assertTrue(rc1.intersects(rc2));
    }

    @Test
    public void intersects3() {
        Container rc = new RunContainer();
        Container bc = new BitmapContainer();

        rc = rc.add(10, 50);
        bc = bc.add(100, 500);
        assertFalse(rc.intersects(bc));
    }

    @Test(expected = RuntimeException.class)
    public void constructor1() {
        new RunContainer(new short[] {1, 2, 10, 3}, 5);
    }

    @Test
    public void ensureCapacity() {
        RunContainer rc = new RunContainer();
        rc.iset((short) 13);
        assertTrue(rc.contains((short) 13));

        rc.ensureCapacity(10);
        assertTrue(rc.contains((short) 13));
    }

    @Test
    public void testRangeCardinality() {
        BitmapContainer bc = TestBitmapContainer.generateContainer((short) 100, (short) 10000, 5);
        RunContainer rc = new RunContainer(new short[] {7, 300, 400, 900, 1400, 2200}, 3);
        Container result = rc.or(bc);
        assertEquals(8677, result.getCardinality());
    }

    @Test
    public void testRangeCardinality2() {
        BitmapContainer bc = TestBitmapContainer.generateContainer((short) 100, (short) 10000, 5);
        bc.iset((short) 22345); // important case to have greater element than run container
        bc.iset(Short.MAX_VALUE);
        RunContainer rc = new RunContainer(new short[] {7, 300, 400, 900, 1400, 18000}, 3);
        assertTrue(rc.getCardinality() > ArrayContainer.DEFAULT_MAX_SIZE);
        Container result = rc.andNot(bc);
        assertEquals(11437, result.getCardinality());
    }

    @Test
    public void testRangeCardinality3() {
        BitmapContainer bc = TestBitmapContainer.generateContainer((short) 100, (short) 10000, 5);
        Container rc = new RunContainer(new short[] {7, 300, 400, 900, 1400, 5200}, 3);
        Container result = rc.and(bc);
        assertEquals(5046, result.getCardinality());
    }

    @Test
    public void testRangeCardinality4() {
        BitmapContainer bc = TestBitmapContainer.generateContainer((short) 100, (short) 10000, 5);
        RunContainer rc = new RunContainer(new short[] {7, 300, 400, 900, 1400, 2200}, 3);
        BitmapContainer result = (BitmapContainer) rc.xor(bc);
        assertEquals(6031, result.getCardinality());
    }

    @Test(expected = NoSuchElementException.class)
    public void testFirst_Empty() {
        new RunContainer().first();
    }

    @Test(expected = NoSuchElementException.class)
    public void testLast_Empty() {
        new RunContainer().last();
    }

    @Test
    public void testFirstLast() {
        Container rc = new RunContainer();
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
        Container rc = new RunContainer();
        Container subset = new BitmapContainer();
        assertTrue(rc.contains(subset));
    }

    @Test
    public void testContainsBitmapContainer_IncludeProperSubset() {
        Container rc = new RunContainer().add(0, 10);
        Container subset = new BitmapContainer().add(0, 9);
        assertTrue(rc.contains(subset));
    }


    @Test
    public void testContainsBitmapContainer_IncludeProperSubsetDifferentStart() {
        Container rc = new RunContainer().add(0, 10);
        Container subset = new BitmapContainer().add(1, 9);
        assertTrue(rc.contains(subset));
    }

    @Test
    public void testContainsBitmapContainer_ExcludeShiftedSet() {
        Container rc = new RunContainer().add(0, 10);
        Container subset = new BitmapContainer().add(2, 12);
        assertFalse(rc.contains(subset));
    }

    @Test
    public void testContainsBitmapContainer_IncludeSelf() {
        Container rc = new RunContainer().add(0, 10);
        Container subset = new BitmapContainer().add(0, 10);
        assertTrue(rc.contains(subset));
    }

    @Test
    public void testContainsBitmapContainer_ExcludeSuperSet() {
        Container rc = new RunContainer().add(0, 10);
        Container superset = new BitmapContainer().add(0, 20);
        assertFalse(rc.contains(superset));
    }

    @Test
    public void testContainsBitmapContainer_ExcludeDisJointSet() {
        Container rc = new RunContainer().add(0, 10);
        Container disjoint = new BitmapContainer().add(20, 40);
        assertFalse(rc.contains(disjoint));
        assertFalse(disjoint.contains(rc));
    }

    @Test
    public void testContainsRunContainer_EmptyContainsEmpty() {
        Container rc = new RunContainer();
        Container subset = new RunContainer();
        assertTrue(rc.contains(subset));
    }

    @Test
    public void testContainsRunContainer_IncludeProperSubset() {
        Container rc = new RunContainer().add(0, 10);
        Container subset = new RunContainer().add(0, 9);
        assertTrue(rc.contains(subset));
    }

    @Test
    public void testContainsRunContainer_IncludeSelf() {
        Container rc = new RunContainer().add(0, 10);
        Container subset = new RunContainer().add(0, 10);
        assertTrue(rc.contains(subset));
    }

    @Test
    public void testContainsRunContainer_ExcludeSuperSet() {
        Container rc = new RunContainer().add(0, 10);
        Container superset = new RunContainer().add(0, 20);
        assertFalse(rc.contains(superset));
    }

    @Test
    public void testContainsRunContainer_IncludeProperSubsetDifferentStart() {
        Container rc = new RunContainer().add(0, 10);
        Container subset = new RunContainer().add(1, 9);
        assertTrue(rc.contains(subset));
    }

    @Test
    public void testContainsRunContainer_ExcludeShiftedSet() {
        Container rc = new RunContainer().add(0, 10);
        Container subset = new RunContainer().add(2, 12);
        assertFalse(rc.contains(subset));
    }

    @Test
    public void testContainsRunContainer_ExcludeDisJointSet() {
        Container rc = new RunContainer().add(0, 10);
        Container disjoint = new RunContainer().add(20, 40);
        assertFalse(rc.contains(disjoint));
        assertFalse(disjoint.contains(rc));
    }

    @Test
    public void testContainsArrayContainer_EmptyContainsEmpty() {
        Container rc = new RunContainer();
        Container subset = new ArrayContainer();
        assertTrue(rc.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_IncludeProperSubset() {
        Container rc = new RunContainer().add(0, 10);
        Container subset = new ArrayContainer().add(0, 9);
        assertTrue(rc.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_IncludeProperSubsetDifferentStart() {
        Container rc = new RunContainer().add(0, 10);
        Container subset = new ArrayContainer().add(2, 9);
        assertTrue(rc.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_ExcludeShiftedSet() {
        Container rc = new RunContainer().add(0, 10);
        Container shifted = new ArrayContainer().add(2, 12);
        assertFalse(rc.contains(shifted));
    }

    @Test
    public void testContainsArrayContainer_IncludeSelf() {
        Container rc = new RunContainer().add(0, 10);
        Container subset = new ArrayContainer().add(0, 10);
        assertTrue(rc.contains(subset));
    }

    @Test
    public void testContainsArrayContainer_ExcludeSuperSet() {
        Container rc = new RunContainer().add(0, 10);
        Container superset = new ArrayContainer().add(0, 20);
        assertFalse(rc.contains(superset));
    }

    @Test
    public void testContainsArrayContainer_ExcludeDisJointSet() {
        Container rc = new RunContainer().add(0, 10);
        Container disjoint = new ArrayContainer().add(20, 40);
        assertFalse(rc.contains(disjoint));
        assertFalse(disjoint.contains(rc));

        disjoint = new ArrayContainer().iset((short) 512);
        assertFalse(rc.contains(disjoint));
        assertFalse(disjoint.contains(rc));

        rc = rc.add(12, 14).add(16, 18).add(20, 22);
        assertFalse(rc.contains(disjoint));
        assertFalse(disjoint.contains(rc));

        rc.trim();
        assertFalse(rc.contains(disjoint));
        assertFalse(disjoint.contains(rc));
    }

    @Test
    public void testEqualsArrayContainer_Equal() {
        Container rc = new RunContainer().add(0, 10);
        Container ac = new ArrayContainer().add(0, 10);
        assertTrue(rc.sameContents(ac));
        assertTrue(ac.sameContents(rc));
    }

    @Test
    public void testEqualsArrayContainer_NotEqual_ArrayLarger() {
        Container rc = new RunContainer().add(0, 10);
        Container ac = new ArrayContainer().add(0, 11);
        assertFalse(rc.sameContents(ac));
        assertFalse(ac.sameContents(rc));
    }

    @Test
    public void testEqualsArrayContainer_NotEqual_ArraySmaller() {
        Container rc = new RunContainer().add(0, 10);
        Container ac = new ArrayContainer().add(0, 9);
        assertFalse(rc.sameContents(ac));
        assertFalse(ac.sameContents(rc));
    }

    @Test
    public void testEqualsArrayContainer_NotEqual_ArrayShifted() {
        Container rc = new RunContainer().add(0, 10);
        Container ac = new ArrayContainer().add(1, 11);
        assertFalse(rc.sameContents(ac));
        assertFalse(ac.sameContents(rc));
    }

    @Test
    public void testEqualsArrayContainer_NotEqual_ArrayDiscontiguous() {
        Container rc = new RunContainer().add(0, 10);
        Container ac = new ArrayContainer().add(0, 11);
        ac.iflip((short) 9);
        assertFalse(rc.sameContents(ac));
        assertFalse(ac.sameContents(rc));
    }

    @Test
    public void testEquals_FullRunContainerWithArrayContainer() {
        Container full = new RunContainer().add(0, 1 << 16);
        assertNotEquals(full, new ArrayContainer().add(0, 10));
    }

    @Test
    public void testFullConstructor() {
        assertTrue(Container.full().isAllOnes());
    }

    @Test
    public void testRangeConstructor() {
        RunContainer c = new RunContainer(0, 1 << 16);
        assertTrue(c.isAllOnes());
        assertEquals(65536, c.getCardinality());
    }

    @Test
    public void testRangeConstructor2() {
        RunContainer c = new RunContainer(17, 1000);
        assertEquals(983, c.getCardinality());
    }

    @Test
    public void testRangeConstructor3() {
        RunContainer a = new RunContainer(17, 45679);
        RunContainer b = new RunContainer();
        b.iadd(17, 45679);
        assertSameContents(a, b);
    }

    @Test
    public void testRangeConstructor4() {
        RunContainer c = new RunContainer(0, 45679);
        assertEquals(45679, c.getCardinality());
    }

    @Test
    public void testSimpleCardinality() {
        RunContainer c = new RunContainer();
        c.iset((short) 1);
        c.iset((short) 17);
        assertEquals(2, c.getCardinality());
    }

    @Test
    public void testIntersectsWithRange() {
        Container container = new RunContainer().add(0, 10);
        assertTrue(container.intersects(0, 1));
        assertTrue(container.intersects(0, 101));
        assertTrue(container.intersects(0, 1 << 16));
        assertFalse(container.intersects(11, 1 << 16));
    }


    @Test
    public void testIntersectsWithRangeUnsigned() {
        Container container = new RunContainer().add(lower16Bits(-50), lower16Bits(-10));
        assertFalse(container.intersects(0, 1));
        assertTrue(container.intersects(0, lower16Bits(-40)));
        assertFalse(container.intersects(lower16Bits(-100), lower16Bits(-55)));
        assertFalse(container.intersects(-9, 1 << 16));
        assertTrue(container.intersects(11, 1 << 16));
    }


    @Test
    public void testIntersectsWithRangeManyRuns() {
        Container container = new RunContainer().add(0, 10).add(lower16Bits(-50), lower16Bits(-10));
        assertTrue(container.intersects(0, 1));
        assertTrue(container.intersects(0, 101));
        assertTrue(container.intersects(0, lower16Bits(-1)));
        assertTrue(container.intersects(11, lower16Bits(-1)));
        assertTrue(container.intersects(0, lower16Bits(-40)));
        assertFalse(container.intersects(lower16Bits(-100), lower16Bits(-55)));
        assertFalse(container.intersects(lower16Bits(-9), lower16Bits(-1)));
        assertTrue(container.intersects(11, 1 << 16));
    }

    @Test
    public void testContainsFull() {
        assertTrue(Container.full().contains(0, 1 << 16));
        assertFalse(Container.full().iflip((short) (1 << 15)).contains(0, 1 << 16));
    }

    @Test
    public void testContainsRange() {
        Container rc = new RunContainer().add(1, 100).add(5000, 10000);
        assertFalse(rc.contains(0, 100));
        assertFalse(rc.contains(0, 100000));
        assertTrue(rc.contains(1, 100));
        assertTrue(rc.contains(1, 99));
        assertTrue(rc.contains(2, 100));
        assertTrue(rc.contains(5000, 10000));
        assertTrue(rc.contains(5000, 9999));
        assertTrue(rc.contains(5001, 9999));
        assertTrue(rc.contains(5001, 10000));
        assertFalse(rc.contains(100, 5000));
        assertFalse(rc.contains(50, 5000));
        assertFalse(rc.contains(4000, 6000));
        assertFalse(rc.contains(10001, 20000));
    }

    @Test
    public void testContainsRange3() {
        Container rc = new RunContainer().add(1, 100)
                .add(300, 300)
                .add(400, 500)
                .add(502, 600)
                .add(700, 10000);
        assertFalse(rc.contains(0, 100));
        assertFalse(rc.contains(500, 600));
        assertFalse(rc.contains(501, 600));
        assertTrue(rc.contains(502, 600));
        assertFalse(rc.contains(600, 700));
        assertTrue(rc.contains(9999, 10000));
        assertFalse(rc.contains(9999, 10001));
    }

    @Test
    public void testNextValue() {
        RunContainer container = new RunContainer(new short[] {64, 64}, 1);
        assertEquals(64, container.nextValue((short) 0));
        assertEquals(64, container.nextValue((short) 64));
        assertEquals(65, container.nextValue((short) 65));
        assertEquals(128, container.nextValue((short) 128));
        assertEquals(-1, container.nextValue((short) 129));
    }

    @Test
    public void testNextSetBitBetweenRuns() {
        RunContainer container = new RunContainer(new short[] {64, 64, 256, 64}, 2);
        assertEquals(64, container.nextValue((short) 0));
        assertEquals(64, container.nextValue((short) 64));
        assertEquals(65, container.nextValue((short) 65));
        assertEquals(128, container.nextValue((short) 128));
        assertEquals(256, container.nextValue((short) 129));
        assertEquals(-1, container.nextValue((short) 512));
    }

    @Test
    public void testShortRangeIteratorSearch0() {
        final RunContainer container = new RunContainer(new short[] {64, 64, 256, 64}, 2);
        final SearchRangeIterator it = container.getShortRangeIterator(0);
        assertTrue(it.hasNext());
        it.next(); // current is now the first run.
        assertTrue(it.hasNext());
        it.next(); // current is now the second run.
        final int v = 261;
        final boolean a = it.advance(v);
        assertTrue(a);
        assertEquals(v, it.start());
        for (int i = 255; i <= 256 + 64; ++i) {
            if (!(it instanceof RunContainerRangeIterator)) {
                fail("wrong iterator type");
            }
            RunContainerRangeIterator it2 =
                    new RunContainerRangeIterator((RunContainerRangeIterator) it);
            final int ii = i;
            final ContainerUtil.TargetComparator t = (final int value) -> (ii - value);
            final boolean b = it2.search(t);
            final String m = "i=" + i;
            assertEquals(m, v <= i && i <= 256 + 64, b);
            if (i < v) {
                assertEquals(m, v, it2.start());
            } else if (i <= 256 + 64) {
                assertEquals(m, i, it2.start());
            } else {
                assertEquals(m, 256 + 64, it2.start());
            }
        }
    }

    @Test
    public void testIsAllOnes() {
        Container c = new RunContainer();
        int start = 0;
        for (int shift = 1; shift <= 16; ++shift) {
            final int end = (1 << shift) - 1;
            c = c.add(start, end);
            assertEquals("run", c.getContainerName());
            assertFalse(c.isAllOnes());
            start = end;
        }
        c = c.add((1 << 16) - 1, 1 << 16);
        assertEquals("run", c.getContainerName());
        assertTrue(c.isAllOnes());
    }

    @Test
    public void testRemoveRangeRegression0() {
        Container c = new RunContainer();
        c = c.iadd(1, 60);
        c = c.iadd(61, 68);
        c = c.iadd(69, 89);
        c = c.iadd(90, 112);
        c = c.iadd(113, 204);
        c = c.iadd(205, 226);
        c = c.iadd(227, 231);
        c = c.iadd(232, 233);
        c = c.iadd(234, 235);
        assertEquals("run", c.getContainerName());
        assertTrue(c.contains((short) 232));
        c = c.iremove(0, 233);
        assertFalse(c.contains((short) 232));
        assertEquals(1, c.getCardinality());
        assertTrue(c.contains((short) 234));
    }

    @Test
    public void testSelectContainerRegression0() {
        Container c = new RunContainer();
        for (int i = 0; i < 128; ++i) {
            c = c.iadd(4 * i, 4 * i + 3);
        }
        assertEquals("run", c.getContainerName());
        final int card = 128 * 3;
        assertEquals(card, c.getCardinality());
        final Container s = c.select(card - 12, card);
        assertEquals(12, s.getCardinality());
        final Container pre = c.select(0, card - 12);
        assertFalse(pre.intersects(s));
        assertEquals(c.getCardinality(), pre.or(s).getCardinality());
    }

    @Test
    public void testRunContainerOverlapsCases() {
        Container c = new RunContainer();
        c = c.iadd(3, 6 + 1);
        c = c.iadd(20, 22 + 1);
        assertEquals("run", c.getContainerName());
        assertFalse(c.overlapsRange(7, 19 + 1));
        assertTrue(c.overlapsRange(2, 3 + 1));
        assertTrue(c.overlapsRange(6, 7 + 1));
        assertTrue(c.overlapsRange(2, 7 + 1));
    }

    @Test
    public void testRunContainer32kBoundary() {
        Container c = new RunContainer();
        c = c.iadd(32 * 1024, 32 * 1024 + 3);
        c = c.iset((short) (32 * 1024 - 1));
        assertEquals("run", c.getContainerName());
        assertEquals(4, c.getCardinality());
        assertEquals(1, ((RunContainer) c).nbrruns);
    }

    @Test
    public void testReverseIteratorAdvanceRegression0() {
        Container c = new RunContainer();
        c = c.iadd(65534, 65535);
        assertEquals("run", c.getContainerName());
        final ShortAdvanceIterator it = c.getReverseShortIterator();
        assertTrue(it.hasNext());
        it.next();
        assertTrue(it.advance(65534));
    }

    @Test
    public void testSetWithHint() {
        Container c = new RunContainer(10, 15, 20, 25);
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
        Container c = new RunContainer(10, 15, 20, 25);
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
    public void testSetWithHint2() {
        final int v0 = 10;
        final int v1 = 30;
        Container c = new RunContainer(v0, v0 + 1, v1, v1 + 1);
        final int v2 = 13;
        final int v3 = 15;
        final PositionHint hint = new PositionHint();
        c = c.iset((short) v2, hint);
        c = c.iset((short) v3, hint);
        assertEquals(4, c.getCardinality());
        final Container expected = new ArrayContainer(new short[] {(short) v0, (short) v2, (short) v3, (short) v1});
        assertTrue(expected.sameContents(c));
    }

    @Test
    public void testUnsetWithHint2() {
        final int v0 = 10;
        final int v1 = 30;
        Container c = new RunContainer(v0, v0 + 1, v1, v1 + 1);
        final int v2 = 13;
        final int v3 = 15;
        c = c.iadd(v2, v2 + 1);
        c = c.iadd(v3, v3 + 1);
        final PositionHint hint = new PositionHint();
        c = c.iunset((short) v2, hint);
        c = c.iunset((short) v3, hint);
        assertEquals(2, c.getCardinality());
        final Container expected = new ArrayContainer(new short[] {(short) v0, (short) v1});
        assertTrue(expected.sameContents(c));
    }

    @Test
    public void testContainsBitmap() {
        Container c0 = new BitmapContainer();
        c0 = c0.add(10, 20);
        c0 = c0.add(30, 40);
        c0 = c0.iset((short) MAX_VALUE);
        Container c1 = new RunContainer(10, 40);
        assertFalse(c1.contains(c0));

        assertTrue(new RunContainer().contains(new BitmapContainer()));

        assertFalse(new RunContainer().contains(new BitmapContainer().iset((short) MAX_VALUE)));
    }

    public Supplier<Container> makeContainer() {
        return RunContainer::new;
    }

    public String containerName() {
        return "run";
    }

    private static int lower16Bits(int x) {
        return ((short) x) & Container.MAX_VALUE;
    }
}
