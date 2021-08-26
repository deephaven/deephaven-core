/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package io.deephaven.db.v2.utils.rsp.container;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.*;

import org.junit.Test;

/**
 * Various tests on Container and its subclasses, ArrayContainer and BitmapContainer
 */
@SuppressWarnings({"static-method"})
public class TestContainer {

    private final static Class<?>[] CONTAINER_TYPES =
            new Class[] {ArrayContainer.class, BitmapContainer.class, RunContainer.class};

    @Test
    public void testNames() {
        assertTrue(new BitmapContainer().getContainerName().equals("bitmap"));
        assertTrue(new ArrayContainer().getContainerName().equals("array"));
        assertTrue(new RunContainer().getContainerName().equals("run"));
        assertTrue(new SingletonContainer((short) 1).getContainerName().equals("singleton"));
        assertTrue(new TwoValuesContainer((short) 1, (short) 3).getContainerName().equals("twovalues"));
        assertTrue(new SingleRangeContainer(1, 3).getContainerName().equals("singlerange"));
        if (ImmutableContainer.ENABLED) {
            assertTrue(Container.empty().getContainerName().equals("empty"));
        }
    }

    public static boolean checkContent(Container c, short[] s) {
        ShortIterator si = c.getShortIterator();
        int ctr = 0;
        boolean fail = false;
        while (si.hasNext()) {
            if (ctr == s.length) {
                fail = true;
                break;
            }
            if (si.next() != s[ctr]) {
                fail = true;
                break;
            }
            ++ctr;
        }
        if (ctr != s.length) {
            fail = true;
        }
        if (fail) {
            System.out.print("fail, found ");
            si = c.getShortIterator();
            while (si.hasNext()) {
                System.out.print(" " + si.next());
            }
            System.out.print("\n expected ");
            for (final short s1 : s) {
                System.out.print(" " + s1);
            }
            System.out.println();
        }
        return !fail;
    }

    public static Container makeContainer(short[] ss) {
        Container c = new ArrayContainer();
        for (final short s : ss) {
            c = c.iset(s);
        }
        return c;
    }

    @Test
    public void and1() throws InstantiationException, IllegalAccessException {
        System.out.println("and1");
        for (Class<?> ct : CONTAINER_TYPES) {
            Container ac = (Container) ct.newInstance();
            ac.iset((short) 1);
            ac.iset((short) 3);
            ac.iset((short) 5);
            ac.iset((short) 50000);
            ac.iset((short) 50001);
            for (Class<?> ct1 : CONTAINER_TYPES) {
                Container ac1 = (Container) ct1.newInstance();
                Container result = ac.and(ac1);
                assertTrue(checkContent(result, new short[] {}));
                assertEquals(0, result.getCardinality());
            }
        }
    }

    @Test
    public void and2() throws InstantiationException, IllegalAccessException {
        System.out.println("and2");
        for (Class<?> ct : CONTAINER_TYPES) {
            Container ac = (Container) ct.newInstance();
            ac.iset((short) 1);
            for (Class<?> ct1 : CONTAINER_TYPES) {
                Container ac1 = (Container) ct1.newInstance();

                ac1.iset((short) 1);
                ac1.iset((short) 4);
                ac1.iset((short) 5);
                ac1.iset((short) 50000);
                ac1.iset((short) 50002);
                ac1.iset((short) 50003);
                ac1.iset((short) 50004);

                Container result = ac.and(ac1);
                assertTrue(checkContent(result, new short[] {1}));
            }
        }
    }

    @Test
    public void and3() throws InstantiationException, IllegalAccessException {
        System.out.println("and3");
        for (Class<?> ct : CONTAINER_TYPES) {
            Container ac = (Container) ct.newInstance();

            ac.iset((short) 1);
            ac.iset((short) 3);
            ac.iset((short) 5);
            ac.iset((short) 50000);
            ac.iset((short) 50001);

            // array ends first

            for (Class<?> ct1 : CONTAINER_TYPES) {
                Container ac1 = (Container) ct1.newInstance();

                ac1.iset((short) 1);
                ac1.iset((short) 4);
                ac1.iset((short) 5);
                ac1.iset((short) 50000);
                ac1.iset((short) 50002);
                ac1.iset((short) 50003);
                ac1.iset((short) 50004);

                Container result = ac.and(ac1);
                assertTrue(checkContent(result, new short[] {1, 5, (short) 50000}));
            }
        }
    }

    @Test
    public void and4() throws InstantiationException, IllegalAccessException {
        System.out.println("and4");
        for (Class<?> ct : CONTAINER_TYPES) {
            Container ac = (Container) ct.newInstance();

            ac.iset((short) 1);
            ac.iset((short) 3);
            ac.iset((short) 5);
            ac.iset((short) 50000);
            ac.iset((short) 50001);
            ac.iset((short) 50011);

            // iterator ends first

            for (Class<?> ct1 : CONTAINER_TYPES) {
                Container ac1 = (Container) ct1.newInstance();

                ac1.iset((short) 1);
                ac1.iset((short) 4);
                ac1.iset((short) 5);
                ac1.iset((short) 50000);
                ac1.iset((short) 50002);
                ac1.iset((short) 50003);
                ac1.iset((short) 50004);

                Container result = ac.and(ac1);
                assertTrue(checkContent(result, new short[] {1, 5, (short) 50000}));
            }
        }
    }

    @Test
    public void and5() throws InstantiationException, IllegalAccessException {
        System.out.println("and5");
        for (Class<?> ct : CONTAINER_TYPES) {
            Container ac = (Container) ct.newInstance();

            ac.iset((short) 1);
            ac.iset((short) 3);
            ac.iset((short) 5);
            ac.iset((short) 50000);
            ac.iset((short) 50001);

            // end together

            for (Class<?> ct1 : CONTAINER_TYPES) {
                Container ac1 = (Container) ct1.newInstance();

                ac1.iset((short) 1);
                ac1.iset((short) 4);
                ac1.iset((short) 5);
                ac1.iset((short) 50000);
                ac1.iset((short) 50001);

                Container result = ac.and(ac1);
                assertTrue(checkContent(result, new short[] {1, 5, (short) 50000, (short) 50001}));
            }
        }
    }

    @Test
    public void inotTest1() {
        // Array container, range is complete
        final short[] content = {1, 3, 5, 7, 9};
        Container c = makeContainer(content);
        c = c.inot(0, 65536);
        final short[] s = new short[65536 - content.length];
        int pos = 0;
        for (int i = 0; i < 65536; ++i) {
            if (Arrays.binarySearch(content, (short) i) < 0) {
                s[pos++] = (short) i;
            }
        }
        assertTrue(checkContent(c, s));
    }

    @Test
    public void inotTest10() {
        System.out.println("inotTest10");
        // Array container, inverting a range past any set bit
        final short[] content = new short[3];
        content[0] = 0;
        content[1] = 2;
        content[2] = 4;
        final Container c = makeContainer(content);
        final Container c1 = c.inot(65190, 65201);
        assertTrue(c1 instanceof ArrayContainer);
        assertEquals(14, c1.getCardinality());
        assertTrue(checkContent(c1,
                new short[] {0, 2, 4, (short) 65190, (short) 65191, (short) 65192, (short) 65193,
                        (short) 65194, (short) 65195, (short) 65196, (short) 65197, (short) 65198,
                        (short) 65199, (short) 65200}));
    }

    @Test
    public void inotTest2() {
        // Array and then Bitmap container, range is complete
        final short[] content = {1, 3, 5, 7, 9};
        Container c = makeContainer(content);
        c = c.inot(0, 65535);
        c = c.inot(0, 65535);
        assertTrue(checkContent(c, content));
    }

    @Test
    public void inotTest3() {
        // Bitmap to bitmap, full range

        Container c = new ArrayContainer();
        for (int i = 0; i < 65536; i += 2) {
            c = c.iset((short) i);
        }

        c = c.inot(0, 65536);
        assertTrue(c.contains((short) 3) && !c.contains((short) 4));
        assertEquals(32768, c.getCardinality());
        c = c.inot(0, 65536);
        for (int i = 0; i < 65536; i += 2) {
            assertTrue(c.contains((short) i) && !c.contains((short) (i + 1)));
        }
    }

    @Test
    public void inotTest4() {
        // Array container, range is partial, result stays array
        final short[] content = {1, 3, 5, 7, 9};
        Container c = makeContainer(content);
        c = c.inot(4, 1000);
        assertTrue(c instanceof ArrayContainer);
        assertEquals(999 - 4 + 1 - 3 + 2, c.getCardinality());
        c = c.inot(4, 1000); // back
        assertTrue(checkContent(c, content));
    }

    @Test
    public void inotTest5() {
        System.out.println("inotTest5");
        // Bitmap container, range is partial, result stays bitmap
        final short[] content = new short[32768 - 5];
        content[0] = 0;
        content[1] = 2;
        content[2] = 4;
        content[3] = 6;
        content[4] = 8;
        for (int i = 10; i <= 32767; ++i) {
            content[i - 10 + 5] = (short) i;
        }
        Container c = makeContainer(content);
        c = c.inot(4, 1000);
        assertTrue(c instanceof RunContainer);
        assertEquals(31773, c.getCardinality());
        c = c.inot(4, 1000); // back, as a bitmap
        assertTrue(c instanceof RunContainer);
        assertTrue(checkContent(c, content));

    }

    @Test
    public void inotTest6() {
        System.out.println("inotTest6");
        // Bitmap container, range is partial and in one word, result
        // stays bitmap
        final short[] content = new short[32768 - 5];
        content[0] = 0;
        content[1] = 2;
        content[2] = 4;
        content[3] = 6;
        content[4] = 8;
        for (int i = 10; i <= 32767; ++i) {
            content[i - 10 + 5] = (short) i;
        }
        Container c = makeContainer(content);
        c = c.inot(4, 9);
        assertTrue(c instanceof RunContainer);
        assertEquals(32762, c.getCardinality());
        c = c.inot(4, 9); // back, as a bitmap
        assertTrue(c instanceof RunContainer);
        assertTrue(checkContent(c, content));
    }

    @Test
    public void inotTest7() {
        System.out.println("inotTest7");
        // Bitmap container, range is partial, result flips to array
        final short[] content = new short[32768 - 5];
        content[0] = 0;
        content[1] = 2;
        content[2] = 4;
        content[3] = 6;
        content[4] = 8;
        for (int i = 10; i <= 32767; ++i) {
            content[i - 10 + 5] = (short) i;
        }
        Container c = makeContainer(content);
        c = c.inot(5, 31001);
        if (c.getCardinality() <= ArrayContainer.DEFAULT_MAX_SIZE) {
            assertTrue(c instanceof RunContainer);
        } else {
            assertTrue(c instanceof BitmapContainer);
        }
        assertEquals(1773, c.getCardinality());
        c = c.inot(5, 31001); // back, as a bitmap
        if (c.getCardinality() <= ArrayContainer.DEFAULT_MAX_SIZE) {
            assertTrue(c instanceof ArrayContainer);
        } else {
            assertTrue(c instanceof RunContainer);
        }
        assertTrue(checkContent(c, content));
    }

    // case requiring contraction of ArrayContainer.
    @Test
    public void inotTest8() {
        System.out.println("inotTest8");
        // Array container
        final short[] content = new short[21];
        for (int i = 0; i < 18; ++i) {
            content[i] = (short) i;
        }
        content[18] = 21;
        content[19] = 22;
        content[20] = 23;

        Container c = makeContainer(content);
        c = c.inot(5, 22);
        assertTrue(c instanceof ArrayContainer);

        assertEquals(10, c.getCardinality());
        c = c.inot(5, 22); // back, as a bitmap
        assertTrue(c instanceof ArrayContainer);
        assertTrue(checkContent(c, content));
    }

    // mostly same tests, except for not. (check original unaffected)
    @Test
    public void notTest1() {
        // Array container, range is complete
        final short[] content = {1, 3, 5, 7, 9};
        final Container c = makeContainer(content);
        final Container c1 = c.not(0, 65536);
        final short[] s = new short[65536 - content.length];
        int pos = 0;
        for (int i = 0; i < 65536; ++i) {
            if (Arrays.binarySearch(content, (short) i) < 0) {
                s[pos++] = (short) i;
            }
        }
        assertTrue(checkContent(c1, s));
        assertTrue(checkContent(c, content));
    }

    @Test
    public void notTest10() {
        System.out.println("notTest10");
        // Array container, inverting a range past any set bit
        // attempting to recreate a bug (but bug required extra space
        // in the array with just the right junk in it.
        final short[] content = new short[40];
        for (int i = 244; i <= 283; ++i) {
            content[i - 244] = (short) i;
        }
        final Container c = makeContainer(content);
        final Container c1 = c.not(51413, 51471);
        assertTrue(c1 instanceof ArrayContainer);
        assertEquals(40 + 58, c1.getCardinality());
        final short[] rightAns = new short[98];
        for (int i = 244; i <= 283; ++i) {
            rightAns[i - 244] = (short) i;
        }
        for (int i = 51413; i <= 51470; ++i) {
            rightAns[i - 51413 + 40] = (short) i;
        }

        assertTrue(checkContent(c1, rightAns));
    }

    @Test
    public void notTest11() {
        System.out.println("notTest11");
        // Array container, inverting a range before any set bit
        // attempting to recreate a bug (but required extra space
        // in the array with the right junk in it.
        final short[] content = new short[40];
        for (int i = 244; i <= 283; ++i) {
            content[i - 244] = (short) i;
        }
        final Container c = makeContainer(content);
        final Container c1 = c.not(1, 59);
        assertTrue(c1 instanceof ArrayContainer);
        assertEquals(40 + 58, c1.getCardinality());
        final short[] rightAns = new short[98];
        for (int i = 1; i <= 58; ++i) {
            rightAns[i - 1] = (short) i;
        }
        for (int i = 244; i <= 283; ++i) {
            rightAns[i - 244 + 58] = (short) i;
        }

        assertTrue(checkContent(c1, rightAns));
    }

    @Test
    public void notTest2() {
        // Array and then Bitmap container, range is complete
        final short[] content = {1, 3, 5, 7, 9};
        final Container c = makeContainer(content);
        final Container c1 = c.not(0, 65535);
        final Container c2 = c1.not(0, 65535);
        assertTrue(checkContent(c2, content));
    }

    @Test
    public void notTest3() {
        // Bitmap to bitmap, full range

        Container c = new ArrayContainer();
        for (int i = 0; i < 65536; i += 2) {
            c = c.iset((short) i);
        }

        final Container c1 = c.not(0, 65536);
        assertTrue(c1.contains((short) 3) && !c1.contains((short) 4));
        assertEquals(32768, c1.getCardinality());
        final Container c2 = c1.not(0, 65536);
        for (int i = 0; i < 65536; i += 2) {
            assertTrue(c2.contains((short) i) && !c2.contains((short) (i + 1)));
        }
    }

    @Test
    public void notTest4() {
        System.out.println("notTest4");
        // Array container, range is partial, result stays array
        final short[] content = {1, 3, 5, 7, 9};
        final Container c = makeContainer(content);
        final Container c1 = c.not(4, 1000);
        assertTrue(c1 instanceof ArrayContainer);
        assertEquals(999 - 4 + 1 - 3 + 2, c1.getCardinality());
        final Container c2 = c1.not(4, 1000); // back
        assertTrue(checkContent(c2, content));
    }

    @Test
    public void notTest5() {
        System.out.println("notTest5");
        // Bitmap container, range is partial, result stays bitmap
        final short[] content = new short[32768 - 5];
        content[0] = 0;
        content[1] = 2;
        content[2] = 4;
        content[3] = 6;
        content[4] = 8;
        for (int i = 10; i <= 32767; ++i) {
            content[i - 10 + 5] = (short) i;
        }
        final Container c = makeContainer(content);
        final Container c1 = c.not(4, 1000);
        assertTrue(c1 instanceof RunContainer);
        assertEquals(31773, c1.getCardinality());
        final Container c2 = c1.not(4, 1000); // back, as a bitmap
        assertTrue(c2 instanceof RunContainer);
        assertTrue(checkContent(c2, content));
    }

    @Test
    public void notTest6() {
        System.out.println("notTest6");
        // Bitmap container, range is partial and in one word, result
        // stays bitmap
        final short[] content = new short[32768 - 5];
        content[0] = 0;
        content[1] = 2;
        content[2] = 4;
        content[3] = 6;
        content[4] = 8;
        for (int i = 10; i <= 32767; ++i) {
            content[i - 10 + 5] = (short) i;
        }
        final Container c = makeContainer(content);
        final Container c1 = c.not(4, 9);
        assertTrue(c1 instanceof RunContainer);
        assertEquals(32762, c1.getCardinality());
        final Container c2 = c1.not(4, 9); // back, as a bitmap
        assertTrue(c2 instanceof RunContainer);
        assertTrue(checkContent(c2, content));
    }

    @Test
    public void notTest7() {
        System.out.println("notTest7");
        // Bitmap container, range is partial, result flips to array
        final short[] content = new short[32768 - 5];
        content[0] = 0;
        content[1] = 2;
        content[2] = 4;
        content[3] = 6;
        content[4] = 8;
        for (int i = 10; i <= 32767; ++i) {
            content[i - 10 + 5] = (short) i;
        }
        final Container c = makeContainer(content);
        final Container c1 = c.not(5, 31001);
        if (c1.getCardinality() <= ArrayContainer.DEFAULT_MAX_SIZE) {
            assertTrue(c1 instanceof RunContainer);
        } else {
            assertTrue(c1 instanceof BitmapContainer);
        }
        assertEquals(1773, c1.getCardinality());
        final Container c2 = c1.not(5, 31001); // back, as a bitmap
        if (c2.getCardinality() <= ArrayContainer.DEFAULT_MAX_SIZE) {
            assertTrue(c2 instanceof ArrayContainer);
        } else {
            assertTrue(c2 instanceof RunContainer);
        }
        assertTrue(checkContent(c2, content));
    }

    @Test
    public void notTest8() {
        System.out.println("notTest8");
        // Bitmap container, range is partial on the lower end
        final short[] content = new short[32768 - 5];
        content[0] = 0;
        content[1] = 2;
        content[2] = 4;
        content[3] = 6;
        content[4] = 8;
        for (int i = 10; i <= 32767; ++i) {
            content[i - 10 + 5] = (short) i;
        }
        final Container c = makeContainer(content);
        final Container c1 = c.not(4, 65536);
        assertTrue(c1 instanceof RunContainer);
        assertEquals(32773, c1.getCardinality());
        final Container c2 = c1.not(4, 65536); // back, as a bitmap
        assertTrue(c2 instanceof RunContainer);
        assertTrue(checkContent(c2, content));
    }

    @Test
    public void notTest9() {
        System.out.println("notTest9");
        // Bitmap container, range is partial on the upper end, not
        // single word
        final short[] content = new short[32768 - 5];
        content[0] = 0;
        content[1] = 2;
        content[2] = 4;
        content[3] = 6;
        content[4] = 8;
        for (int i = 10; i <= 32767; ++i) {
            content[i - 10 + 5] = (short) i;
        }
        final Container c = makeContainer(content);
        final Container c1 = c.not(0, 65201);
        assertTrue(c1 instanceof RunContainer);
        assertEquals(32438, c1.getCardinality());
        final Container c2 = c1.not(0, 65201); // back, as a bitmap
        assertTrue(c2 instanceof RunContainer);
        assertTrue(checkContent(c2, content));
    }


    @Test
    public void numberOfRuns() {
        short[] positions = {3, 4, 5, 10, 11, 13, 15, 62, 63, 64, 65};
        Container ac = new ArrayContainer();
        Container bc = new BitmapContainer();
        Container rc = new RunContainer();
        for (short position : positions) {
            ac.iset(position);
            bc.iset(position);
            rc.iset(position);
        }
        assertEquals(rc.numberOfRuns(), ac.numberOfRuns());
        assertEquals(rc.numberOfRuns(), bc.numberOfRuns());
    }


    @Test
    public void numberOfRuns1() {
        System.out.println("numberOfRuns1");
        Random r = new Random(12345);

        int j = 0;
        for (double density = 0.001; density < 0.8; density *= 2) {
            ++j;
            ArrayList<Integer> values = new ArrayList<Integer>();
            for (int i = 0; i < 65536; ++i) {
                if (r.nextDouble() < density) {
                    values.add(i);
                }
            }
            Integer[] positions = values.toArray(new Integer[0]);
            Container ac = new ArrayContainer(); // at high density becomes Bitmap
            Container bc = new BitmapContainer();
            Container rc = new RunContainer();
            for (int position : positions) {
                ac = ac.iset((short) position);
                bc = bc.iset((short) position);
                rc = rc.iset((short) position);
            }
            final int rruns = rc.numberOfRuns();
            assertEquals("j==" + j, rruns, ac.numberOfRuns());
            assertEquals("j==" + j, rruns, bc.numberOfRuns());
        }
    }


    @Test
    public void or1() {
        System.out.println("or1");
        ArrayContainer ac = new ArrayContainer();
        ac.iset((short) 1);
        ac.iset((short) 3);
        ac.iset((short) 5);
        ac.iset((short) 50000);
        ac.iset((short) 50001);

        ArrayContainer ac1 = new ArrayContainer(); // empty iterator
        Container result = ac.or(ac1.getShortIterator());
        assertTrue(checkContent(result, new short[] {1, 3, 5, (short) 50000, (short) 50001}));
    }


    @Test
    public void or2() {
        System.out.println("or2");
        ArrayContainer ac = new ArrayContainer();
        // empty array

        ArrayContainer ac1 = new ArrayContainer();
        ac1.iset((short) 1);
        ac1.iset((short) 4);
        ac1.iset((short) 5);
        ac1.iset((short) 50000);
        ac1.iset((short) 50002);
        ac1.iset((short) 50003);
        ac1.iset((short) 50004);

        Container result = ac.or(ac1.getShortIterator());
        assertTrue(checkContent(result,
                new short[] {1, 4, 5, (short) 50000, (short) 50002, (short) 50003, (short) 50004}));
    }

    @Test
    public void or3() {
        System.out.println("or3");
        ArrayContainer ac = new ArrayContainer();
        ac.iset((short) 1);
        ac.iset((short) 3);
        ac.iset((short) 5);
        ac.iset((short) 50000);
        ac.iset((short) 50001);

        // array ends first

        ArrayContainer ac1 = new ArrayContainer();
        ac1.iset((short) 1);
        ac1.iset((short) 4);
        ac1.iset((short) 5);
        ac1.iset((short) 50000);
        ac1.iset((short) 50002);
        ac1.iset((short) 50003);
        ac1.iset((short) 50004);

        Container result = ac.or(ac1.getShortIterator());
        assertTrue(checkContent(result, new short[] {1, 3, 4, 5, (short) 50000, (short) 50001,
                (short) 50002, (short) 50003, (short) 50004}));
    }

    @Test
    public void or4() {
        System.out.println("or4");
        ArrayContainer ac = new ArrayContainer();
        ac.iset((short) 1);
        ac.iset((short) 3);
        ac.iset((short) 5);
        ac.iset((short) 50000);
        ac.iset((short) 50001);
        ac.iset((short) 50011);

        // iterator ends first

        ArrayContainer ac1 = new ArrayContainer();
        ac1.iset((short) 1);
        ac1.iset((short) 4);
        ac1.iset((short) 5);
        ac1.iset((short) 50000);
        ac1.iset((short) 50002);
        ac1.iset((short) 50003);
        ac1.iset((short) 50004);


        Container result = ac.or(ac1.getShortIterator());
        assertTrue(checkContent(result, new short[] {1, 3, 4, 5, (short) 50000, (short) 50001,
                (short) 50002, (short) 50003, (short) 50004, (short) 50011}));
    }


    @Test
    public void or5() {
        System.out.println("or5");
        ArrayContainer ac = new ArrayContainer();
        ac.iset((short) 1);
        ac.iset((short) 3);
        ac.iset((short) 5);
        ac.iset((short) 50000);
        ac.iset((short) 50001);

        // end together

        ArrayContainer ac1 = new ArrayContainer();
        ac1.iset((short) 1);
        ac1.iset((short) 4);
        ac1.iset((short) 5);
        ac1.iset((short) 50000);
        ac1.iset((short) 50001);

        Container result = ac.or(ac1.getShortIterator());
        assertTrue(checkContent(result, new short[] {1, 3, 4, 5, (short) 50000, (short) 50001}));
    }

    @Test
    public void rangeOfOnesTest1() {
        final Container c = Container.rangeOfOnes(4, 11); // sparse
        // assertTrue(c instanceof ArrayContainer);
        assertEquals(10 - 4 + 1, c.getCardinality());
        assertTrue(checkContent(c, new short[] {4, 5, 6, 7, 8, 9, 10}));
    }


    @Test
    public void rangeOfOnesTest2() {
        final Container c = Container.rangeOfOnes(1000, 35001); // dense
        // assertTrue(c instanceof BitmapContainer);
        assertEquals(35000 - 1000 + 1, c.getCardinality());
    }


    @Test
    public void rangeOfOnesTest2A() {
        final Container c = Container.rangeOfOnes(1000, 35001); // dense
        final short[] s = new short[35000 - 1000 + 1];
        for (int i = 1000; i <= 35000; ++i) {
            s[i - 1000] = (short) i;
        }
        assertTrue(checkContent(c, s));
    }


    @Test
    public void rangeOfOnesTest3() {
        // bdry cases
        Container.rangeOfOnes(1, ArrayContainer.DEFAULT_MAX_SIZE);
    }

    @Test
    public void rangeOfOnesTest4() {
        Container.rangeOfOnes(1, ArrayContainer.DEFAULT_MAX_SIZE + 2);
    }

    @Test
    public void testRunOptimize1() {
        ArrayContainer ac = new ArrayContainer();
        for (short s : new short[] {1, 2, 3, 4, 5, 6, 7, 8, 9, (short) 50000, (short) 50001}) {
            ac.iset(s);
        }
        Container c = ac.runOptimize();
        assertTrue(c instanceof RunContainer);
        TestContainerBase.assertSameContents(ac, c);
    }

    @Test
    public void testRunOptimize2() {
        BitmapContainer bc = new BitmapContainer();
        for (int i = 0; i < 40000; ++i) {
            bc.iset((short) i);
        }
        Container c = bc.runOptimize();
        assertTrue(c instanceof RunContainer);
        TestContainerBase.assertSameContents(bc, c);
    }


    @Test
    public void testRunOptimize2A() {
        BitmapContainer bc = new BitmapContainer();
        for (int i = 0; i < 40000; i += 2) {
            bc.iset((short) i);
        }
        Container c = bc.runOptimize();
        assertTrue(c instanceof BitmapContainer);
        assertSame(c, bc);
    }

    @Test
    public void testRunOptimize3() {
        RunContainer rc = new RunContainer();
        for (short s : new short[] {1, 2, 3, 4, 5, 6, 7, 8, 9, (short) 50000, (short) 50001}) {
            rc.iset(s);
        }
        Container c = rc.runOptimize();
        assertTrue(c instanceof RunContainer);
        assertSame(c, rc);
    }

    @Test
    public void testRunOptimize3A() {
        RunContainer rc = new RunContainer();
        for (short s : new short[] {1, 3, 5, 7, 9, 11, 17, 21, (short) 50000, (short) 50002}) {
            rc.iset(s);
        }
        Container c = rc.runOptimize();
        assertTrue(c instanceof ArrayContainer);
        TestContainerBase.assertSameContents(c, rc);
    }

    @Test
    public void testRunOptimize3B() {
        RunContainer rc = new RunContainer();
        for (short i = 100; i < 30000; i += 2) {
            rc.iset(i);
        }
        Container c = rc.runOptimize();
        assertTrue(c instanceof ArrayContainer);
        TestContainerBase.assertSameContents(c, rc);
    }

    @Test
    public void transitionTest() {
        Container c = new ArrayContainer();
        for (int i = 0; i < ArrayContainer.DEFAULT_MAX_SIZE; ++i) {
            c = c.iset((short) i);
        }
        assertEquals(c.getCardinality(), ArrayContainer.DEFAULT_MAX_SIZE);
        assertTrue(c instanceof ArrayContainer);
        for (int i = 0; i < ArrayContainer.DEFAULT_MAX_SIZE; ++i) {
            c = c.iset((short) i);
        }
        assertEquals(c.getCardinality(), ArrayContainer.DEFAULT_MAX_SIZE);
        assertTrue(c instanceof ArrayContainer);
        c = c.iset((short) ArrayContainer.DEFAULT_MAX_SIZE);
        assertEquals(c.getCardinality(), ArrayContainer.DEFAULT_MAX_SIZE + 1);
        assertTrue(c instanceof RunContainer);
        c = c.iunset((short) ArrayContainer.DEFAULT_MAX_SIZE);
        assertEquals(c.getCardinality(), ArrayContainer.DEFAULT_MAX_SIZE);
        assertTrue(c instanceof RunContainer);
    }

    @Test
    public void xor1() {
        System.out.println("xor1");
        ArrayContainer ac = new ArrayContainer();
        ac.iset((short) 1);
        ac.iset((short) 3);
        ac.iset((short) 5);
        ac.iset((short) 50000);
        ac.iset((short) 50001);

        ArrayContainer ac1 = new ArrayContainer(); // empty iterator
        Container result = ac.xor(ac1.getShortIterator());
        assertTrue(checkContent(result, new short[] {1, 3, 5, (short) 50000, (short) 50001}));
    }

    @Test
    public void xor2() {
        System.out.println("xor2");
        ArrayContainer ac = new ArrayContainer();
        // empty array

        ArrayContainer ac1 = new ArrayContainer();
        ac1.iset((short) 1);
        ac1.iset((short) 4);
        ac1.iset((short) 5);
        ac1.iset((short) 50000);
        ac1.iset((short) 50002);
        ac1.iset((short) 50003);
        ac1.iset((short) 50004);

        Container result = ac.xor(ac1.getShortIterator());
        assertTrue(checkContent(result,
                new short[] {1, 4, 5, (short) 50000, (short) 50002, (short) 50003, (short) 50004}));
    }


    @Test
    public void xor3() {
        System.out.println("xor3");
        ArrayContainer ac = new ArrayContainer();
        ac.iset((short) 1);
        ac.iset((short) 3);
        ac.iset((short) 5);
        ac.iset((short) 50000);
        ac.iset((short) 50001);

        // array ends first

        ArrayContainer ac1 = new ArrayContainer();
        ac1.iset((short) 1);
        ac1.iset((short) 4);
        ac1.iset((short) 5);
        ac1.iset((short) 50000);
        ac1.iset((short) 50002);
        ac1.iset((short) 50003);
        ac1.iset((short) 50004);

        Container result = ac.xor(ac1.getShortIterator());
        assertTrue(checkContent(result,
                new short[] {3, 4, (short) 50001, (short) 50002, (short) 50003, (short) 50004}));
    }

    @Test
    public void testConsistentToString() {
        ArrayContainer ac = new ArrayContainer();
        BitmapContainer bc = new BitmapContainer();
        RunContainer rc = new RunContainer();
        for (short i : new short[] {0, 2, 17, Short.MAX_VALUE, -3, -1}) {
            ac.iset(i);
            bc.iset(i);
            rc.iset(i);
        }
        String expected = "{ 0,2,17,32767,65533,65535 }";

        assertEquals(expected, ac.toString());
        assertEquals(expected, bc.toString());
        String normalizedRCstr = rc.toString()
                .replaceAll("\\d+\\]\\[", "")
                .replace('[', '{')
                .replaceFirst(",\\d+\\]", "}");
        assertEquals(expected, normalizedRCstr);
    }

    @Test
    public void xor4() {
        System.out.println("xor4");
        ArrayContainer ac = new ArrayContainer();
        ac.iset((short) 1);
        ac.iset((short) 3);
        ac.iset((short) 5);
        ac.iset((short) 50000);
        ac.iset((short) 50001);
        ac.iset((short) 50011);

        // iterator ends first

        ArrayContainer ac1 = new ArrayContainer();
        ac1.iset((short) 1);
        ac1.iset((short) 4);
        ac1.iset((short) 5);
        ac1.iset((short) 50000);
        ac1.iset((short) 50002);
        ac1.iset((short) 50003);
        ac1.iset((short) 50004);


        Container result = ac.xor(ac1.getShortIterator());
        assertTrue(checkContent(result, new short[] {3, 4, (short) 50001, (short) 50002, (short) 50003,
                (short) 50004, (short) 50011}));
    }


    @Test
    public void xor5() {
        System.out.println("xor5");
        ArrayContainer ac = new ArrayContainer();
        ac.iset((short) 1);
        ac.iset((short) 3);
        ac.iset((short) 5);
        ac.iset((short) 50000);
        ac.iset((short) 50001);

        // end together

        ArrayContainer ac1 = new ArrayContainer();
        ac1.iset((short) 1);
        ac1.iset((short) 4);
        ac1.iset((short) 5);
        ac1.iset((short) 50000);
        ac1.iset((short) 50001);

        Container result = ac.xor(ac1.getShortIterator());
        assertTrue(checkContent(result, new short[] {3, 4}));
    }

    @Test
    public void testSubsetOf() {
        ContainerTestCommon.BoolContainerOp testOp = (Container c1, Container c2) -> c1.subsetOf(c2);
        ContainerTestCommon.BoolContainerOp validateOp =
                (Container c1, Container c2) -> c1.and(c2).xor(c1).getCardinality() == 0;
        ContainerTestCommon.doTestBoolOp(testOp, validateOp);
    }
}
