/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import junit.framework.TestCase;

public class ArrayUtilTest extends TestCase {

    public void testPushArray() {
        String[] sa = ArrayUtil.pushArray("foo", null, String.class);
        assertFalse(sa == null);
        assertEquals(String.class, sa.getClass().getComponentType());
        assertEquals(1, sa.length);
        assertEquals("foo", sa[0]);

        String[] sa2 = ArrayUtil.pushArray("foo", sa, String.class);
        assertFalse(sa2 == sa);
        assertEquals(String.class, sa.getClass().getComponentType());
        assertEquals(2, sa2.length);
        assertEquals("foo", sa2[0]);
        assertEquals("foo", sa2[1]);

        String[] sa3 = ArrayUtil.pushArray("bar", sa2, String.class);
        assertFalse(sa3 == sa);
        assertFalse(sa3 == sa2);
        assertEquals(String.class, sa.getClass().getComponentType());
        assertEquals(3, sa3.length);
        assertEquals("foo", sa3[0]);
        assertEquals("foo", sa3[1]);
        assertEquals("bar", sa3[2]);
    }

    public void testDeleteArrayPos() {
        String[] sa;

        sa = ArrayUtil.deleteArrayPos(0, new String[] {"a"});
        assertTrue(sa == null);

        // ---

        sa = ArrayUtil.deleteArrayPos(0, new String[] {"a", "b"});
        assertFalse(sa == null);
        assertEquals(String.class, sa.getClass().getComponentType());
        assertEquals(1, sa.length);
        assertEquals("b", sa[0]);

        sa = ArrayUtil.deleteArrayPos(1, new String[] {"a", "b"});
        assertFalse(sa == null);
        assertEquals(String.class, sa.getClass().getComponentType());
        assertEquals(1, sa.length);
        assertEquals("a", sa[0]);

        // ---

        sa = ArrayUtil.deleteArrayPos(0, new String[] {"a", "b", "c"});
        assertFalse(sa == null);
        assertEquals(String.class, sa.getClass().getComponentType());
        assertEquals(2, sa.length);
        assertEquals("b", sa[0]);
        assertEquals("c", sa[1]);

        sa = ArrayUtil.deleteArrayPos(1, new String[] {"a", "b", "c"});
        assertFalse(sa == null);
        assertEquals(String.class, sa.getClass().getComponentType());
        assertEquals(2, sa.length);
        assertEquals("a", sa[0]);
        assertEquals("c", sa[1]);

        sa = ArrayUtil.deleteArrayPos(2, new String[] {"a", "b", "c"});
        assertFalse(sa == null);
        assertEquals(String.class, sa.getClass().getComponentType());
        assertEquals(2, sa.length);
        assertEquals("a", sa[0]);
        assertEquals("b", sa[1]);

        // ---

        int[] ia;
        ia = ArrayUtil.deleteArrayPos(0, new int[] {1, 2, 3});
        assertFalse(ia == null);
        assertEquals(int.class, ia.getClass().getComponentType());
        assertEquals(2, ia.length);
        assertEquals(2, ia[0]);
        assertEquals(3, ia[1]);

        ia = ArrayUtil.deleteArrayPos(1, new int[] {1, 2, 3});
        assertFalse(ia == null);
        assertEquals(int.class, ia.getClass().getComponentType());
        assertEquals(2, ia.length);
        assertEquals(1, ia[0]);
        assertEquals(3, ia[1]);

        ia = ArrayUtil.deleteArrayPos(2, new int[] {1, 2, 3});
        assertFalse(ia == null);
        assertEquals(int.class, ia.getClass().getComponentType());
        assertEquals(2, ia.length);
        assertEquals(1, ia[0]);
        assertEquals(2, ia[1]);

        // ---

        double[] da;
        da = ArrayUtil.deleteArrayPos(0, new double[] {1.0, 2.0, 3.0});
        assertFalse(da == null);
        assertEquals(double.class, da.getClass().getComponentType());
        assertEquals(2, da.length);
        assertEquals(2.0, da[0]);
        assertEquals(3.0, da[1]);

        da = ArrayUtil.deleteArrayPos(1, new double[] {1.0, 2.0, 3.0});
        assertFalse(da == null);
        assertEquals(double.class, da.getClass().getComponentType());
        assertEquals(2, da.length);
        assertEquals(1.0, da[0]);
        assertEquals(3.0, da[1]);

        da = ArrayUtil.deleteArrayPos(2, new double[] {1.0, 2.0, 3.0});
        assertFalse(da == null);
        assertEquals(double.class, da.getClass().getComponentType());
        assertEquals(2, da.length);
        assertEquals(1.0, da[0]);
        assertEquals(2.0, da[1]);
    }

    public void testAddToArray() {
        String[] sa = ArrayUtil.addToArray("foo", null, String.class);
        assertFalse(sa == null);
        assertEquals(String.class, sa.getClass().getComponentType());
        assertEquals(1, sa.length);
        assertEquals("foo", sa[0]);

        String[] sa2 = ArrayUtil.addToArray("foo", sa, String.class);
        assertTrue(sa2 == sa);

        String[] sa3 = ArrayUtil.addToArray("bar", sa, String.class);
        assertFalse(sa3 == sa);
        assertEquals(String.class, sa.getClass().getComponentType());
        assertEquals(2, sa3.length);
        assertEquals("foo", sa3[0]);
        assertEquals("bar", sa3[1]);
    }

    public void testInsert() {
        String[] sa = ArrayUtil.insert(new String[0], 0, "foo", String.class);
        assertFalse(sa == null);
        assertEquals(String.class, sa.getClass().getComponentType());
        assertEquals(1, sa.length);
        assertEquals("foo", sa[0]);

        String[] sa_last = ArrayUtil.insert(sa, 1, "bar", String.class);
        assertFalse(sa_last == sa);
        assertEquals(String.class, sa_last.getClass().getComponentType());
        assertEquals(2, sa_last.length);
        assertEquals("foo", sa_last[0]);
        assertEquals("bar", sa_last[1]);

        String[] sa_first = ArrayUtil.insert(sa, 0, "bar", String.class);
        assertFalse(sa_first == sa);
        assertEquals(String.class, sa_first.getClass().getComponentType());
        assertEquals(2, sa_first.length);
        assertEquals("bar", sa_first[0]);
        assertEquals("foo", sa_first[1]);

        String[] sa_third = ArrayUtil.insert(sa_last, 1, "third", String.class);
        assertFalse(sa_third == sa_last);
        assertEquals(String.class, sa_third.getClass().getComponentType());
        assertEquals(3, sa_third.length);
        assertEquals("foo", sa_third[0]);
        assertEquals("third", sa_third[1]);
        assertEquals("bar", sa_third[2]);

        // --

        int[] ia = ArrayUtil.insert(new int[0], 0, 1);
        assertFalse(ia == null);
        assertEquals(int.class, ia.getClass().getComponentType());
        assertEquals(1, ia.length);
        assertEquals(1, ia[0]);

        int[] ia_last = ArrayUtil.insert(ia, 1, 2);
        assertFalse(ia_last == ia);
        assertEquals(int.class, ia_last.getClass().getComponentType());
        assertEquals(2, ia_last.length);
        assertEquals(1, ia_last[0]);
        assertEquals(2, ia_last[1]);

        int[] ia_first = ArrayUtil.insert(ia, 0, 2);
        assertFalse(ia_first == ia);
        assertEquals(int.class, ia_first.getClass().getComponentType());
        assertEquals(2, ia_first.length);
        assertEquals(2, ia_first[0]);
        assertEquals(1, ia_first[1]);

        int[] ia_third = ArrayUtil.insert(ia_last, 1, 3);
        assertFalse(ia_third == ia_last);
        assertEquals(int.class, ia_third.getClass().getComponentType());
        assertEquals(3, ia_third.length);
        assertEquals(1, ia_third[0]);
        assertEquals(3, ia_third[1]);
        assertEquals(2, ia_third[2]);

        // --

        double[] da = ArrayUtil.insert(new double[0], 0, 1.0);
        assertFalse(da == null);
        assertEquals(double.class, da.getClass().getComponentType());
        assertEquals(1, da.length);
        assertEquals(1.0, da[0]);

        double[] da_last = ArrayUtil.insert(da, 1, 2.0);
        assertFalse(da_last == da);
        assertEquals(double.class, da_last.getClass().getComponentType());
        assertEquals(2, da_last.length);
        assertEquals(1.0, da_last[0]);
        assertEquals(2.0, da_last[1]);

        double[] da_first = ArrayUtil.insert(da, 0, 2.0);
        assertFalse(da_first == da);
        assertEquals(double.class, da_first.getClass().getComponentType());
        assertEquals(2, da_first.length);
        assertEquals(2.0, da_first[0]);
        assertEquals(1.0, da_first[1]);

        double[] da_third = ArrayUtil.insert(da_last, 1, 3.0);
        assertFalse(da_third == da_last);
        assertEquals(double.class, da_third.getClass().getComponentType());
        assertEquals(3, da_third.length);
        assertEquals(1.0, da_third[0]);
        assertEquals(3.0, da_third[1]);
        assertEquals(2.0, da_third[2]);

        // --

        long[] la = ArrayUtil.insert(new long[0], 0, 1);
        assertFalse(la == null);
        assertEquals(long.class, la.getClass().getComponentType());
        assertEquals(1, la.length);
        assertEquals(1, la[0]);

        long[] la_last = ArrayUtil.insert(la, 1, 2);
        assertFalse(la_last == la);
        assertEquals(long.class, la_last.getClass().getComponentType());
        assertEquals(2, la_last.length);
        assertEquals(1, la_last[0]);
        assertEquals(2, la_last[1]);

        long[] la_first = ArrayUtil.insert(la, 0, 2);
        assertFalse(la_first == la);
        assertEquals(long.class, la_first.getClass().getComponentType());
        assertEquals(2, la_first.length);
        assertEquals(2, la_first[0]);
        assertEquals(1, la_first[1]);

        long[] la_third = ArrayUtil.insert(la_last, 1, 3);
        assertFalse(la_third == la_last);
        assertEquals(long.class, la_third.getClass().getComponentType());
        assertEquals(3, la_third.length);
        assertEquals(1, la_third[0]);
        assertEquals(3, la_third[1]);
        assertEquals(2, la_third[2]);
    }

    public static class StringWrapper {
        public String s;

        public StringWrapper(String s) {
            this.s = s;
        }

        public boolean equals(Object other) {
            return other instanceof StringWrapper && ((StringWrapper) other).s.equals(this.s);
        }

        public static class UnaryEquals implements Predicate.Unary<StringWrapper> {
            String other;

            UnaryEquals(String other) {
                this.other = other;
            }

            public boolean call(StringWrapper arg) {
                return arg.s.equals(other);
            }
        }

        public static class BinaryEquals implements Predicate.Binary<StringWrapper, String> {
            public boolean call(StringWrapper arg, String other) {
                return arg.s.equals(other);
            }
        }

        public static class NullaryFactory implements Function.Nullary<StringWrapper> {
            String s;

            NullaryFactory(String s) {
                this.s = s;
            }

            public StringWrapper call() {
                return new StringWrapper(s);
            }
        }

        public static class UnaryFactory implements Function.Unary<StringWrapper, String> {
            public StringWrapper call(String s) {
                return new StringWrapper(s);
            }
        }
    }

    public void testAddToArrayIdentity() {
        StringWrapper sw1 = new StringWrapper("foo");
        StringWrapper sw2 = new StringWrapper("foo");
        StringWrapper sw3 = new StringWrapper("bar");

        StringWrapper[] sa = ArrayUtil.addToArray(sw1, null, StringWrapper.class);
        assertFalse(sa == null);
        assertEquals(StringWrapper.class, sa.getClass().getComponentType());
        assertEquals(1, sa.length);
        assertEquals(sw1, sa[0]);

        StringWrapper[] sa2 = ArrayUtil.addToArray(sw2, sa, StringWrapper.class);
        assertTrue(sa2 == sa);
        assertTrue(sa2[0] == sw2);

        StringWrapper[] sa3 = ArrayUtil.addToArray(sw3, sa, StringWrapper.class);
        assertFalse(sa3 == sa);
        assertEquals(StringWrapper.class, sa.getClass().getComponentType());
        assertEquals(2, sa3.length);
        assertEquals(sw1, sa3[0]);
        assertEquals(sw2, sa3[0]);
        assertEquals(sw3, sa3[1]);
    }

    public void testRemoveFromArray() {
        String sa[] = new String[] {"foo", "bar", "zot"};

        String sa2[] = ArrayUtil.removeFromArray("qux", sa);
        assertTrue(sa2 == sa);

        sa2 = ArrayUtil.removeFromArray("foo", sa);
        assertEquals(2, sa2.length);
        assertEquals("bar", sa2[0]);
        assertEquals("zot", sa2[1]);

        sa2 = ArrayUtil.removeFromArray("bar", sa);
        assertEquals(2, sa2.length);
        assertEquals("foo", sa2[0]);
        assertEquals("zot", sa2[1]);

        sa2 = ArrayUtil.removeFromArray("zot", sa);
        assertEquals(2, sa2.length);
        assertEquals("foo", sa2[0]);
        assertEquals("bar", sa2[1]);

        sa2 = ArrayUtil.removeFromArray("bar", sa2);
        assertEquals(1, sa2.length);
        assertEquals("foo", sa2[0]);

        sa2 = ArrayUtil.removeFromArray("foo", sa2);
        assertNull(sa2);
    }

    public void testReplaceInArray() {
        StringWrapper foo = new StringWrapper("foo");
        StringWrapper bar = new StringWrapper("bar");
        StringWrapper zot = new StringWrapper("zot");
        StringWrapper foo2 = new StringWrapper("foo");

        StringWrapper[] sa = new StringWrapper[] {foo, bar};

        assertFalse(ArrayUtil.replaceInArray(zot, sa));
        assertTrue(sa[0] == foo);
        assertTrue(sa[1] == bar);

        assertTrue(ArrayUtil.replaceInArray(foo2, sa));
        assertTrue(sa[0] == foo2);
        assertTrue(sa[1] == bar);

        assertTrue(ArrayUtil.replaceInArray(bar, sa));
        assertTrue(sa[0] == foo2);
        assertTrue(sa[1] == bar);
    }

    public void testAddUnlessUnary() {
        StringWrapper[] sa = ArrayUtil.addUnless(null, StringWrapper.class,
            new StringWrapper.UnaryEquals("foo"), new StringWrapper.NullaryFactory("foo"));
        assertFalse(sa == null);
        assertEquals(StringWrapper.class, sa.getClass().getComponentType());
        assertEquals(1, sa.length);
        assertEquals("foo", sa[0].s);
        StringWrapper sw1 = sa[0];

        StringWrapper[] sa2 = ArrayUtil.addUnless(sa, StringWrapper.class,
            new StringWrapper.UnaryEquals("foo"), new StringWrapper.NullaryFactory("foo"));
        assertTrue(sa2 == sa);
        assertTrue(sa2[0] == sw1);

        StringWrapper[] sa3 = ArrayUtil.addUnless(sa, StringWrapper.class,
            new StringWrapper.UnaryEquals("bar"), new StringWrapper.NullaryFactory("bar"));
        assertFalse(sa3 == sa);
        assertEquals(StringWrapper.class, sa.getClass().getComponentType());
        assertEquals(2, sa3.length);
        assertEquals("foo", sa3[0].s);
        assertEquals("bar", sa3[1].s);
    }

    public void testAddUnlessBinary() {
        StringWrapper[] sa = ArrayUtil.addUnless(null, StringWrapper.class,
            new StringWrapper.BinaryEquals(), new StringWrapper.UnaryFactory(), "foo");
        assertFalse(sa == null);
        assertEquals(StringWrapper.class, sa.getClass().getComponentType());
        assertEquals(1, sa.length);
        assertEquals("foo", sa[0].s);
        StringWrapper sw1 = sa[0];

        StringWrapper[] sa2 = ArrayUtil.addUnless(sa, StringWrapper.class,
            new StringWrapper.BinaryEquals(), new StringWrapper.UnaryFactory(), "foo");
        assertTrue(sa2 == sa);
        assertTrue(sa2[0] == sw1);

        StringWrapper[] sa3 = ArrayUtil.addUnless(sa, StringWrapper.class,
            new StringWrapper.BinaryEquals(), new StringWrapper.UnaryFactory(), "bar");
        assertFalse(sa3 == sa);
        assertEquals(StringWrapper.class, sa.getClass().getComponentType());
        assertEquals(2, sa3.length);
        assertEquals("foo", sa3[0].s);
        assertEquals("bar", sa3[1].s);
    }

    public void testReplaceOrAdd() {
        StringWrapper[] sa = ArrayUtil.replaceOrAdd(null, StringWrapper.class,
            new StringWrapper.BinaryEquals(), new StringWrapper.UnaryFactory(), "foo");
        assertFalse(sa == null);
        assertEquals(StringWrapper.class, sa.getClass().getComponentType());
        assertEquals(1, sa.length);
        assertEquals("foo", sa[0].s);
        StringWrapper sw1 = sa[0];

        StringWrapper[] sa2 = ArrayUtil.replaceOrAdd(sa, StringWrapper.class,
            new StringWrapper.BinaryEquals(), new StringWrapper.UnaryFactory(), "foo");
        assertTrue(sa2 == sa);
        assertEquals("foo", sa2[0].s);
        assertTrue(sa2[0] != sw1);

        StringWrapper[] sa3 = ArrayUtil.addUnless(sa, StringWrapper.class,
            new StringWrapper.BinaryEquals(), new StringWrapper.UnaryFactory(), "bar");
        assertFalse(sa3 == sa);
        assertEquals(StringWrapper.class, sa.getClass().getComponentType());
        assertEquals(2, sa3.length);
        assertEquals("foo", sa3[0].s);
        assertEquals("bar", sa3[1].s);
    }

    public void testRemoveIfUnary() {
        StringWrapper foo = new StringWrapper("foo");
        StringWrapper bar = new StringWrapper("bar");
        StringWrapper zot = new StringWrapper("zot");
        StringWrapper[] sa = new StringWrapper[] {foo, bar, zot};

        StringWrapper[] sa2 = ArrayUtil.removeIf(sa, new StringWrapper.UnaryEquals("qux"));
        assertTrue(sa2 == sa);

        sa2 = ArrayUtil.removeIf(sa, new StringWrapper.UnaryEquals("foo"));
        assertEquals(2, sa2.length);
        assertEquals("bar", sa2[0].s);
        assertEquals("zot", sa2[1].s);

        sa2 = ArrayUtil.removeIf(sa, new StringWrapper.UnaryEquals("bar"));
        assertEquals(2, sa2.length);
        assertEquals("foo", sa2[0].s);
        assertEquals("zot", sa2[1].s);

        sa2 = ArrayUtil.removeIf(sa, new StringWrapper.UnaryEquals("zot"));
        assertEquals(2, sa2.length);
        assertEquals("foo", sa2[0].s);
        assertEquals("bar", sa2[1].s);

        sa2 = ArrayUtil.removeIf(sa2, new StringWrapper.UnaryEquals("bar"));
        assertEquals(1, sa2.length);
        assertEquals("foo", sa2[0].s);

        sa2 = ArrayUtil.removeIf(sa2, new StringWrapper.UnaryEquals("foo"));
        assertNull(sa2);
    }

    public void testRemoveIfBinary() {
        StringWrapper foo = new StringWrapper("foo");
        StringWrapper bar = new StringWrapper("bar");
        StringWrapper zot = new StringWrapper("zot");
        StringWrapper[] sa = new StringWrapper[] {foo, bar, zot};

        StringWrapper[] sa2 = ArrayUtil.removeIf(sa, new StringWrapper.BinaryEquals(), "qux");
        assertTrue(sa2 == sa);

        sa2 = ArrayUtil.removeIf(sa, new StringWrapper.BinaryEquals(), "foo");
        assertEquals(2, sa2.length);
        assertEquals("bar", sa2[0].s);
        assertEquals("zot", sa2[1].s);

        sa2 = ArrayUtil.removeIf(sa, new StringWrapper.BinaryEquals(), "bar");
        assertEquals(2, sa2.length);
        assertEquals("foo", sa2[0].s);
        assertEquals("zot", sa2[1].s);

        sa2 = ArrayUtil.removeIf(sa, new StringWrapper.BinaryEquals(), "zot");
        assertEquals(2, sa2.length);
        assertEquals("foo", sa2[0].s);
        assertEquals("bar", sa2[1].s);

        sa2 = ArrayUtil.removeIf(sa2, new StringWrapper.BinaryEquals(), "bar");
        assertEquals(1, sa2.length);
        assertEquals("foo", sa2[0].s);

        sa2 = ArrayUtil.removeIf(sa2, new StringWrapper.BinaryEquals(), "foo");
        assertNull(sa2);
    }

    private static void nullOrEqual(final String sa, final String sb) {
        if (sa == null) {
            assertTrue(sb == null);
        } else {
            assertTrue(sa.equals(sb));
        }
    }

    private static void checkRange(final String[] a, final int aFromIndex,
        final String[] b, final int bFromIndex,
        final int count) {
        assertTrue(aFromIndex + count <= a.length);
        assertTrue(bFromIndex + count <= b.length);
        for (int i = 0; i < count; ++i) {
            nullOrEqual(a[aFromIndex + i], b[bFromIndex + i]);
        }
    }

    private static void checkMerge(final String[] ss1, final String[] ss2) {
        final String[] ans = ArrayUtil.merge(ss1, ss2, String.class);
        assertTrue(ans.length == ss1.length + ss2.length);
        checkRange(ans, 0, ss1, 0, ss1.length);
        checkRange(ans, ss1.length, ss2, 0, ss2.length);
    }

    public void testMerge() {
        final String[] ss1 = {"a", "b", "c"};
        final String[] ss2 = {"d", "c", "e", "f", "g"};
        final String[] empty = {};
        final String[] one = {"just1"};
        checkMerge(ss1, ss2);
        checkMerge(ss1, empty);
        checkMerge(empty, ss2);
        checkMerge(one, ss2);
        checkMerge(ss1, one);
    }

    public void testContainsObject() {
        String[] strings = null;
        assertFalse(ArrayUtil.contains(strings, "a"));
        assertFalse(ArrayUtil.contains(strings, null));

        strings = new String[] {};
        assertFalse(ArrayUtil.contains(strings, "a"));
        assertFalse(ArrayUtil.contains(strings, null));

        strings = new String[] {"a"};
        assertTrue(ArrayUtil.contains(strings, "a"));
        assertFalse(ArrayUtil.contains(strings, "b"));
        assertFalse(ArrayUtil.contains(strings, null));

        strings = new String[] {"a", null};
        assertTrue(ArrayUtil.contains(strings, "a"));
        assertFalse(ArrayUtil.contains(strings, "b"));
        assertTrue(ArrayUtil.contains(strings, null));

        strings = new String[] {"a", null, "b"};
        assertTrue(ArrayUtil.contains(strings, "a"));
        assertTrue(ArrayUtil.contains(strings, "b"));
        assertTrue(ArrayUtil.contains(strings, null));
    }

    public void checkEquals(final String[] s1, final String[] s2) {
        assertEquals(s1.length, s2.length);
        for (int i = 0; i < s1.length; ++i) {
            final String ss1 = s1[i];
            final String ss2 = s2[i];
            assertEquals(ss1, ss2);
        }
    }

    public void testContainsInt() {
        int[] ints = null;
        assertFalse(ArrayUtil.contains(ints, 5));

        ints = new int[] {};
        assertFalse(ArrayUtil.contains(ints, 5));

        ints = new int[] {0};
        assertFalse(ArrayUtil.contains(ints, 5));

        ints = new int[] {0, 5};
        assertTrue(ArrayUtil.contains(ints, 5));
    }

    public void testMergeMulti() {
        String[] s1 = {"a", "b", "c"};
        String[] s2 = {"d", "e", "f", "g"};
        String[] s3 = {"h", "i"};
        String[] m1 = ArrayUtil.merge(ArrayUtil.merge(s1, s2, String.class), s3, String.class);
        String[] m2 = ArrayUtil.merge(String.class, s1, s2, s3);
        checkEquals(m1, m2);
    }

    public void testIsSorted() {
        // ints
        assertTrue(ArrayUtil.isSorted(new int[] {1, 2, 3, 5}));
        assertTrue(ArrayUtil.isSorted(new int[] {1, 2}));
        assertTrue(ArrayUtil.isSorted(new int[] {1}));
        assertTrue(ArrayUtil.isSorted(new int[0]));
        assertTrue(ArrayUtil.isSorted((int[]) null));

        assertFalse(ArrayUtil.isSorted(new int[] {1, 5, 3, 4}));
        assertFalse(ArrayUtil.isSorted(new int[] {5, 2, 3, 4}));
        assertFalse(ArrayUtil.isSorted(new int[] {1, 2, 3, 0}));

        // longs
        assertTrue(ArrayUtil.isSorted(new long[] {1, 2, 3, 5}));
        assertTrue(ArrayUtil.isSorted(new long[] {1, 2}));
        assertTrue(ArrayUtil.isSorted(new long[] {1}));
        assertTrue(ArrayUtil.isSorted(new long[0]));
        assertTrue(ArrayUtil.isSorted((long[]) null));

        assertFalse(ArrayUtil.isSorted(new long[] {1, 5, 3, 4}));
        assertFalse(ArrayUtil.isSorted(new long[] {5, 2, 3, 4}));
        assertFalse(ArrayUtil.isSorted(new long[] {1, 2, 3, 0}));

        // doubles
        assertTrue(ArrayUtil.isSorted(new double[] {1, 2, 3, 5}));
        assertTrue(ArrayUtil.isSorted(new double[] {1, 2}));
        assertTrue(ArrayUtil.isSorted(new double[] {1}));
        assertTrue(ArrayUtil.isSorted(new double[0]));
        assertTrue(ArrayUtil.isSorted((double[]) null));

        assertFalse(ArrayUtil.isSorted(new double[] {1, 5, 3, 4}));
        assertFalse(ArrayUtil.isSorted(new double[] {5, 2, 3, 4}));
        assertFalse(ArrayUtil.isSorted(new double[] {1, 2, 3, 0}));

        // Objects
        assertTrue(ArrayUtil.isSorted(new String[] {"A", "B", "C", "D"}));
        assertTrue(ArrayUtil.isSorted(new String[] {"A", "B"}));
        assertTrue(ArrayUtil.isSorted(new String[] {"A"}));
        assertTrue(ArrayUtil.isSorted(new String[] {null, "A", "B"}));
        assertTrue(ArrayUtil.isSorted(new String[0]));
        assertTrue(ArrayUtil.isSorted((String[]) null));

        assertFalse(ArrayUtil.isSorted(new String[] {"A", "Z", "C", "D"}));
        assertFalse(ArrayUtil.isSorted(new String[] {"Z", "B", "C", "D"}));
        assertFalse(ArrayUtil.isSorted(new String[] {"B", "C", "D", "A"}));
        assertFalse(ArrayUtil.isSorted(new String[] {"A", "C", "D", null}));
        assertFalse(ArrayUtil.isSorted(new String[] {"B", null, "D", "A"}));
    }
}
