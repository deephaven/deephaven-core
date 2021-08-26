package io.deephaven.base;

import junit.framework.TestCase;
import org.junit.Test;

public class TestPair {
    @Test
    public void testCompare() {
        Pair<String, String> p1 = new Pair<>("A", "B");
        Pair<String, String> p2 = new Pair<>("A", "C");
        Pair<String, String> p3 = new Pair<>("B", "C");
        Pair<String, String> p4 = new Pair<>("B", "D");
        Pair<String, String> p5 = new Pair<>("B", "D");
        Pair<String, String> p6 = new Pair<>(null, "B");
        Pair<String, String> p7 = new Pair<>(null, "A");
        Pair<String, String> p8 = new Pair<>(null, "A");
        Pair<String, String> p9 = new Pair<>("A", null);
        Pair<String, String> p10 = new Pair<>("A", null);
        Pair<String, String> p11 = new Pair<>(null, null);
        Pair<String, String> p12 = new Pair<>(null, null);

        TestCase.assertFalse(p4.equals(p3));
        TestCase.assertEquals(p4, p4);
        TestCase.assertEquals(p4, p5);
        TestCase.assertEquals(p11, p11);
        TestCase.assertEquals(p11, p12);

        TestCase.assertEquals(-1, p1.compareTo(p2));
        TestCase.assertEquals(1, p2.compareTo(p1));
        TestCase.assertEquals(-1, p1.compareTo(p3));
        TestCase.assertEquals(1, p3.compareTo(p1));
        TestCase.assertEquals(1, p4.compareTo(p3));
        TestCase.assertEquals(-1, p3.compareTo(p4));

        // noinspection EqualsWithItself
        TestCase.assertEquals(0, p7.compareTo(p7));
        // noinspection EqualsWithItself
        TestCase.assertEquals(0, p8.compareTo(p8));
        TestCase.assertEquals(0, p7.compareTo(p8));
        TestCase.assertEquals(0, p8.compareTo(p7));

        // noinspection EqualsWithItself
        TestCase.assertEquals(0, p9.compareTo(p9));
        // noinspection EqualsWithItself
        TestCase.assertEquals(0, p9.compareTo(p9));
        TestCase.assertEquals(0, p9.compareTo(p10));
        TestCase.assertEquals(0, p9.compareTo(p10));

        TestCase.assertEquals(1, p1.compareTo(p9));
        TestCase.assertEquals(-1, p9.compareTo(p1));
        TestCase.assertEquals(1, p1.compareTo(p9));
        TestCase.assertEquals(-1, p9.compareTo(p1));

        TestCase.assertEquals(-1, p7.compareTo(p6));
        TestCase.assertEquals(1, p6.compareTo(p7));
    }
}
