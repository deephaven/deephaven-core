package io.deephaven.util.datastructures;

import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;

import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Unit tests for {@link WeakIdentityHashSet}.
 */
public class TestWeakIdentityHashSet {

    @Test
    public void testAdd() {
        String[] values = IntStream.range(0, 1000).mapToObj(Integer::toString).toArray(String[]::new);
        final WeakIdentityHashSet<String> set = new WeakIdentityHashSet<>();
        Stream.of(values).forEach(v -> TestCase.assertTrue(set.add(v)));
        Stream.of(values).forEach(v -> TestCase.assertFalse(set.add(v)));
        values = null;
        System.gc();
        values = IntStream.range(0, 1000).mapToObj(Integer::toString).toArray(String[]::new);
        Stream.of(values).forEach(v -> TestCase.assertTrue(set.add(v)));
        Stream.of(values).forEach(v -> TestCase.assertFalse(set.add(v)));
    }

    @Test
    public void testClear() {
        final String[] values = IntStream.range(1000, 2000).mapToObj(Integer::toString).toArray(String[]::new);
        final WeakIdentityHashSet<String> set = new WeakIdentityHashSet<>();
        Stream.of(values).forEach(v -> TestCase.assertTrue(set.add(v)));
        Stream.of(values).forEach(v -> TestCase.assertFalse(set.add(v)));
        set.clear();
        Stream.of(values).forEach(v -> TestCase.assertTrue(set.add(v)));
        Stream.of(values).forEach(v -> TestCase.assertFalse(set.add(v)));
    }

    @Test
    public void testForEach() {
        final String[] values = IntStream.range(1000, 2000).mapToObj(Integer::toString).toArray(String[]::new);

        final WeakIdentityHashSet<String> set = new WeakIdentityHashSet<>();
        final MutableInt counter = new MutableInt(0);

        set.forEach(s -> {
            TestCase.assertNotNull(s);
            counter.increment();
        });
        TestCase.assertEquals(0, counter.intValue());

        Stream.of(values).forEach(v -> TestCase.assertTrue(set.add(v)));
        Stream.of(values).forEach(v -> TestCase.assertFalse(set.add(v)));

        counter.setValue(0);
        set.forEach(s -> {
            TestCase.assertNotNull(s);
            counter.increment();
        });
        TestCase.assertEquals(values.length, counter.intValue());

        set.clear();

        counter.setValue(0);
        set.forEach(s -> {
            TestCase.assertNotNull(s);
            counter.increment();
        });
        TestCase.assertEquals(0, counter.intValue());

        Stream.of(values).forEach(v -> TestCase.assertTrue(set.add(v)));
        Stream.of(values).forEach(v -> TestCase.assertFalse(set.add(v)));

        counter.setValue(0);
        set.forEach(s -> {
            TestCase.assertNotNull(s);
            counter.increment();
        });
        TestCase.assertEquals(values.length, counter.intValue());
    }
}
