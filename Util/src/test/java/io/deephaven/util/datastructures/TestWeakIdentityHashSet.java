//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures;

import io.deephaven.util.mutable.MutableInt;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link WeakIdentityHashSet}.
 */
public class TestWeakIdentityHashSet {

    @Test
    public void testAdd() {
        String[] values = IntStream.range(0, 1000).mapToObj(Integer::toString).toArray(String[]::new);
        final WeakIdentityHashSet<String> set = new WeakIdentityHashSet<>();
        Arrays.stream(values).forEach(v -> assertTrue(set.add(v)));
        Arrays.stream(values).forEach(v -> assertFalse(set.add(v)));
        values = null;
        System.gc();
        values = IntStream.range(0, 1000).mapToObj(Integer::toString).toArray(String[]::new);
        Arrays.stream(values).forEach(v -> assertTrue(set.add(v)));
        Arrays.stream(values).forEach(v -> assertFalse(set.add(v)));
    }

    @Test
    public void testClear() {
        final String[] values = IntStream.range(1000, 2000).mapToObj(Integer::toString).toArray(String[]::new);
        final WeakIdentityHashSet<String> set = new WeakIdentityHashSet<>();
        Arrays.stream(values).forEach(v -> assertTrue(set.add(v)));
        Arrays.stream(values).forEach(v -> assertFalse(set.add(v)));
        set.clear();
        Arrays.stream(values).forEach(v -> assertTrue(set.add(v)));
        Arrays.stream(values).forEach(v -> assertFalse(set.add(v)));
    }

    @Test
    public void testForEach() {
        final String[] values = IntStream.range(1000, 2000).mapToObj(Integer::toString).toArray(String[]::new);

        final WeakIdentityHashSet<String> set = new WeakIdentityHashSet<>();
        final MutableInt counter = new MutableInt(0);

        set.forEach(s -> {
            assertNotNull(s);
            counter.increment();
        });
        assertEquals(0, counter.get());

        Arrays.stream(values).forEach(v -> assertTrue(set.add(v)));
        Arrays.stream(values).forEach(v -> assertFalse(set.add(v)));

        counter.set(0);
        set.forEach(s -> {
            assertNotNull(s);
            counter.increment();
        });
        assertEquals(values.length, counter.get());

        set.clear();

        counter.set(0);
        set.forEach(s -> {
            assertNotNull(s);
            counter.increment();
        });
        assertEquals(0, counter.get());

        Arrays.stream(values).forEach(v -> assertTrue(set.add(v)));
        Arrays.stream(values).forEach(v -> assertFalse(set.add(v)));

        counter.set(0);
        set.forEach(s -> {
            assertNotNull(s);
            counter.increment();
        });
        assertEquals(values.length, counter.get());
    }
}
