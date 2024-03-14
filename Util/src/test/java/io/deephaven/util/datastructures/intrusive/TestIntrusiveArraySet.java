//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.intrusive;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class TestIntrusiveArraySet {
    private static class IntrusiveValue implements Comparable<IntrusiveValue> {
        private IntrusiveValue(int sentinel) {
            this.sentinel = sentinel;
        }

        private static List<IntrusiveValue> make(int... sentinel) {
            return Arrays.stream(sentinel).mapToObj(IntrusiveValue::new).collect(Collectors.toList());
        }

        int sentinel;
        int slot;

        @Override
        public String toString() {
            return "IntrusiveValue{" +
                    "sentinel=" + sentinel +
                    ", slot=" + slot +
                    '}';
        }

        @Override
        public int compareTo(@NotNull final IntrusiveValue other) {
            return Integer.compare(sentinel, other.sentinel);
        }
    }

    private static class Adapter implements IntrusiveArraySet.Adapter<IntrusiveValue> {

        @Override
        public int getSlot(IntrusiveValue element) {
            return element.slot;
        }

        @Override
        public void setSlot(IntrusiveValue element, int slot) {
            element.slot = slot;
        }
    }

    @Test
    public void testSimple() {
        final IntrusiveArraySet<IntrusiveValue> set = new IntrusiveArraySet<>(new Adapter(), IntrusiveValue.class);
        final IntrusiveValue twentyThree = new IntrusiveValue(23);

        assertFalse(set.contains(twentyThree));
        assertTrue(set.isEmpty());

        final List<IntrusiveValue> values = IntrusiveValue.make(1, 2, 3, 5, 7, 11, 13, 17);

        set.addAll(values);
        assertFalse(set.isEmpty());
        assertEquals(values.size(), set.size());

        set.add(values.get(0));
        assertEquals(values.size(), set.size());

        assertFalse(set.contains(twentyThree));
        assertTrue(set.contains(values.get(1)));
        assertTrue(set.containsAll(values));

        final List<IntrusiveValue> copy = new ArrayList<>(set);

        assertEquals(values, copy);

        final IntrusiveValue nineteen = new IntrusiveValue(19);
        values.add(nineteen);
        set.add(nineteen);
        assertEquals(values.size(), set.size());

        set.removeIf(next -> next.sentinel == 1 || next.sentinel == 7 || next.sentinel == 19);

        assertFalse(set.contains(values.get(0)));
        assertFalse(set.contains(values.get(4)));
        assertFalse(set.contains(nineteen));
        assertFalse(set.contains(twentyThree));

        values.remove(0);
        values.remove(3);
        values.remove(nineteen);

        copy.clear();
        copy.addAll(set);

        copy.sort(Comparator.naturalOrder());
        assertEquals(values, copy);

        set.retainAll(values);

        copy.clear();
        copy.addAll(set);

        copy.sort(Comparator.naturalOrder());
        assertEquals(values, copy);

        final List<IntrusiveValue> valueCopy = new ArrayList<>(values);
        valueCopy.removeIf(x -> x.sentinel < 13);
        valueCopy.add(valueCopy.get(0));
        valueCopy.add(valueCopy.get(0));

        set.retainAll(valueCopy);

        copy.clear();
        copy.addAll(set);

        copy.sort(Comparator.comparing(v -> v.sentinel));

        valueCopy.remove(valueCopy.size() - 1);
        valueCopy.remove(valueCopy.size() - 1);

        assertEquals(valueCopy, copy);
    }

    @Test
    public void testSort() {
        final IntrusiveArraySet<IntrusiveValue> set = new IntrusiveArraySet<>(new Adapter(), IntrusiveValue.class);
        set.sort(Comparator.naturalOrder());

        final List<IntrusiveValue> values = IntrusiveValue.make(78, 1, 22, 5, 4, 3, 66, 67, 0, 100);
        set.ensureCapacity(values.size());
        set.addAll(values);

        assertArrayEquals(set.toArray(), values.toArray());

        set.sort(Comparator.naturalOrder());
        values.sort(Comparator.naturalOrder());

        assertArrayEquals(set.toArray(), values.toArray());

        set.remove(values.remove(2));
        set.remove(values.remove(5));

        set.sort(Comparator.naturalOrder());
        values.sort(Comparator.naturalOrder());

        assertArrayEquals(set.toArray(), values.toArray());

        values.addAll(IntrusiveValue.make(101, 102, 100, 99, 65, 87, 2, 2, 4));
        set.ensureCapacity(100);
        set.addAll(values);

        assertArrayEquals(set.toArray(), values.toArray());

        set.sort(Comparator.naturalOrder());
        values.sort(Comparator.naturalOrder());

        assertArrayEquals(set.toArray(), values.toArray());
    }
}
