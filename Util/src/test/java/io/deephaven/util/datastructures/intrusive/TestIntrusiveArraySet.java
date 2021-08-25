package io.deephaven.util.datastructures.intrusive;

import junit.framework.TestCase;

import java.util.*;
import java.util.stream.Collectors;

public class TestIntrusiveArraySet extends TestCase {
    private static class IntrusiveValue {
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

        final List<IntrusiveValue> copy = new ArrayList<>();
        for (IntrusiveValue value : set) {
            copy.add(value);
        }

        assertEquals(values, copy);

        final IntrusiveValue nineteen = new IntrusiveValue(19);
        values.add(nineteen);
        set.add(nineteen);
        assertEquals(values.size(), set.size());

        for (final Iterator<IntrusiveValue> it = set.iterator(); it.hasNext();) {
            final IntrusiveValue next = it.next();
            if (next.sentinel == 1 || next.sentinel == 7 || next.sentinel == 19) {
                it.remove();
            }
        }

        assertFalse(set.contains(values.get(0)));
        assertFalse(set.contains(values.get(4)));
        assertFalse(set.contains(nineteen));
        assertFalse(set.contains(twentyThree));

        values.remove(0);
        values.remove(3);
        values.remove(nineteen);

        copy.clear();
        copy.addAll(set);

        copy.sort(Comparator.comparing(v -> v.sentinel));
        assertEquals(values, copy);

        set.retainAll(values);

        copy.clear();
        copy.addAll(set);

        copy.sort(Comparator.comparing(v -> v.sentinel));
        assertEquals(values, copy);

        final List<IntrusiveValue> valueCopy = new ArrayList<>(values);
        valueCopy.removeIf(x -> x.sentinel == 2 || x.sentinel < 13);
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
}
