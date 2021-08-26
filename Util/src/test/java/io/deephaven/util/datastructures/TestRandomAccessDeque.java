package io.deephaven.util.datastructures;

import io.deephaven.base.testing.BaseArrayTestCase;
import junit.framework.TestCase;

import java.util.*;
import java.util.stream.Collectors;

public class TestRandomAccessDeque extends BaseArrayTestCase {
    public void testSimple() {
        List<Integer> values = new ArrayList<>(Arrays.asList(2, 3, 4, 5));

        RandomAccessDeque<Integer> deque = new RandomAccessDeque<>(values);

        checkEquals(values, deque);

        values.add(0, 1);
        deque.addFirst(1);

        checkEquals(values, deque);

        values.add(6);
        deque.addLast(6);

        checkEquals(values, deque);

        Iterator<Integer> vit = values.iterator();
        Iterator<Integer> dit = values.iterator();
        while (vit.hasNext()) {
            TestCase.assertEquals(vit.hasNext(), dit.hasNext());
            Integer vv = vit.next();
            Integer dd = dit.next();
            TestCase.assertEquals(vv, dd);
            TestCase.assertEquals(vit.hasNext(), dit.hasNext());
        }

        show(values, deque);

        int n = 0;
        vit = values.iterator();
        dit = deque.iterator();
        while (vit.hasNext()) {
            TestCase.assertEquals(vit.hasNext(), dit.hasNext());
            Integer vv = vit.next();
            Integer dd = dit.next();
            TestCase.assertEquals(vv, dd);
            TestCase.assertEquals(vit.hasNext(), dit.hasNext());
            if (++n % 2 == 0) {
                vit.remove();
                dit.remove();
            }
        }
        checkEquals(values, deque);

        show(values, deque);

        values.removeIf(x -> x % 3 == 0);
        assertTrue(deque.removeIf(x -> x == 3));

        checkEquals(values, deque);

        assertFalse(deque.removeIf(x -> x == 999));

        TestCase.assertTrue(Arrays.equals(values.toArray(), deque.toArray()));
        TestCase.assertTrue(Arrays.equals(values.toArray(new Integer[0]), deque.toArray(new Integer[0])));
        TestCase.assertTrue(
                Arrays.equals(values.toArray(new Integer[values.size()]), deque.toArray(new Integer[deque.size()])));

        values.addAll(Arrays.asList(7, 8, 9));
        deque.addAll(Arrays.asList(7, 8, 9));
        checkEquals(values, deque);

        values.removeAll(Arrays.asList(3, 7));
        deque.removeAll(Arrays.asList(3, 7));
        checkEquals(values, deque);

        values.retainAll(Arrays.asList(1, 8));
        deque.retainAll(Arrays.asList(1, 8));
        checkEquals(values, deque);

        values.remove(new Integer(1));
        deque.remove(1);
        show(values, deque);
        checkEquals(values, deque);

        deque.clear();
        TestCase.assertEquals(true, deque.isEmpty());
        values.clear();
        checkEquals(values, deque);

        values.addAll(Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18));
        deque.addAll(Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18));
        checkEquals(values, deque);

        for (int ii = 0; ii < 1000; ++ii) {
            values.add(ii);
            deque.add(ii);
        }
        checkEquals(values, deque);

        // noinspection SimplifyStreamApiCallChains
        List<Integer> streamResult = deque.stream().collect(Collectors.toList());
        TestCase.assertEquals(values, streamResult);

        Set<Integer> psResult = new HashSet<>(deque.parallelStream().collect(Collectors.toSet()));
        Set<Integer> valuesSet = new HashSet<>(values);

        Set<Integer> missing = new HashSet<>(valuesSet);
        missing.removeAll(psResult);
        System.out.println("Missing from psResult: " + missing);

        Set<Integer> missing2 = new HashSet<>(psResult);
        missing2.removeAll(valuesSet);
        System.out.println("Missing from values: " + missing2);

        TestCase.assertEquals(valuesSet, psResult);
    }

    private void show(List<Integer> values, RandomAccessDeque<Integer> deque) {
        StringBuilder builder = new StringBuilder();
        builder.append(values.size()).append(": ");
        for (int ii = 0; ii < values.size(); ++ii) {
            if (ii > 0) {
                builder.append(", ");
            }
            Integer vv = values.get(ii);
            boolean different = ii >= deque.size() || !Objects.equals(deque.get(ii), vv);
            if (different) {
                builder.append("*");
            }
            builder.append(vv);
            if (different) {
                builder.append("*");
            }
        }
        builder.append("\n");

        builder.append(deque.size()).append(": ");
        for (int ii = 0; ii < deque.size(); ++ii) {
            if (ii > 0) {
                builder.append(", ");
            }
            Integer dd = deque.get(ii);
            boolean different = ii >= values.size() || !Objects.equals(values.get(ii), dd);
            if (different) {
                builder.append("*");
            }
            builder.append(dd);
            if (different) {
                builder.append("*");
            }
        }
        builder.append("\n");

        System.out.println(builder);
    }

    private void checkEquals(List<Integer> values, RandomAccessDeque<Integer> deque) {
        TestCase.assertEquals(values.size(), deque.size());
        for (int ii = 0; ii < deque.size(); ++ii) {
            TestCase.assertEquals(values.get(ii), deque.get(ii));
        }
    }
}
