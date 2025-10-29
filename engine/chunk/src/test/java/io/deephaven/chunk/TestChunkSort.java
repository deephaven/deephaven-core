//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import junit.framework.TestCase;
import org.junit.Test;

import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.*;

import static io.deephaven.util.QueryConstants.*;

/**
 * Verify expectations about chunk sorting for NULL / NaN and regular values. Performs exhaustive testing of all
 * permutations of an expected data set for each primitive type.
 */
public class TestChunkSort {

    // region PermutationIterator
    public static class PermutationIterator<E extends Comparable<E>> implements Iterator<List<E>> {

        private final List<E> currentPermutation;
        private boolean firstPermutation = true;
        private boolean hasNextPermutation;
        private final Comparator<E> comparator = Comparator.nullsFirst(Comparator.naturalOrder());

        public PermutationIterator(List<E> initialList) {
            // Sort the initial list to start with the lexicographically smallest permutation
            this.currentPermutation = new java.util.ArrayList<>(initialList);
            Collections.sort(this.currentPermutation, comparator);
            this.hasNextPermutation = true; // Assume there's at least one permutation
        }

        @Override
        public boolean hasNext() {
            return hasNextPermutation;
        }

        @Override
        public List<E> next() {
            if (!hasNextPermutation) {
                throw new NoSuchElementException();
            }

            if (firstPermutation) {
                firstPermutation = false;
                return currentPermutation; // Return a copy of the first permutation
            }

            // Find the next permutation
            int k = -1;
            for (int i = currentPermutation.size() - 2; i >= 0; i--) {
                if (Objects.compare(currentPermutation.get(i), currentPermutation.get(i + 1), comparator) < 0) {
                    k = i;
                    break;
                }
            }

            if (k == -1) { // Current permutation is the last one
                hasNextPermutation = false;
            } else {
                int l = -1;
                for (int i = currentPermutation.size() - 1; i > k; i--) {
                    if (Objects.compare(currentPermutation.get(k), currentPermutation.get(i), comparator) < 0) {
                        l = i;
                        break;
                    }
                }

                Collections.swap(currentPermutation, k, l);
                Collections.reverse(currentPermutation.subList(k + 1, currentPermutation.size()));
            }

            return currentPermutation;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove operation is not supported.");
        }

        private static long permutationCount(int n) {
            if (n < 0) {
                throw new IllegalArgumentException("Factorial is not defined for negative numbers.");
            }
            if (n > 20) {
                System.out.println("Warning: Factorial of " + n
                        + " will exceed the capacity of a long and may result in overflow.");
            }

            long result = 1;
            for (int i = 1; i <= n; i++) {
                result *= i;
            }
            return result;
        }
    }
    // endregion PermutationIterator

    @Test
    public void testCharChunkSort() {
        final List<Character> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(NULL_CHAR); // NOTE: NULL_CHAR == Character.MAX_VALUE
        expected.add(Character.MIN_VALUE);
        expected.add((char) 0);
        expected.add((char) 1);
        expected.add((char) 2);
        expected.add((char) 3);
        expected.add((char) (Character.MAX_VALUE - 1));

        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");
        final PermutationIterator<Character> iterator = new PermutationIterator<>(expected);

        while (iterator.hasNext()) {
            final List<Character> permutation = iterator.next();
            try (final WritableCharChunk<Any> chunk = WritableCharChunk.makeWritableChunk(permutation.size())) {
                chunk.setSize(0);
                permutation.forEach(chunk::add);
                chunk.sort();
                TestCase.assertEquals(expected.size(), chunk.size());
                for (int i = 0; i < expected.size(); i++) {
                    TestCase.assertEquals(
                            "Expected " + expected.get(i) + " but got " + chunk.get(i) + " at index " + i,
                            (char) expected.get(i), chunk.get(i));
                }
            }
        }
    }

    @Test
    public void testByteChunkSort() {
        final List<Byte> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(NULL_BYTE); // NOTE: NULL_BYTE == Byte.MIN_VALUE
        expected.add((byte) (Byte.MIN_VALUE + 1));
        expected.add((byte) -2);
        expected.add((byte) 0);
        expected.add((byte) 1);
        expected.add((byte) (Byte.MAX_VALUE - 1));
        expected.add(Byte.MAX_VALUE);

        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");
        final PermutationIterator<Byte> iterator = new PermutationIterator<>(expected);

        while (iterator.hasNext()) {
            final List<Byte> permutation = iterator.next();
            try (final WritableByteChunk<Any> chunk = WritableByteChunk.makeWritableChunk(permutation.size())) {
                chunk.setSize(0);
                permutation.forEach(chunk::add);
                chunk.sort();
                TestCase.assertEquals(expected.size(), chunk.size());
                for (int i = 0; i < expected.size(); i++) {
                    TestCase.assertEquals(
                            "Expected " + expected.get(i) + " but got " + chunk.get(i) + " at index " + i,
                            (byte) expected.get(i), chunk.get(i));
                }
            }
        }
    }

    @Test
    public void testShortChunkSort() {
        final List<Short> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(NULL_SHORT); // NOTE: NULL_SHORT == Short.MIN_VALUE
        expected.add((short) (Short.MIN_VALUE + 1));
        expected.add((short) -2);
        expected.add((short) 0);
        expected.add((short) 1);
        expected.add((short) (Short.MAX_VALUE - 1));
        expected.add(Short.MAX_VALUE);

        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");
        final PermutationIterator<Short> iterator = new PermutationIterator<>(expected);

        while (iterator.hasNext()) {
            final List<Short> permutation = iterator.next();
            try (final WritableShortChunk<Any> chunk = WritableShortChunk.makeWritableChunk(permutation.size())) {
                chunk.setSize(0);
                permutation.forEach(chunk::add);
                chunk.sort();
                TestCase.assertEquals(expected.size(), chunk.size());
                for (int i = 0; i < expected.size(); i++) {
                    TestCase.assertEquals(
                            "Expected " + expected.get(i) + " but got " + chunk.get(i) + " at index " + i,
                            (short) expected.get(i), chunk.get(i));
                }
            }
        }
    }

    @Test
    public void testIntChunkSort() {
        final List<Integer> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(NULL_INT); // NOTE: NULL_INT == Integer.MIN_VALUE
        expected.add(Integer.MIN_VALUE + 1);
        expected.add(-2);
        expected.add(0);
        expected.add(1);
        expected.add(Integer.MAX_VALUE - 1);
        expected.add(Integer.MAX_VALUE);

        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");
        final PermutationIterator<Integer> iterator = new PermutationIterator<>(expected);

        while (iterator.hasNext()) {
            final List<Integer> permutation = iterator.next();
            try (final WritableIntChunk<Any> chunk = WritableIntChunk.makeWritableChunk(permutation.size())) {
                chunk.setSize(0);
                permutation.forEach(chunk::add);
                chunk.sort();
                TestCase.assertEquals(expected.size(), chunk.size());
                for (int i = 0; i < expected.size(); i++) {
                    TestCase.assertEquals(
                            "Expected " + expected.get(i) + " but got " + chunk.get(i) + " at index " + i,
                            (int) expected.get(i), chunk.get(i));
                }
            }
        }
    }

    @Test
    public void testLongChunkSort() {
        final List<Long> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(NULL_LONG); // NOTE: NULL_LONG == Long.MIN_VALUE
        expected.add(Long.MIN_VALUE + 1);
        expected.add(-2L);
        expected.add(0L);
        expected.add(1L);
        expected.add(Long.MAX_VALUE - 1);
        expected.add(Long.MAX_VALUE);

        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");
        final PermutationIterator<Long> iterator = new PermutationIterator<>(expected);

        while (iterator.hasNext()) {
            final List<Long> permutation = iterator.next();
            try (final WritableLongChunk<Any> chunk = WritableLongChunk.makeWritableChunk(permutation.size())) {
                chunk.setSize(0);
                permutation.forEach(chunk::add);
                chunk.sort();
                TestCase.assertEquals(expected.size(), chunk.size());
                for (int i = 0; i < expected.size(); i++) {
                    TestCase.assertEquals(
                            "Expected " + expected.get(i) + " but got " + chunk.get(i) + " at index " + i,
                            (long) expected.get(i), chunk.get(i));
                }
            }
        }
    }

    @Test
    public void testFloatChunkSort() {
        final List<Float> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(NULL_FLOAT); // NOTE: NULL_FLOAT == -Float.MAX_VALUE
        expected.add(Float.NEGATIVE_INFINITY);
        expected.add(MIN_FINITE_FLOAT);
        expected.add(-Float.MIN_NORMAL);
        expected.add(-0.0f);
        expected.add(+0.0f);
        expected.add(Float.MIN_NORMAL);
        expected.add(Float.MAX_VALUE);
        expected.add(Float.POSITIVE_INFINITY);
        expected.add(Float.NaN);

        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");
        final PermutationIterator<Float> iterator = new PermutationIterator<>(expected);

        while (iterator.hasNext()) {
            final List<Float> permutation = iterator.next();
            try (final WritableFloatChunk<Any> chunk = WritableFloatChunk.makeWritableChunk(permutation.size())) {
                chunk.setSize(0);
                permutation.forEach(chunk::add);
                chunk.sort();
                TestCase.assertEquals(expected.size(), chunk.size());
                for (int i = 0; i < expected.size(); i++) {
                    if (Float.isNaN(expected.get(i))) {
                        TestCase.assertTrue(Float.isNaN(chunk.get(i)));
                    } else if (expected.get(i) != chunk.get(i)) {
                        TestCase.fail("Expected " + expected.get(i) + " but got " + chunk.get(i) + " at index " + i);
                    }
                }
            }
        }
    }

    @Test
    public void testDoubleChunkSort() {
        final List<Double> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(NULL_DOUBLE); // NOTE: NULL_DOUBLE == -Double.MAX_VALUE
        expected.add(Double.NEGATIVE_INFINITY);
        expected.add(MIN_FINITE_DOUBLE);
        expected.add(-Double.MIN_NORMAL);
        expected.add(-0.0);
        expected.add(+0.0);
        expected.add(Double.MIN_NORMAL);
        expected.add(Double.MAX_VALUE);
        expected.add(Double.POSITIVE_INFINITY);
        expected.add(Double.NaN);

        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");
        final PermutationIterator<Double> iterator = new PermutationIterator<>(expected);

        while (iterator.hasNext()) {
            final List<Double> permutation = iterator.next();
            try (final WritableDoubleChunk<Any> chunk = WritableDoubleChunk.makeWritableChunk(permutation.size())) {
                chunk.setSize(0);
                permutation.forEach(chunk::add);
                chunk.sort();
                TestCase.assertEquals(expected.size(), chunk.size());
                for (int i = 0; i < expected.size(); i++) {
                    if (Double.isNaN(expected.get(i))) {
                        TestCase.assertTrue(Double.isNaN(chunk.get(i)));
                    } else {
                        TestCase.assertEquals(
                                "Expected " + expected.get(i) + " but got " + chunk.get(i) + " at index " + i,
                                (double) expected.get(i), chunk.get(i));
                    }
                }
            }
        }
    }

    @Test
    public void testStringChunkSort() {
        final List<String> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(null);
        expected.add("A");
        expected.add("AAAA");
        expected.add("BBBB");
        expected.add("a");
        expected.add("aaaa");
        expected.add("bbbb");

        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");
        final PermutationIterator<String> iterator = new PermutationIterator<>(expected);

        while (iterator.hasNext()) {
            final List<String> permutation = iterator.next();
            try (final WritableObjectChunk<String, ? extends Any> chunk = WritableObjectChunk.makeWritableChunk(permutation.size())) {
                chunk.setSize(0);
                permutation.forEach(chunk::add);
                chunk.sort();
                TestCase.assertEquals(expected.size(), chunk.size());
                for (int i = 0; i < expected.size(); i++) {
                    TestCase.assertEquals(
                            "Expected " + expected.get(i) + " but got " + chunk.get(i) + " at index " + i,
                            expected.get(i), chunk.get(i));
                }
            }
        }
    }

    @Test
    public void testBigIntegerChunkSort() {
        final List<BigInteger> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(null);
        expected.add(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE));
        expected.add(BigInteger.valueOf(Long.MIN_VALUE));
        expected.add(BigInteger.valueOf(-1));
        expected.add(BigInteger.valueOf(0));
        expected.add(BigInteger.valueOf(1));
        expected.add(BigInteger.valueOf(Long.MAX_VALUE));
        expected.add(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));

        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");
        final PermutationIterator<BigInteger> iterator = new PermutationIterator<>(expected);

        while (iterator.hasNext()) {
            final List<BigInteger> permutation = iterator.next();
            try (final WritableObjectChunk<BigInteger, ? extends Any> chunk = WritableObjectChunk.makeWritableChunk(permutation.size())) {
                chunk.setSize(0);
                permutation.forEach(chunk::add);
                chunk.sort();
                TestCase.assertEquals(expected.size(), chunk.size());
                for (int i = 0; i < expected.size(); i++) {
                    TestCase.assertEquals(
                            "Expected " + expected.get(i) + " but got " + chunk.get(i) + " at index " + i,
                            expected.get(i), chunk.get(i));
                }
            }
        }
    }

    @Test
    public void testBigDecimalChunkSort() {
        final List<java.math.BigDecimal> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(null);
        expected.add(new BigDecimal("-1E+1000"));
        expected.add(new BigDecimal("-1E+10"));
        expected.add(new BigDecimal("-1"));
        expected.add(new BigDecimal("0"));
        expected.add(new BigDecimal("1"));
        expected.add(new BigDecimal("1E+10"));
        expected.add(new BigDecimal("1E+1000"));

        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");
        final PermutationIterator<java.math.BigDecimal> iterator = new PermutationIterator<>(expected);

        while (iterator.hasNext()) {
            final List<java.math.BigDecimal> permutation = iterator.next();
            try (final WritableObjectChunk<java.math.BigDecimal, ? extends Any> chunk = WritableObjectChunk.makeWritableChunk(permutation.size())) {
                chunk.setSize(0);
                permutation.forEach(chunk::add);
                chunk.sort();
                TestCase.assertEquals(expected.size(), chunk.size());
                for (int i = 0; i < expected.size(); i++) {
                    TestCase.assertEquals(
                            "Expected " + expected.get(i) + " but got " + chunk.get(i) + " at index " + i,
                            expected.get(i), chunk.get(i));
                }
            }
        }
    }
}
