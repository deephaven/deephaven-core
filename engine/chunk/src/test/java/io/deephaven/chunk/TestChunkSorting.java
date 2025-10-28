//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.*;

import static io.deephaven.util.QueryConstants.*;

/**
 * Verify expectations about chunk sorting for NULL / NaN and regular values. Performs exhaustive testing of all
 * permutations of an expected data set for each primitive type.
 */
public class TestChunkSorting {

    // region PermutationIterator
    public static class PermutationIterator<E extends Comparable<E>> implements Iterator<List<E>> {

        private final List<E> currentPermutation;
        private boolean firstPermutation = true;
        private boolean hasNextPermutation;

        public PermutationIterator(List<E> initialList) {
            // Sort the initial list to start with the lexicographically smallest permutation
            this.currentPermutation = new java.util.ArrayList<>(initialList);
            Collections.sort(this.currentPermutation);
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
                if (currentPermutation.get(i).compareTo(currentPermutation.get(i + 1)) < 0) {
                    k = i;
                    break;
                }
            }

            if (k == -1) { // Current permutation is the last one
                hasNextPermutation = false;
            } else {
                int l = -1;
                for (int i = currentPermutation.size() - 1; i > k; i--) {
                    if (currentPermutation.get(k).compareTo(currentPermutation.get(i)) < 0) {
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
    public void testChar() {
        final List<Character> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(NULL_CHAR);
        expected.add(Character.MIN_VALUE);
        expected.add((char) 0);
        expected.add((char) 1);
        expected.add((char) 2);
        expected.add((char) 3);
        expected.add((char) (Character.MAX_VALUE - 1)); // NOTE: Character.MAX_VALUE == NULL_DOUBLE

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
    public void testByte() {
        final List<Byte> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(NULL_BYTE);
        expected.add((byte) (Byte.MIN_VALUE + 1)); // NOTE: Byte.MIN_VALUE == NULL_BYTE
        expected.add((byte) -2);
        expected.add((byte) 0);
        expected.add((byte) 1);
        expected.add((byte) (Byte.MAX_VALUE - 1)); // NOTE: Byte.MIN_VALUE == NULL_BYTE
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
    public void testShort() {
        final List<Short> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(NULL_SHORT);
        expected.add((short) (Short.MIN_VALUE + 1)); // NOTE: Short.MIN_VALUE == NULL_SHORT
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
    public void testInt() {
        final List<Integer> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(NULL_INT);
        expected.add(Integer.MIN_VALUE + 1); // NOTE: Integer.MIN_VALUE == NULL_INT
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
    public void testLong() {
        final List<Long> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(NULL_LONG);
        expected.add(Long.MIN_VALUE + 1); // NOTE: Long.MIN_VALUE == NULL_LONG
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
    public void testFloat() {
        final List<Float> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(NULL_FLOAT);
        expected.add(Float.NEGATIVE_INFINITY);
        expected.add(Math.nextUp(-Float.MAX_VALUE)); // NOTE: -Float.MAX_VALUE == NULL_FLOAT
        expected.add(-2.0f);
        expected.add(-0.0f);
        expected.add(+0.0f);
        expected.add(1.0f);
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
    public void testDouble() {
        final List<Double> expected = new ArrayList<>();
        // this is explicitly the order we expect after sorting
        expected.add(NULL_DOUBLE);
        expected.add(Double.NEGATIVE_INFINITY);
        expected.add(Math.nextUp(-Double.MAX_VALUE)); // NOTE: -Double.MAX_VALUE == NULL_DOUBLE
        expected.add(-2.0);
        expected.add(-0.0);
        expected.add(+0.0);
        expected.add(1.0);
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
}
