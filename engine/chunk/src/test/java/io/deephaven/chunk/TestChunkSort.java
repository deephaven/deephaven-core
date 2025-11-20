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

    final static int MAX_OFFSET = 100;
    final static int[] OFFSET_ARRAY = new int[] {0, 1, MAX_OFFSET / 2, MAX_OFFSET};

    // These assertions reflect sorting assumptions made by chunk sorting code. If these assertions
    // change, this code will need to be carefully audited.
    @Test
    public void testValueTypeAssertions() {
        TestCase.assertEquals(NULL_CHAR, Character.MAX_VALUE);
        TestCase.assertEquals(NULL_BYTE, Byte.MIN_VALUE);
        TestCase.assertEquals(NULL_SHORT, Short.MIN_VALUE);
        TestCase.assertEquals(NULL_INT, Integer.MIN_VALUE);
        TestCase.assertEquals(NULL_LONG, Long.MIN_VALUE);
        TestCase.assertEquals(NULL_FLOAT, -Float.MAX_VALUE);
        TestCase.assertEquals(NULL_DOUBLE, -Double.MAX_VALUE);

        TestCase.assertTrue(Float.NEGATIVE_INFINITY < NULL_FLOAT);
        TestCase.assertTrue(Double.NEGATIVE_INFINITY < NULL_DOUBLE);
    }

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

        validateCharChunkSort(expected);
    }

    @Test
    public void testCharChunkSortSpecialCases() {
        final List<Character> expected = new ArrayList<>();

        // Zero values
        expected.clear();
        validateCharChunkSort(expected);

        // Single value
        expected.clear();
        expected.add((char) 10);
        validateCharChunkSort(expected);

        // All null
        expected.clear();
        expected.add(NULL_CHAR);
        expected.add(NULL_CHAR);
        expected.add(NULL_CHAR);
        validateCharChunkSort(expected);

        // No null
        expected.clear();
        expected.add((char) 0);
        expected.add((char) 1);
        expected.add((char) 2);
        expected.add((char) 3);
        validateCharChunkSort(expected);
    }

    private static void validateCharChunkSort(List<Character> expected) {
        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");

        final int setSize = expected.size();

        // Reusable chunk large enough to hold any permutation plus the max offset.
        try (final WritableCharChunk<Any> chunk = WritableCharChunk.makeWritableChunk(setSize + MAX_OFFSET)) {
            final PermutationIterator<Character> iterator = new PermutationIterator<>(expected);
            while (iterator.hasNext()) {
                final List<Character> permutation = iterator.next();

                for (final int offset : OFFSET_ARRAY) {
                    chunk.setSize(offset);
                    permutation.forEach(chunk::add);
                    chunk.sort(offset, setSize);
                    TestCase.assertEquals(expected.size(), chunk.size() - offset);
                    for (int i = 0; i < expected.size(); i++) {
                        TestCase.assertEquals(
                                "Expected " + expected.get(i) + " but got " + chunk.get(i + offset) + " at index " + i,
                                (char) expected.get(i), chunk.get(i + offset));
                    }
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

        validateByteChunkSort(expected);
    }

    private static void validateByteChunkSort(List<Byte> expected) {
        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");

        final int setSize = expected.size();

        // Reusable chunk large enough to hold any permutation plus the max offset.
        try (final WritableByteChunk<Any> chunk = WritableByteChunk.makeWritableChunk(setSize + MAX_OFFSET)) {
            final PermutationIterator<Byte> iterator = new PermutationIterator<>(expected);
            while (iterator.hasNext()) {
                final List<Byte> permutation = iterator.next();

                for (final int offset : OFFSET_ARRAY) {
                    chunk.setSize(offset);
                    permutation.forEach(chunk::add);
                    chunk.sort(offset, setSize);
                    TestCase.assertEquals(expected.size(), chunk.size() - offset);
                    for (int i = 0; i < expected.size(); i++) {
                        TestCase.assertEquals(
                                "Expected " + expected.get(i) + " but got " + chunk.get(i + offset) + " at index " + i,
                                (byte) expected.get(i), chunk.get(i + offset));
                    }
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

        validateShortChunkSort(expected);
    }

    private static void validateShortChunkSort(List<Short> expected) {
        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");

        final int setSize = expected.size();

        // Reusable chunk large enough to hold any permutation plus the max offset.
        try (final WritableShortChunk<Any> chunk = WritableShortChunk.makeWritableChunk(setSize + MAX_OFFSET)) {
            final PermutationIterator<Short> iterator = new PermutationIterator<>(expected);
            while (iterator.hasNext()) {
                final List<Short> permutation = iterator.next();

                for (final int offset : OFFSET_ARRAY) {
                    chunk.setSize(offset);
                    permutation.forEach(chunk::add);
                    chunk.sort(offset, setSize);
                    TestCase.assertEquals(expected.size(), chunk.size() - offset);
                    for (int i = 0; i < expected.size(); i++) {
                        TestCase.assertEquals(
                                "Expected " + expected.get(i) + " but got " + chunk.get(i + offset) + " at index " + i,
                                (short) expected.get(i), chunk.get(i + offset));
                    }
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

        validateIntChunkSort(expected);
    }

    private static void validateIntChunkSort(List<Integer> expected) {
        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");

        final int setSize = expected.size();

        // Reusable chunk large enough to hold any permutation plus the max offset.
        try (final WritableIntChunk<Any> chunk = WritableIntChunk.makeWritableChunk(setSize + MAX_OFFSET)) {
            final PermutationIterator<Integer> iterator = new PermutationIterator<>(expected);
            while (iterator.hasNext()) {
                final List<Integer> permutation = iterator.next();

                for (final int offset : OFFSET_ARRAY) {
                    chunk.setSize(offset);
                    permutation.forEach(chunk::add);
                    chunk.sort(offset, setSize);
                    TestCase.assertEquals(expected.size(), chunk.size() - offset);
                    for (int i = 0; i < expected.size(); i++) {
                        TestCase.assertEquals(
                                "Expected " + expected.get(i) + " but got " + chunk.get(i + offset) + " at index " + i,
                                (int) expected.get(i), chunk.get(i + offset));
                    }
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

        validateLongChunkSort(expected);
    }

    private static void validateLongChunkSort(List<Long> expected) {
        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");

        final int setSize = expected.size();

        // Reusable chunk large enough to hold any permutation plus the max offset.
        try (final WritableLongChunk<Any> chunk = WritableLongChunk.makeWritableChunk(setSize + MAX_OFFSET)) {
            final PermutationIterator<Long> iterator = new PermutationIterator<>(expected);
            while (iterator.hasNext()) {
                final List<Long> permutation = iterator.next();

                for (final int offset : OFFSET_ARRAY) {
                    chunk.setSize(offset);
                    permutation.forEach(chunk::add);
                    chunk.sort(offset, setSize);
                    TestCase.assertEquals(expected.size(), chunk.size() - offset);
                    for (int i = 0; i < expected.size(); i++) {
                        TestCase.assertEquals(
                                "Expected " + expected.get(i) + " but got " + chunk.get(i + offset) + " at index " + i,
                                (long) expected.get(i), chunk.get(i + offset));
                    }
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

        validateFloatChunkSort(expected);
    }

    @Test
    public void testFloatChunkSortSpecialCases() {
        final List<Float> expected = new ArrayList<>();

        // Zero values
        expected.clear();
        validateFloatChunkSort(expected);

        // Single value
        expected.clear();
        expected.add(10.0f);
        validateFloatChunkSort(expected);

        // No negative infinity
        expected.clear();
        expected.add(NULL_FLOAT);
        expected.add(1.0f);
        expected.add(2.0f);
        expected.add(3.0f);
        expected.add(Float.POSITIVE_INFINITY);
        expected.add(Float.NaN);
        validateFloatChunkSort(expected);

        // No NULL_FLOAT
        expected.clear();
        expected.add(Float.NEGATIVE_INFINITY);
        expected.add(1.0f);
        expected.add(2.0f);
        expected.add(3.0f);
        expected.add(Float.POSITIVE_INFINITY);
        expected.add(Float.NaN);
        validateFloatChunkSort(expected);

        // Only NULL_FLOAT, negative infinity
        expected.clear();
        expected.add(NULL_FLOAT);
        expected.add(NULL_FLOAT);
        expected.add(Float.NEGATIVE_INFINITY);
        expected.add(Float.NEGATIVE_INFINITY);
        validateFloatChunkSort(expected);
    }

    private static void validateFloatChunkSort(List<Float> expected) {
        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");

        final int setSize = expected.size();

        // Reusable chunk large enough to hold any permutation plus the max offset.
        try (final WritableFloatChunk<Any> chunk = WritableFloatChunk.makeWritableChunk(setSize + MAX_OFFSET)) {
            final PermutationIterator<Float> iterator = new PermutationIterator<>(expected);
            while (iterator.hasNext()) {
                final List<Float> permutation = iterator.next();

                for (final int offset : OFFSET_ARRAY) {
                    chunk.setSize(offset);
                    permutation.forEach(chunk::add);
                    chunk.sort(offset, setSize);
                    TestCase.assertEquals(expected.size(), chunk.size() - offset);
                    for (int i = 0; i < expected.size(); i++) {
                        if (Float.isNaN(expected.get(i))) {
                            TestCase.assertTrue(Float.isNaN(chunk.get(i + offset)));
                        } else if (expected.get(i) != chunk.get(i + offset)) {
                            TestCase.fail("Expected " + expected.get(i) + " but got " + chunk.get(i + offset)
                                    + " at index " + i);
                        }
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

        validateDoubleChunkSort(expected);
    }

    @Test
    public void testDoubleChunkSortSpecialCases() {
        final List<Double> expected = new ArrayList<>();

        // Zero values
        expected.clear();
        validateDoubleChunkSort(expected);

        // Single value
        expected.clear();
        expected.add(10.0);
        validateDoubleChunkSort(expected);

        // No negative infinity
        expected.clear();
        expected.add(NULL_DOUBLE);
        expected.add(1.0);
        expected.add(2.0);
        expected.add(3.0);
        expected.add(Double.POSITIVE_INFINITY);
        expected.add(Double.NaN);
        validateDoubleChunkSort(expected);

        // No NULL_DOUBLE
        expected.clear();
        expected.add(Double.NEGATIVE_INFINITY);
        expected.add(1.0);
        expected.add(2.0);
        expected.add(3.0);
        expected.add(Double.POSITIVE_INFINITY);
        expected.add(Double.NaN);
        validateDoubleChunkSort(expected);

        // Only NULL_DOUBLE, negative infinity
        expected.clear();
        expected.add(NULL_DOUBLE);
        expected.add(NULL_DOUBLE);
        expected.add(Double.NEGATIVE_INFINITY);
        expected.add(Double.NEGATIVE_INFINITY);
        validateDoubleChunkSort(expected);
    }

    private static void validateDoubleChunkSort(List<Double> expected) {
        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");

        final int setSize = expected.size();

        // Reusable chunk large enough to hold any permutation plus the max offset.
        try (final WritableDoubleChunk<Any> chunk = WritableDoubleChunk.makeWritableChunk(setSize + MAX_OFFSET)) {
            final PermutationIterator<Double> iterator = new PermutationIterator<>(expected);
            while (iterator.hasNext()) {
                final List<Double> permutation = iterator.next();

                for (final int offset : OFFSET_ARRAY) {
                    chunk.setSize(offset);
                    permutation.forEach(chunk::add);
                    chunk.sort(offset, setSize);
                    TestCase.assertEquals(expected.size(), chunk.size() - offset);
                    for (int i = 0; i < expected.size(); i++) {
                        if (Double.isNaN(expected.get(i))) {
                            TestCase.assertTrue(Double.isNaN(chunk.get(i + offset)));
                        } else if (expected.get(i) != chunk.get(i + offset)) {
                            TestCase.fail("Expected " + expected.get(i) + " but got " + chunk.get(i + offset)
                                    + " at index " + i);
                        }
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

        validateStringChunkSort(expected);
    }

    private static void validateStringChunkSort(List<String> expected) {
        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");

        final int setSize = expected.size();

        // Reusable chunk large enough to hold any permutation plus the max offset.
        try (final WritableObjectChunk<String, Any> chunk =
                WritableObjectChunk.makeWritableChunk(setSize + MAX_OFFSET)) {
            final PermutationIterator<String> iterator = new PermutationIterator<>(expected);
            while (iterator.hasNext()) {
                final List<String> permutation = iterator.next();

                for (final int offset : OFFSET_ARRAY) {
                    chunk.setSize(offset);
                    permutation.forEach(chunk::add);
                    chunk.sort(offset, setSize);
                    TestCase.assertEquals(expected.size(), chunk.size() - offset);
                    for (int i = 0; i < expected.size(); i++) {
                        TestCase.assertEquals(
                                "Expected " + expected.get(i) + " but got " + chunk.get(i + offset) + " at index " + i,
                                expected.get(i), chunk.get(i + offset));
                    }
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

        validateBigIntegerChunkSort(expected);
    }

    private static void validateBigIntegerChunkSort(List<BigInteger> expected) {
        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");

        final int setSize = expected.size();

        // Reusable chunk large enough to hold any permutation plus the max offset.
        try (final WritableObjectChunk<BigInteger, Any> chunk =
                WritableObjectChunk.makeWritableChunk(setSize + MAX_OFFSET)) {
            final PermutationIterator<BigInteger> iterator = new PermutationIterator<>(expected);
            while (iterator.hasNext()) {
                final List<BigInteger> permutation = iterator.next();

                for (final int offset : OFFSET_ARRAY) {
                    chunk.setSize(offset);
                    permutation.forEach(chunk::add);
                    chunk.sort(offset, setSize);
                    TestCase.assertEquals(expected.size(), chunk.size() - offset);
                    for (int i = 0; i < expected.size(); i++) {
                        TestCase.assertEquals(
                                "Expected " + expected.get(i) + " but got " + chunk.get(i + offset) + " at index " + i,
                                expected.get(i), chunk.get(i + offset));
                    }
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

        validateBigDecimalChunkSort(expected);
    }

    private static void validateBigDecimalChunkSort(List<BigDecimal> expected) {
        System.out.println("Exploring " + PermutationIterator.permutationCount(expected.size()) + " permutations");

        final int setSize = expected.size();

        // Reusable chunk large enough to hold any permutation plus the max offset.
        try (final WritableObjectChunk<BigDecimal, Any> chunk =
                WritableObjectChunk.makeWritableChunk(setSize + MAX_OFFSET)) {
            final PermutationIterator<BigDecimal> iterator = new PermutationIterator<>(expected);
            while (iterator.hasNext()) {
                final List<BigDecimal> permutation = iterator.next();

                for (final int offset : OFFSET_ARRAY) {
                    chunk.setSize(offset);
                    permutation.forEach(chunk::add);
                    chunk.sort(offset, setSize);
                    TestCase.assertEquals(expected.size(), chunk.size() - offset);
                    for (int i = 0; i < expected.size(); i++) {
                        TestCase.assertEquals(
                                "Expected " + expected.get(i) + " but got " + chunk.get(i + offset) + " at index " + i,
                                expected.get(i), chunk.get(i + offset));
                    }
                }
            }
        }
    }
}
