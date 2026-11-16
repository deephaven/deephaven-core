//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.staticpercentile;

import io.deephaven.util.compare.ObjectComparisons;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Random;
import java.util.function.IntUnaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StaticPercentileSelectTest {
    private static final int[] SIZES = {1, 2, 3, 15, 16, 17, 31, 100, 1000, 10_007};

    /**
     * Value generators by shape; the argument is the index and the size is captured by the caller.
     */
    private static IntUnaryOperator[] shapes(final int size, final Random random) {
        return new IntUnaryOperator[] {
                ii -> random.nextInt(),
                ii -> ii,
                ii -> size - ii,
                ii -> 7,
                ii -> random.nextInt(3),
                ii -> ii % 2 == 0 ? ii : size - ii,
        };
    }

    @Test
    public void testLong() {
        final Random random = new Random(0xC0FFEE);
        for (final int size : SIZES) {
            for (final IntUnaryOperator shape : shapes(size, random)) {
                final long[] values = new long[size];
                for (int ii = 0; ii < size; ++ii) {
                    values[ii] = shape.applyAsInt(ii);
                }
                final long[] sorted = values.clone();
                Arrays.sort(sorted);
                for (final int position : positions(size)) {
                    final long[] array = Arrays.copyOf(values, size + 3);
                    LongStaticPercentileOperator.select(array, size, position);
                    assertEquals(sorted[position], array[position]);
                    for (int ii = 0; ii < size; ++ii) {
                        assertTrue(ii < position ? array[ii] <= array[position] : array[ii] >= array[position]);
                    }
                    if (position + 1 < size) {
                        assertEquals(sorted[position + 1], LongStaticPercentileOperator.min(array, position + 1, size));
                    }
                }
            }
        }
    }

    @Test
    public void testChar() {
        final Random random = new Random(0xCAFE);
        for (final int size : SIZES) {
            for (final IntUnaryOperator shape : shapes(size, random)) {
                final char[] values = new char[size];
                for (int ii = 0; ii < size; ++ii) {
                    values[ii] = (char) (shape.applyAsInt(ii) & 0x7FFF);
                }
                final char[] sorted = values.clone();
                Arrays.sort(sorted);
                for (final int position : positions(size)) {
                    final char[] array = Arrays.copyOf(values, size + 3);
                    CharStaticPercentileOperator.select(array, size, position);
                    assertEquals(sorted[position], array[position]);
                    for (int ii = 0; ii < size; ++ii) {
                        assertTrue(ii < position ? array[ii] <= array[position] : array[ii] >= array[position]);
                    }
                    if (position + 1 < size) {
                        assertEquals(sorted[position + 1], CharStaticPercentileOperator.min(array, position + 1, size));
                    }
                }
            }
        }
    }

    @Test
    public void testDouble() {
        final Random random = new Random(0xBEEF);
        for (final int size : SIZES) {
            for (final IntUnaryOperator shape : shapes(size, random)) {
                final double[] values = new double[size];
                for (int ii = 0; ii < size; ++ii) {
                    values[ii] = shape.applyAsInt(ii) / 4.0;
                }
                final double[] sorted = values.clone();
                Arrays.sort(sorted);
                for (final int position : positions(size)) {
                    final double[] array = Arrays.copyOf(values, size + 3);
                    DoubleStaticPercentileOperator.select(array, size, position);
                    assertEquals(sorted[position], array[position], 0);
                    for (int ii = 0; ii < size; ++ii) {
                        assertTrue(ii < position ? array[ii] <= array[position] : array[ii] >= array[position]);
                    }
                    if (position + 1 < size) {
                        assertEquals(sorted[position + 1],
                                DoubleStaticPercentileOperator.min(array, position + 1, size), 0);
                    }
                }
            }
        }
    }

    @Test
    public void testObject() {
        final Random random = new Random(0xFACE);
        for (final int size : SIZES) {
            for (final IntUnaryOperator shape : shapes(size, random)) {
                final Object[] values = new Object[size];
                for (int ii = 0; ii < size; ++ii) {
                    values[ii] = BigDecimal.valueOf(shape.applyAsInt(ii));
                }
                final Object[] sorted = values.clone();
                Arrays.sort(sorted, ObjectComparisons::compare);
                for (final int position : positions(size)) {
                    final Object[] array = Arrays.copyOf(values, size + 3);
                    ObjectStaticPercentileOperator.select(array, size, position);
                    assertEquals(sorted[position], array[position]);
                    for (int ii = 0; ii < size; ++ii) {
                        final int comparison = ObjectComparisons.compare(array[ii], array[position]);
                        assertTrue(ii < position ? comparison <= 0 : comparison >= 0);
                    }
                }
            }
        }
    }

    private static int[] positions(final int size) {
        return Arrays.stream(new int[] {0, size / 4, size / 2, (size - 1) / 2, size - 2, size - 1})
                .filter(position -> position >= 0)
                .distinct()
                .toArray();
    }
}
