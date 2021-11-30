/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import org.apache.commons.lang3.ArrayUtils;

public class TestSpecialPrimitives extends BaseArrayTestCase {
    final int n = 4000;

    public void testRandom() {
        final double[] vals = new double[n];

        for (int i = 0; i < n; i++) {
            vals[i] = SpecialPrimitives.random();
        }

        final double min = DoubleNumericPrimitives.min(vals);
        final double max = DoubleNumericPrimitives.max(vals);
        final double avg = DoubleNumericPrimitives.avg(vals);
        final double std = DoubleNumericPrimitives.std(vals);

        assertTrue(0 <= min);
        assertTrue(max <= 1);
        assertEquals(0.5, avg, 0.025);
        assertEquals(Math.sqrt(1.0 / 12.0), std, 0.05);
    }

    public void testRandomBool() {
        final boolean[] vals = SpecialPrimitives.randomBool(n);

        for (boolean i : new boolean[] {true, false}) {
            boolean found = false;
            for (boolean v : vals) {
                if (v == i) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }
    }

    public void testRandomInt() {
        final int a = 5;
        final int b = 11;
        final int[] vals = SpecialPrimitives.randomInt(a, b, n);

        final int[] d = IntegerPrimitives.distinct(vals);
        assertEquals(b - a, d.length);

        for (int i = a; i < b; i++) {
            assertTrue(ArrayUtils.contains(d, i));
        }
    }

    public void testRandomLong() {
        final int a = 5;
        final int b = 11;
        final long[] vals = SpecialPrimitives.randomLong(a, b, n);

        final long[] d = LongPrimitives.distinct(vals);
        assertEquals(b - a, d.length);

        for (long i = a; i < b; i++) {
            boolean found = false;
            for (long v : d) {
                if (v == i) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }
    }

    public void testRandomFloat() {
        final float a = (float) 4.5;
        final float b = (float) 95.6;
        final float[] vals = SpecialPrimitives.randomFloat(a, b, n);

        final double min = FloatNumericPrimitives.min(vals);
        final double max = FloatNumericPrimitives.max(vals);
        final double avg = FloatNumericPrimitives.avg(vals);
        final double std = FloatNumericPrimitives.std(vals);

        assertTrue(a <= min);
        assertTrue(max <= b);
        assertEquals((a + b) / 2, avg, 0.025 * (b - a));
        assertEquals(Math.sqrt(1.0 / 12.0 * (b - a) * (b - a)), std, 0.05 * (b - a));
    }

    public void testRandomDouble() {
        final double a = (double) 4.5;
        final double b = (double) 95.6;
        final double[] vals = SpecialPrimitives.randomDouble(a, b, n);

        final double min = DoubleNumericPrimitives.min(vals);
        final double max = DoubleNumericPrimitives.max(vals);
        final double avg = DoubleNumericPrimitives.avg(vals);
        final double std = DoubleNumericPrimitives.std(vals);

        assertTrue(a <= min);
        assertTrue(max <= b);
        assertEquals((a + b) / 2, avg, 0.025 * (b - a));
        assertEquals(Math.sqrt(1.0 / 12.0 * (b - a) * (b - a)), std, 0.05 * (b - a));
    }

    public void testRandomGaussian() {
        final double m = (double) 4.5;
        final double s = (double) 2.6;
        final double[] vals = SpecialPrimitives.randomGaussian(m, s, n);

        final double avg = DoubleNumericPrimitives.avg(vals);
        final double std = DoubleNumericPrimitives.std(vals);

        assertEquals(m, avg, 0.2);
        assertEquals(s, std, 0.2);
    }

}
