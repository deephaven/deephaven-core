/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import org.apache.commons.lang3.ArrayUtils;

import static io.deephaven.function.Random.*;

/**
 * Test Random.
 */
public class TestRandom extends BaseArrayTestCase {
    final int n = 10000;

    public void testRandom() {
        final double[] vals = new double[n];

        for (int i = 0; i < n; i++) {
            vals[i] = random();
        }

        final double min = Numeric.min(vals);
        final double max = Numeric.max(vals);
        final double avg = Numeric.avg(vals);
        final double std = Numeric.std(vals);

        assertTrue(0 <= min);
        assertTrue(max <= 1);
        assertEquals(0.5, avg, 0.025);
        assertEquals(Math.sqrt(1.0 / 12.0), std, 0.05);
    }

    public void testRandomBool() {
        final boolean[] vals = randomBool(n);

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
        final int[] vals = randomInt(a, b, n);

        final int[] d = Basic.distinct(vals);
        assertEquals(b - a, d.length);

        for (int i = a; i < b; i++) {
            assertTrue(ArrayUtils.contains(d, i));
        }
    }

    public void testRandomLong() {
        final int a = 5;
        final int b = 11;
        final long[] vals = randomLong(a, b, n);

        final long[] d = Basic.distinct(vals);
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
        final float[] vals = randomFloat(a, b, n);

        final double min = Numeric.min(vals);
        final double max = Numeric.max(vals);
        final double avg = Numeric.avg(vals);
        final double std = Numeric.std(vals);

        assertTrue(a <= min);
        assertTrue(max <= b);
        assertEquals((a + b) / 2, avg, 0.025 * (b - a));
        assertEquals(Math.sqrt(1.0 / 12.0 * (b - a) * (b - a)), std, 0.05 * (b - a));
    }

    @SuppressWarnings("RedundantCast")
    public void testRandomDouble() {
        final double a = (double) 4.5;
        final double b = (double) 95.6;
        final double[] vals = randomDouble(a, b, n);

        final double min = Numeric.min(vals);
        final double max = Numeric.max(vals);
        final double avg = Numeric.avg(vals);
        final double std = Numeric.std(vals);

        assertTrue(a <= min);
        assertTrue(max <= b);
        assertEquals((a + b) / 2, avg, 0.025 * (b - a));
        assertEquals(Math.sqrt(1.0 / 12.0 * (b - a) * (b - a)), std, 0.05 * (b - a));
    }

    @SuppressWarnings("RedundantCast")
    public void testRandomGaussian() {
        final double m = (double) 4.5;
        final double s = (double) 2.6;
        final double[] vals = randomGaussian(m, s, n * 10);

        final double avg = Numeric.avg(vals);
        final double std = Numeric.std(vals);

        assertEquals(m, avg, 0.2);
        assertEquals(s, std, 0.2);
    }

}
