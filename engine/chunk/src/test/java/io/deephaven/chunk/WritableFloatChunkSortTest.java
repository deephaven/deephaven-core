//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.FloatComparisons;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class WritableFloatChunkSortTest {

    private static final float[] SPECIAL_VALUES = new float[] {
            QueryConstants.NULL_FLOAT,
            Float.NEGATIVE_INFINITY,
            QueryConstants.MIN_FINITE_FLOAT,
            -Float.MIN_NORMAL,
            WritableChunkTestUtil.negativeZeroFloat(),
            WritableChunkTestUtil.positiveZeroFloat(),
            Float.MIN_NORMAL,
            Math.nextDown(Float.MAX_VALUE),
            Float.MAX_VALUE,
            Float.POSITIVE_INFINITY,
            Float.NaN,
    };

    @Test
    public void presorted() {
        final float[] x = SPECIAL_VALUES.clone();
        Assert.eqTrue(isSorted(x, 0, x.length), "isSorted(x, 0, x.length)");
        sort(x, 0, x.length);
        Assert.eqTrue(Arrays.equals(x, SPECIAL_VALUES), "Arrays.equals");
    }

    @Test
    public void sortFixesZero() {
        final float[] x = new float[] {
                QueryConstants.NULL_FLOAT,
                Float.NEGATIVE_INFINITY,
                QueryConstants.MIN_FINITE_FLOAT,
                -Float.MIN_NORMAL,
                WritableChunkTestUtil.positiveZeroFloat(),
                WritableChunkTestUtil.negativeZeroFloat(),
                Float.MIN_NORMAL,
                Math.nextDown(Float.MAX_VALUE),
                Float.MAX_VALUE,
                Float.POSITIVE_INFINITY,
                Float.NaN,
        };
        sort(x, 0, x.length);
        Assert.eqTrue(Arrays.equals(x, SPECIAL_VALUES), "Arrays.equals");
    }

    @Test
    public void sortFixesNull() {
        final float[] x = new float[] {
                Float.NEGATIVE_INFINITY,
                QueryConstants.NULL_FLOAT,
                QueryConstants.MIN_FINITE_FLOAT,
                -Float.MIN_NORMAL,
                WritableChunkTestUtil.negativeZeroFloat(),
                WritableChunkTestUtil.positiveZeroFloat(),
                Float.MIN_NORMAL,
                Math.nextDown(Float.MAX_VALUE),
                Float.MAX_VALUE,
                Float.POSITIVE_INFINITY,
                Float.NaN,
        };
        sort(x, 0, x.length);
        Assert.eqTrue(Arrays.equals(x, SPECIAL_VALUES), "Arrays.equals");
    }

    @Test
    public void sortFixesNaN() {
        final float[] x = new float[] {
                Float.NaN,
                QueryConstants.NULL_FLOAT,
                Float.NEGATIVE_INFINITY,
                QueryConstants.MIN_FINITE_FLOAT,
                -Float.MIN_NORMAL,
                WritableChunkTestUtil.negativeZeroFloat(),
                WritableChunkTestUtil.positiveZeroFloat(),
                Float.MIN_NORMAL,
                Math.nextDown(Float.MAX_VALUE),
                Float.MAX_VALUE,
                Float.POSITIVE_INFINITY,
        };
        sort(x, 0, x.length);
        Assert.eqTrue(Arrays.equals(x, SPECIAL_VALUES), "Arrays.equals");
    }

    @Test
    public void zeros() {
        // This is really a test of our test, giving a bit more confidence that isSorted is correct
        Assert.eqFalse(
                isSorted(new float[] {WritableChunkTestUtil.positiveZeroFloat(),
                        WritableChunkTestUtil.negativeZeroFloat()}, 0, 2),
                "isSorted(new float[] { 0.0f, -0.0f }, 0, 2)");
    }

    @Test
    public void biasedBruteSort() {
        final int maxArraySize = 32768;
        // Any changes to sort logic should be manually verified with a larger number of iterations
        final int numIters = 100;
        final long seed = System.currentTimeMillis();
        System.out.println("Seed: " + seed);
        final Random r = new Random(seed);
        for (int arraySize = 2; arraySize <= maxArraySize; arraySize *= 2) {
            final float[] x = new float[arraySize];
            for (int i = 0; i < numIters; ++i) {
                final int a1 = r.nextInt(arraySize);
                final int a2 = r.nextInt(arraySize);
                final int fromInclusive = Math.min(a1, a2);
                final int toExclusive = Math.max(a1, a2);
                biasedFill(r, x, fromInclusive, toExclusive);
                sort(x, fromInclusive, toExclusive);
                {
                    // A bit of extra testing
                    biasedFill(r, x, 0, fromInclusive);
                    sort(x, 0, fromInclusive);

                    biasedFill(r, x, toExclusive, arraySize);
                    sort(x, toExclusive, arraySize);

                    sort(x, 0, arraySize);
                }
            }
        }
    }

    private static void sort(float[] x, int fromInclusive, int toExclusive) {
        // noinspection resource
        final WritableFloatChunk<Any> chunk = WritableFloatChunk.writableChunkWrap(x);
        chunk.sort(fromInclusive, toExclusive - fromInclusive);
        Assert.eqTrue(isSorted(x, fromInclusive, toExclusive), "isSorted(x, fromInclusive, toExclusive)");
    }

    // The crux of this test is centered around the correctness of this method.
    private static boolean isSorted(float[] x, int fromInclusive, int toExclusive) {
        for (int i = fromInclusive + 1; i < toExclusive; ++i) {
            final float a = x[i - 1];
            final float b = x[i];
            if (FloatComparisons.gt(a, b)
                    || (WritableChunkTestUtil.isPositiveZero(a) && WritableChunkTestUtil.isNegativeZero(b))) {
                return false;
            }
        }
        return true;
    }

    private static void biasedFill(Random r, float[] x, int fromInclusive, int toExclusive) {
        for (int i = fromInclusive; i < toExclusive; i++) {
            x[i] = biased(r);
        }
    }

    private static float biased(Random r) {
        final int x = r.nextInt(128);
        if (x < 32) {
            return special(r);
        }
        if (x < 64) {
            return WritableChunkTestUtil.randomBitsFloat(r);
        }
        return r.nextFloat();
    }

    private static float special(Random r) {
        return SPECIAL_VALUES[r.nextInt(SPECIAL_VALUES.length)];
    }
}
