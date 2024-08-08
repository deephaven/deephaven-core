//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.CharComparisons;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class WritableCharChunkSortTest {

    private static final char[] SPECIAL_VALUES = new char[] {
            QueryConstants.NULL_CHAR,
            Character.MIN_VALUE,
            Character.MIN_VALUE + 1,
            Character.MAX_VALUE - 1
    };

    @Test
    public void presorted() {
        final char[] x = SPECIAL_VALUES.clone();
        Assert.eqTrue(isSorted(x, 0, x.length), "isSorted(x, 0, x.length)");
        sort(x, 0, x.length);
        Assert.eqTrue(Arrays.equals(x, SPECIAL_VALUES), "Arrays.equals");
    }

    @Test
    public void sortFixesNull() {
        final char[] x = new char[] {
                Character.MIN_VALUE,
                Character.MIN_VALUE + 1,
                Character.MAX_VALUE - 1,
                QueryConstants.NULL_CHAR,
        };
        sort(x, 0, x.length);
        Assert.eqTrue(Arrays.equals(x, SPECIAL_VALUES), "Arrays.equals");
    }

    @Test
    public void biasedBruteSort() {
        final int maxArraySize = 3;
        // Any changes to sort logic should be manually verified with a larger number of iterations
        final int numIters = 100;
        final long seed = System.currentTimeMillis();
        System.out.println("Seed: " + seed);
        final Random r = new Random(seed);
        for (int arraySize = 3; arraySize <= maxArraySize; arraySize *= 2) {
            final char[] x = new char[arraySize];
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

    private static void sort(char[] x, int fromInclusive, int toExclusive) {
        // noinspection resource
        final WritableCharChunk<Any> chunk = WritableCharChunk.writableChunkWrap(x);
        chunk.sort(fromInclusive, toExclusive - fromInclusive);
        Assert.eqTrue(isSorted(x, fromInclusive, toExclusive), "isSorted(x, fromInclusive, toExclusive)");
    }

    // The crux of this test is centered around the correctness of this method.
    private static boolean isSorted(char[] x, int fromInclusive, int toExclusive) {
        for (int i = fromInclusive + 1; i < toExclusive; ++i) {
            final char a = x[i - 1];
            final char b = x[i];
            if (CharComparisons.gt(a, b)) {
                return false;
            }
        }
        return true;
    }

    private static void biasedFill(Random r, char[] x, int fromInclusive, int toExclusive) {
        for (int i = fromInclusive; i < toExclusive; i++) {
            x[i] = biased(r);
        }
    }

    private static char biased(Random r) {
        final int x = r.nextInt(128);
        if (x < 32) {
            return special(r);
        }
        return (char) r.nextInt();
    }

    private static char special(Random r) {
        return SPECIAL_VALUES[r.nextInt(SPECIAL_VALUES.length)];
    }
}
