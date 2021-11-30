package io.deephaven.engine.testutil;

import gnu.trove.list.array.TLongArrayList;

import java.util.Random;

/**
 * Fisher–Yates shuffle
 */
public class Shuffle {

    public static void shuffleArray(Random r, int[] ar) {
        for (int i = ar.length - 1; i > 0; --i) {
            final int j = r.nextInt(i + 1);
            final int a = ar[j];
            ar[j] = ar[i];
            ar[i] = a;
        }
    }

    public static void shuffleArray(final Random r, long[] ar) {
        for (int i = ar.length - 1; i > 0; --i) {
            final int j = r.nextInt(i + 1);
            final long a = ar[j];
            ar[j] = ar[i];
            ar[i] = a;
        }
    }

    public static void shuffleArray(Random r, final TLongArrayList ar) {
        shuffle(r, ar.size(), (final int i, final int j) -> {
            final long a = ar.get(j);
            ar.set(j, ar.get(i));
            ar.set(i, a);
        });
    }

    private static void shuffle(final Random rnd, final int n, final Swapper swapper) {
        for (int i = n - 1; i > 0; --i) {
            final int j = rnd.nextInt(i + 1);
            swapper.swap(i, j);
        }
    }

    private interface Swapper {
        void swap(int i, int j);
    }
}
