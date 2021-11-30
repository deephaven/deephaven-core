package io.deephaven.engine.rowset.impl;

// See "Numerical Recipes in C", 7-1 "Uniform Deviates", "An Even Quicker Generator".
public class QuickDirtyRandom {
    private int idum;

    public QuickDirtyRandom(final int seed) {
        idum = seed;
    }

    public QuickDirtyRandom() {
        idum = 1;
    }

    public void next() {
        idum = 1664525 * idum + 1013904223;
    }

    public long curr() {
        return unsigned(idum);
    }

    public void reset(final int x) {
        idum = x;
    }

    public static long unsigned(int x) {
        return x & 0xFFFFFFFFL;
    }
}
