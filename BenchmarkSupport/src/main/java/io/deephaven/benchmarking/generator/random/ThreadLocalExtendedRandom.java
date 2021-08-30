package io.deephaven.benchmarking.generator.random;

import java.util.concurrent.ThreadLocalRandom;

/**
 * A {@link ExtendedRandom} that wraps {@link ThreadLocalRandom}.
 */
public class ThreadLocalExtendedRandom extends ExtendedRandom {
    private static final ThreadLocalExtendedRandom INSTANCE = new ThreadLocalExtendedRandom();

    public static ThreadLocalExtendedRandom getInstance() {
        return INSTANCE;
    }

    private ThreadLocalExtendedRandom() {}

    @Override
    public double nextDouble() {
        return ThreadLocalRandom.current().nextDouble();
    }

    @Override
    public long nextLong() {
        return ThreadLocalRandom.current().nextLong();
    }

    @Override
    public int nextInt() {
        return ThreadLocalRandom.current().nextInt();
    }

    @Override
    public int nextInt(int n) {
        return ThreadLocalRandom.current().nextInt(n);
    }
}
