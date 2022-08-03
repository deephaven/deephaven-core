/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.benchmarking.generator.random;

import java.util.Random;

/**
 * A {@link ExtendedRandom} based on an instance of {@link Random}
 */
public class NormalExtendedRandom extends ExtendedRandom {
    private final Random randSrc;

    public NormalExtendedRandom(Random randSrc) {
        this.randSrc = randSrc;
    }

    @Override
    public double nextDouble() {
        return randSrc.nextDouble();
    }

    @Override
    public long nextLong() {
        return randSrc.nextLong();
    }

    @Override
    public int nextInt() {
        return randSrc.nextInt();
    }

    @Override
    public int nextInt(int n) {
        return randSrc.nextInt(n);
    }
}
