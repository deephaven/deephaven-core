//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmarking.generator;


import io.deephaven.benchmarking.generator.random.ExtendedRandom;
import org.jetbrains.annotations.NotNull;

public class RandomStringGenerator implements ObjectGenerator<String> {
    private final int minLen;
    private final int maxLen;

    private final StringBuilder builder = new StringBuilder();
    private ExtendedRandom random;

    public RandomStringGenerator(int minLen, int maxLen) {
        if (minLen <= 0 || maxLen <= 0) {
            throw new IllegalArgumentException(
                    "minLen and maxLen must be positive! (minLen=" + minLen + ", maxLen-" + maxLen + ')');
        }
        if (minLen > maxLen) {
            throw new IllegalArgumentException(
                    "minLen cannot be greater than maxLen (minLen=" + minLen + ", maxLen-" + maxLen + ')');
        }
        if (maxLen == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("maxLen must be less than Integer.MAX_VALUE");
        }

        this.minLen = minLen;
        this.maxLen = maxLen;
    }

    @Override
    public void init(@NotNull final ExtendedRandom random) {
        this.random = random;
    }

    @Override
    public String get() {
        final int len = minLen + random.nextInt(maxLen - minLen + 1);

        builder.setLength(0);
        for (int i = 0; i < len; i++) {
            builder.append((char) ((int) 'A' + random.nextInt(26)));
        }

        return builder.toString();
    }
}
