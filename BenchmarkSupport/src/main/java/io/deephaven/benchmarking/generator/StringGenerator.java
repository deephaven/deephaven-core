package io.deephaven.benchmarking.generator;


import io.deephaven.benchmarking.generator.random.ExtendedRandom;
import io.deephaven.benchmarking.generator.random.NormalExtendedRandom;
import io.deephaven.benchmarking.generator.random.ThreadLocalExtendedRandom;

import java.util.Random;
import java.util.stream.Stream;

public class StringGenerator implements DataGenerator<String> {
    private final int minLen;
    private final int maxLen;

    private final ExtendedRandom random;
    private final StringBuilder builder = new StringBuilder();

    public StringGenerator() {
        this(16);
    }

    public StringGenerator(int maxLen) {
        this(1, maxLen);
    }

    public StringGenerator(int minLen, int maxLen) {
        this(minLen, maxLen, ThreadLocalExtendedRandom.getInstance());
    }

    public StringGenerator(int minLen, int maxLen, long seed) {
        this(minLen, maxLen, new NormalExtendedRandom(new Random(seed)));
    }

    public StringGenerator(int minLen, int maxLen, ExtendedRandom random) {
        if (minLen <= 0 || maxLen <= 0) {
            throw new IllegalArgumentException("minLen and maxLen must be positive! (minLen="
                + minLen + ", maxLen-" + maxLen + ')');
        }
        if (minLen > maxLen) {
            throw new IllegalArgumentException("minLen cannot be greater than maxLen (minLen="
                + minLen + ", maxLen-" + maxLen + ')');
        }
        if (maxLen == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("maxLen must be less than Integer.MAX_VALUE");
        }

        this.minLen = minLen;
        this.maxLen = maxLen;
        this.random = random;
    }

    public String get() {
        final int len = minLen + random.nextInt(maxLen - minLen + 1);

        builder.setLength(0);
        for (int i = 0; i < len; i++) {
            builder.append((char) ((int) 'A' + random.nextInt(26)));
        }

        return builder.toString();
    }

    public Stream<String> stream() {
        final Stream<String> generate = Stream.generate(this::get);
        if (random == null)
            return generate.parallel();
        else
            return generate;
    }
}
