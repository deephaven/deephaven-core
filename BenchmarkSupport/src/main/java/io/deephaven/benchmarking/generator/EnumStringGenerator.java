//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmarking.generator;

import io.deephaven.benchmarking.generator.random.ExtendedRandom;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * A {@link ColumnGenerator<String>} that sources values from a fixed set of values, either randomly, or in rotation.
 */
public class EnumStringGenerator extends RandomStringGenerator {
    public enum Mode {
        Random, Rotate,
    }

    private ExtendedRandom random;

    private String[] enumVals;
    private int enumIndex;

    private final int nVals;
    private final Mode mode;
    private final long enumSeed;

    public EnumStringGenerator(int nVals, long enumSeed, Mode mode, final int minLength, final int maxLength) {
        super(minLength, maxLength);

        this.enumSeed = enumSeed;
        this.mode = mode;
        this.nVals = nVals;
    }

    @Override
    public void init(@NotNull ExtendedRandom random) {
        this.random = random;
        this.enumIndex = 0;

        // We need to use a different random to generate the enum otherwise it's difficult to generate consistent enums
        // between different tables.
        super.init(new ExtendedRandom(new Random(enumSeed)));

        final Set<String> enums = new HashSet<>(nVals);
        while (enums.size() < nVals) {
            enums.add(super.get());
        }

        enumVals = enums.toArray(String[]::new);
    }

    public String get() {
        switch (mode) {
            case Random:
                return enumVals[random.nextInt(0, nVals)];
            case Rotate:
                final int cIdx = enumIndex;
                enumIndex = (enumIndex + 1) % nVals;
                return enumVals[cIdx];
        }

        throw new IllegalStateException("Unsupported mode: " + mode);
    }

    public String[] getEnumVals() {
        return enumVals;
    }
}
