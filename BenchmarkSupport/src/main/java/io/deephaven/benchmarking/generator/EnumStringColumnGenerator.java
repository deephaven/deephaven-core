package io.deephaven.benchmarking.generator;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.benchmarking.generator.random.ExtendedRandom;
import io.deephaven.benchmarking.generator.random.NormalExtendedRandom;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * A {@link ColumnGenerator<String>} that sources values from a fixed set of values, either randomly, or in rotation.
 */
public class EnumStringColumnGenerator extends AbstractStringColumnGenerator {
    public enum Mode {
        Random, Rotate,
    }

    private ExtendedRandom random;

    private String[] enumVals;
    private int enumIndex;

    private final int nVals;
    private final Mode mode;
    private final long enumSeed;

    public EnumStringColumnGenerator(String name, int nVals, int minLength, int maxLength, long enumSeed, Mode mode) {
        super(name, minLength, maxLength);

        this.enumSeed = enumSeed;
        this.mode = mode;
        this.nVals = nVals;
    }

    @Override
    public void init(ExtendedRandom random) {
        this.random = random;
        this.enumIndex = 0;

        final Set<String> enums = new HashSet<>(nVals);

        // We need to use a different random to generate the enum otherwise it's difficult to generate consistent enums
        // between different tables.
        final StringGenerator sg =
                new StringGenerator(getMinLength(), getMaxLength(), new NormalExtendedRandom(new Random(enumSeed)));
        while (enums.size() < nVals) {
            enums.add(sg.get());
        }

        enumVals = enums.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
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
