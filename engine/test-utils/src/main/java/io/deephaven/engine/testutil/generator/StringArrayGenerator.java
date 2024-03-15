//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import java.util.Random;

public class StringArrayGenerator extends AbstractGenerator<String[]> {
    private final int minSize;
    private final int maxSize;

    public StringArrayGenerator(int minSize, int maxSize) {
        this.minSize = minSize;
        this.maxSize = maxSize;
    }

    @Override
    public Class<String[]> getType() {
        return String[].class;
    }

    @Override
    String[] nextValue(Random random) {
        final int size = random.nextInt(maxSize - minSize) + minSize;
        final String[] arr = new String[size];
        for (int ii = 0; ii < size; ii++) {
            arr[ii] = Long.toString(random.nextLong(), 'z' - 'a' + 10);
        }
        return arr;
    }
}
