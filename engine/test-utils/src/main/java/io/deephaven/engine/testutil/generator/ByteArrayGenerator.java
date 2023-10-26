/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.testutil.generator;

import java.util.Random;

public class ByteArrayGenerator extends AbstractGenerator<byte[]> {
    private final byte min;
    private final byte max;
    private final int minSize;
    private final int maxSize;

    public ByteArrayGenerator(byte min, byte max, int minSize, int maxSize) {
        this.min = min;
        this.max = max;
        this.minSize = minSize;
        this.maxSize = maxSize;
    }

    @Override
    public Class<byte[]> getType() {
        return byte[].class;
    }

    @Override
    byte[] nextValue(Random random) {
        final int size = random.nextInt(maxSize - minSize) + minSize;
        final byte[] arr = new byte[size];
        for (int ii = 0; ii < size; ii++) {
            arr[ii] = (byte) (random.nextInt(max - min) + min);
        }
        return arr;
    }
}
