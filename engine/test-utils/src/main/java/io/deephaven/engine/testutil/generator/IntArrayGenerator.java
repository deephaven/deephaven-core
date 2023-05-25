package io.deephaven.engine.testutil.generator;

import java.util.Random;

public class IntArrayGenerator extends AbstractGenerator<int[]> {
    private final int min;
    private final int max;
    private final int minSize;
    private final int maxSize;

    public IntArrayGenerator(int min, int max, int minSize, int maxSize) {
        this.min = min;
        this.max = max;
        this.minSize = minSize;
        this.maxSize = maxSize;
    }

    @Override
    public Class<int[]> getType() {
        return int[].class;
    }

    @Override
    int[] nextValue(Random random) {
        final int size = random.nextInt(maxSize - minSize) + minSize;
        final int[] arr = new int[size];
        for (int ii = 0; ii < size; ii++) {
            arr[ii] = random.nextInt(max - min) + min;
        }
        return arr;
    }
}
