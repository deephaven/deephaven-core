//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;

import java.util.List;
import java.util.Random;

public class UniqueIntArrayGenerator extends AbstractAdaptableUniqueGenerator<IntArrayList, int[]> {
    private final int minSize;
    private final int maxSize;
    private final int min;
    private final int max;

    public UniqueIntArrayGenerator(int min, int max, int minSize, int maxSize) {
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
    ObjectChunk<int[], Values> makeChunk(List<int[]> list) {
        return WritableObjectChunk.writableChunkWrap(list.toArray(new int[list.size()][]));
    }

    @Override
    IntArrayList nextValue(long key, Random random) {
        final int size = random.nextInt(maxSize - minSize) + minSize;
        final IntArrayList list = new IntArrayList(size);
        for (int ii = 0; ii < size; ii++) {
            list.add(random.nextInt(max - min) + min);
        }
        return list;
    }

    @Override
    int[] adapt(IntArrayList value) {
        return value.toIntArray();
    }

    @Override
    IntArrayList invert(int[] value) {
        return new IntArrayList(value);
    }
}
