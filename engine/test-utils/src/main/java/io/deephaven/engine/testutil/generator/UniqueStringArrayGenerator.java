//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class UniqueStringArrayGenerator extends AbstractAdaptableUniqueGenerator<List<String>, String[]> {
    private final int minSize;
    private final int maxSize;

    public UniqueStringArrayGenerator(int minSize, int maxSize) {
        this.minSize = minSize;
        this.maxSize = maxSize;
    }

    @Override
    public Class<String[]> getType() {
        return String[].class;
    }

    @Override
    ObjectChunk<String[], Values> makeChunk(List<String[]> list) {
        return WritableObjectChunk.writableChunkWrap(list.toArray(new String[list.size()][]));
    }

    @Override
    List<String> nextValue(long key, Random random) {
        final int size = random.nextInt(maxSize - minSize) + minSize;
        final String[] arr = new String[size];
        for (int ii = 0; ii < size; ii++) {
            arr[ii] = Long.toString(random.nextLong(), 'z' - 'a' + 10);
        }
        return Arrays.asList(arr);
    }

    @Override
    String[] adapt(List<String> value) {
        return value.toArray(String[]::new);
    }

    @Override
    List<String> invert(String[] value) {
        return Arrays.asList(value);
    }
}
