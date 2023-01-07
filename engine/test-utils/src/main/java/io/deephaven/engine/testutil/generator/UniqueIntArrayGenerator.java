package io.deephaven.engine.testutil.generator;

import gnu.trove.list.array.TIntArrayList;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;

import java.util.List;
import java.util.Random;

public class UniqueIntArrayGenerator extends AbstractAdaptableUniqueGenerator<TIntArrayList, int[]> {
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
    TIntArrayList nextValue(long key, Random random) {
        final int size = random.nextInt(maxSize - minSize) + minSize;
        final TIntArrayList list = new TIntArrayList(size);
        for (int ii = 0; ii < size; ii++) {
            list.add(random.nextInt(max - min) + min);
        }
        return list;
    }

    @Override
    int[] adapt(TIntArrayList value) {
        return value.toArray();
    }

    @Override
    TIntArrayList invert(int[] value) {
        return new TIntArrayList(value);
    }
}
