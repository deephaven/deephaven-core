package io.deephaven.engine.testutil.generator;

import io.deephaven.engine.rowset.RowSet;

import java.util.Random;
import java.util.TreeMap;

public abstract class AbstractReinterpretedGenerator<T, U> implements Generator<T, U> {
    @Override
    public TreeMap<Long, U> populateMap(final TreeMap<Long, U> values, final RowSet toAdd, final Random random) {
        final TreeMap<Long, U> result = new TreeMap<>();
        toAdd.forAllRowKeys((final long nextKey) -> {
            final U value = nextValue(values, nextKey, random);
            result.put(nextKey, value);
            values.put(nextKey, value);
        });
        return result;
    }

    abstract U nextValue(TreeMap<Long, U> values, long key, Random random);
}
