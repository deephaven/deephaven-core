//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.*;

public abstract class AbstractUniqueGenerator<T> implements UniqueTestDataGenerator<T, T> {
    final Map<Long, T> currentValues = new Long2ObjectOpenHashMap<>();
    final WritableRowSet currentRowSet = RowSetFactory.empty();

    @Override
    public ObjectChunk<T, Values> populateChunk(RowSet toAdd, Random random) {
        if (toAdd.isEmpty()) {
            return ObjectChunk.getEmptyChunk();
        }

        // noinspection unchecked
        final T[] result = (T[]) new Object[toAdd.intSize()];

        doRemoveValues(toAdd);

        final HashSet<T> usedValues = new HashSet<>(currentValues.values());
        int offset = 0;
        for (final RowSet.Iterator iterator = toAdd.iterator(); iterator.hasNext();) {
            final long nextKey = iterator.nextLong();
            final T value = getNextUniqueValue(usedValues, nextKey, random);
            usedValues.add(value);
            result[offset++] = value;
            currentValues.put(nextKey, value);
        }
        currentRowSet.insert(toAdd);

        return ObjectChunk.chunkWrap(result);
    }

    private void doRemoveValues(RowSet toAdd) {
        toAdd.forAllRowKeys(currentValues::remove);
        currentRowSet.remove(toAdd);
    }

    private void checkUnique() {
        final Map<T, Long> rowForKey = new HashMap<>();
        currentValues.forEach((k, v) -> {
            final Long ok = rowForKey.put(v, k);
            if (ok != null) {
                throw new IllegalStateException("Duplicate value: " + v + ", ok=" + ok + ", k=" + k);
            }
        });
    }

    @Override
    public void onRemove(RowSet toRemove) {
        doRemoveValues(toRemove);
    }

    @Override
    public void shift(long start, long end, long delta) {
        if (delta < 0) {
            for (long kk = start; kk <= end; ++kk) {
                if (currentValues.containsKey(kk)) {
                    currentValues.put(kk + delta, currentValues.remove(kk));
                }
            }
        } else {
            for (long kk = end; kk >= start; --kk) {
                if (currentValues.containsKey(kk)) {
                    currentValues.put(kk + delta, currentValues.remove(kk));
                }
            }
        }
        try (final RowSet toShift = currentRowSet.subSetByKeyRange(start, end)) {
            currentRowSet.removeRange(start, end);
            currentRowSet.insertWithShift(delta, toShift);
        }
    }

    private T getNextUniqueValue(Set<T> usedValues, long key, Random random) {
        T candidate;
        int triesLeft = 20;

        do {
            if (triesLeft-- <= 0) {
                throw new RuntimeException("Could not generate unique value!");
            }

            candidate = nextValue(key, random);
        } while (usedValues.contains(candidate));

        return candidate;
    }

    @Override
    public boolean hasValues() {
        return currentRowSet.isNonempty();
    }

    @Override
    public T getRandomValue(final Random random) {
        final int size = currentRowSet.intSize();
        final int randpos = random.nextInt(size);
        final long randKey = currentRowSet.get(randpos);
        return currentValues.get(randKey);
    }

    /**
     * Get a random value to add to this set, the AbstractUniqueGenerator will check for uniqueness, the implementing
     * class can simply return any random value. If the value is not unique after a sufficient number of tries, then
     * populateChunk throws an exception.
     *
     * @param key the RowSet key to generate a value for
     * @param random the random number generator to use
     * @return a random value of type T
     */
    abstract T nextValue(long key, Random random);

    @Override
    public Class<T> getColumnType() {
        return getType();
    }
}
