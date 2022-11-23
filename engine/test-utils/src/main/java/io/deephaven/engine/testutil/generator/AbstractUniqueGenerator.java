package io.deephaven.engine.testutil.generator;

import io.deephaven.engine.rowset.RowSet;

import java.util.*;

public abstract class AbstractUniqueGenerator<T> implements Generator<T, T> {
    final Set<T> generatedValues = new HashSet<>();

    public TreeMap<Long, T> populateMap(TreeMap<Long, T> values, RowSet toAdd, Random random) {
        final TreeMap<Long, T> result = new TreeMap<>();
        if (toAdd.size() == 0)
            return result;

        for (final RowSet.Iterator it = toAdd.iterator(); it.hasNext(); ) {
            values.remove(it.nextLong());
        }

        final HashSet<T> usedValues = new HashSet<>(values.values());

        for (final RowSet.Iterator iterator = toAdd.iterator(); iterator.hasNext(); ) {
            final long nextKey = iterator.nextLong();
            final T value = getNextUniqueValue(usedValues, values, nextKey, random);
            usedValues.add(value);
            result.put(nextKey, value);
            values.put(nextKey, value);
        }

        return result;
    }

    // TODO: update the callers so that as we remove rows, we also remove them from the usedValues set;
    // otherwise we can exhaust the set more easily than we should during an incremental update.
    T getNextUniqueValue(Set<T> usedValues, TreeMap<Long, T> values, long key, Random random) {
        T candidate;
        int triesLeft = 20;

        do {
            if (triesLeft-- <= 0) {
                throw new RuntimeException("Could not generate unique value!");
            }

            candidate = nextValue(values, key, random);
        } while (usedValues.contains(candidate));

        generatedValues.add(candidate);

        return candidate;
    }

    Set<T> getGeneratedValues() {
        return Collections.unmodifiableSet(generatedValues);
    }

    abstract T nextValue(TreeMap<Long, T> values, long key, Random random);

    public Class<T> getColumnType() {
        return getType();
    }
}
