package io.deephaven.engine.testutil.generator;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public abstract class AbstractAdaptableUniqueGenerator<SV, CV> implements TestDataGenerator<CV, CV> {
    final Set<SV> generatedValues = new HashSet<>();
    final Map<Long, CV> currentValues = GeneratorCollectionFactory.makeUnsortedMapForType(getType());

    public Chunk<Values> populateChunk(RowSet toAdd, Random random) {
        if (toAdd.size() == 0)
            return ObjectChunk.getEmptyChunk();

        toAdd.forAllRowKeys(currentValues::remove);

        final List<CV> result = GeneratorCollectionFactory.makeListForType(getType());

        final HashSet<SV> usedValues = new HashSet<>();
        currentValues.values().forEach(v -> usedValues.add(invert(v)));

        for (final RowSet.Iterator iterator = toAdd.iterator(); iterator.hasNext();) {
            final long nextKey = iterator.nextLong();
            final SV value = getNextUniqueValue(usedValues, nextKey, random);
            usedValues.add(value);

            final CV adaptedValue = adapt(value);
            result.add(adaptedValue);
            currentValues.put(nextKey, adaptedValue);
        }

        return makeChunk(result);
    }

    SV getNextUniqueValue(Set<SV> usedValues, long key, Random random) {
        SV candidate;
        int triesLeft = 20;

        do {
            if (triesLeft-- <= 0) {
                throw new RuntimeException("Could not generate unique value!");
            }

            candidate = nextValue(key, random);
        } while (usedValues.contains(candidate));

        generatedValues.add(candidate);

        return candidate;
    }

    @Override
    public void onRemove(RowSet toRemove) {
        toRemove.forAllRowKeys(currentValues::remove);
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
    }

    Set<SV> getGeneratedValues() {
        return Collections.unmodifiableSet(generatedValues);
    }

    abstract Chunk<Values> makeChunk(List<CV> list);

    abstract SV nextValue(long key, Random random);

    public Class<CV> getColumnType() {
        return getType();
    }

    abstract CV adapt(SV value);

    abstract SV invert(CV value);
}
