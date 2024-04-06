//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.apache.commons.lang3.mutable.MutableObject;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;

/**
 * Generates a sorted column of objects.
 *
 * Some query operations require sorted values as input, the sorted generators creates columns that are in sorted order.
 *
 * Internally, an Index of current values plus a hashmap of those values is maintained. When new rows are added, the
 * values are randomly selected between the prior and next point.
 */
public abstract class AbstractSortedGenerator<T extends Comparable<? super T>> implements TestDataGenerator<T, T> {
    final Map<Long, T> currentValues = GeneratorCollectionFactory.makeUnsortedMapForType(getType());
    final WritableRowSet currentRowSet = RowSetFactory.empty();

    @Override
    public Chunk<Values> populateChunk(RowSet toAdd, Random random) {
        if (toAdd.size() == 0) {
            return ObjectChunk.getEmptyChunk();
        }

        final SortedMap<Long, T> result = GeneratorCollectionFactory.makeSortedMapForType(getType());

        final RowSet orig = currentRowSet.copy();
        toAdd.forAllRowKeys(currentValues::remove);
        currentRowSet.remove(toAdd);

        final RowSet.Iterator iterator = toAdd.iterator();
        long firstKey = iterator.nextLong();
        long lastKey = firstKey;

        T currentFloor = getFloor(firstKey);
        long ceilingKey = getCeilingKey(firstKey);
        T currentCeiling = getCeilingValue(ceilingKey);

        while (iterator.hasNext()) {
            final long nextKey = iterator.nextLong();
            if (nextKey < ceilingKey) {
                lastKey = nextKey;
            } else {
                generateValues(toAdd.subSetByKeyRange(firstKey, lastKey), currentFloor, currentCeiling, result, random);
                firstKey = nextKey;
                lastKey = firstKey;
                currentFloor = getFloor(firstKey);
                ceilingKey = getCeilingKey(firstKey);
                currentCeiling = getCeilingValue(ceilingKey);
            }
        }

        generateValues(toAdd.subSetByKeyRange(firstKey, lastKey), currentFloor, currentCeiling, result, random);

        currentRowSet.insert(toAdd);
        currentValues.putAll(result);

        // noinspection unchecked
        T[] resultArray = result.values().toArray((T[]) Array.newInstance(getType(), result.size()));
        return ObjectChunk.chunkWrap(resultArray);
    }

    private T getCeilingValue(long ceilingKey) {
        return ceilingKey == Long.MAX_VALUE ? maxValue() : currentValues.get(ceilingKey);
    }

    private long getCeilingKey(long firstKey) {
        if (currentRowSet.isEmpty() || firstKey > currentRowSet.lastRowKey()) {
            return Long.MAX_VALUE;
        }
        final long position = currentRowSet.find(firstKey);
        if (position >= 0) {
            return firstKey;
        }
        return currentRowSet.get(-position - 1);
    }

    private T getFloor(long firstKey) {
        if (currentRowSet.isEmpty() || firstKey < currentRowSet.firstRowKey()) {
            return minValue();
        }
        final long position = currentRowSet.find(firstKey);
        if (position >= 0) {
            return currentValues.get(firstKey);
        }
        final long floorKey = currentRowSet.get(-position - 2);
        Assert.assertion(currentValues.containsKey(floorKey), "currentValues.containsKey(floorKey)");
        return currentValues.get(floorKey);
    }

    /**
     * The maximum value that should be generated.
     *
     * For generating values after the last value, we must know the highest value to generate.
     *
     * @return the maximum value that should be generated.
     */
    abstract T maxValue();

    /**
     * The minimum value that should be generated.
     *
     * For generating values before the first value, we must know the highest value to generate.
     *
     * @return the minimum value that should be generated.
     */
    abstract T minValue();

    private void checkSorted() {
        Assert.eq(currentRowSet.size(), "currentRowSet.size()", currentValues.size(), "currentValues.size()");
        final MutableObject<T> lastValue = new MutableObject<>(minValue());
        currentRowSet.forAllRowKeys(idx -> {
            final T value = currentValues.get(idx);
            Assert.assertion(value.compareTo(lastValue.getValue()) >= 0, "value >= lastValue", value, "value",
                    lastValue.getValue(), "lastValue", value, "value");
            lastValue.setValue(value);
        });
    }

    private void generateValues(RowSet toadd, T floor, T ceiling, SortedMap<Long, T> result, Random random) {
        final int count = (int) toadd.size();
        // noinspection unchecked
        final T[] values = (T[]) Array.newInstance(getType(), count);
        for (int ii = 0; ii < count; ++ii) {
            values[ii] = makeValue(floor, ceiling, random);
        }
        Arrays.sort(values);
        int ii = 0;
        for (final RowSet.Iterator it = toadd.iterator(); it.hasNext();) {
            result.put(it.nextLong(), values[ii++]);
        }
    }

    abstract T makeValue(T floor, T ceiling, Random random);

    public Class<T> getColumnType() {
        return getType();
    }

    @Override
    public void onRemove(RowSet toRemove) {
        toRemove.forAllRowKeys(currentValues::remove);
        currentRowSet.remove(toRemove);
    }

    @Override
    public void shift(long start, long end, long delta) {
        try (final RowSet shifted = currentRowSet.subSetByKeyRange(start, end)) {
            if (delta < 0) {
                shifted.forAllRowKeys(kk -> currentValues.put(kk + delta, currentValues.remove(kk)));
            } else {
                shifted.reverseIterator()
                        .forEachRemaining((long kk) -> currentValues.put(kk + delta, currentValues.remove(kk)));
            }
            currentRowSet.removeRange(start, end);
            currentRowSet.insertWithShift(delta, shifted);
        }
    }
}
