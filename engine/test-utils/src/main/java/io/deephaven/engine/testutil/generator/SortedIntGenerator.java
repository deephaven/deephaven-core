//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit SortedCharGenerator and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.testutil.generator;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.Arrays;
import java.util.Random;

/**
 * Generates a sorted column of intacters.
 *
 * Some query operations require sorted values as input, the sorted generators creates columns that are in sorted order.
 *
 * Internally, an RowSet of current values plus a hashmap of those values is maintained. When new rows are added, the
 * values are randomly selected between the prior and next point.
 */
public class SortedIntGenerator implements TestDataGenerator<Integer, Integer> {
    final Long2IntOpenHashMap currentValues = new Long2IntOpenHashMap();
    final WritableRowSet currentRowSet = RowSetFactory.empty();

    private final int minValue;
    private final int maxValue;

    public SortedIntGenerator(int minValue, int maxValue) {
        if (maxValue == Integer.MAX_VALUE) {
            // Because the "range + 1" code below makes it wrap.
            throw new UnsupportedOperationException("Integer.MAX_VALUE not supported");
        }
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    int makeValue(int floor, int ceiling, Random random) {
        return PrimitiveGeneratorFunctions.generateInt(random, floor, ceiling);
    }

    @Override
    public Class<Integer> getType() {
        return Integer.class;
    }

    @Override
    public Class<Integer> getColumnType() {
        return Integer.class;
    }

    @Override
    public IntChunk<Values> populateChunk(RowSet toAdd, Random random) {
        if (toAdd.isEmpty()) {
            return IntChunk.getEmptyChunk();
        }

        toAdd.forAllRowKeys(currentValues::remove);
        currentRowSet.remove(toAdd);

        final RowSet.Iterator iterator = toAdd.iterator();
        long firstKey = iterator.nextLong();

        int currentFloor = getFloor(firstKey);
        long ceilingKey = getCeilingKey(firstKey);
        int currentCeiling = getCeilingValue(ceilingKey);

        final int[] resultArray = new int[toAdd.intSize()];
        int offset = 0;
        int count = 1;

        if (ceilingKey == Long.MAX_VALUE) {
            count = toAdd.intSize();
        } else {
            while (iterator.hasNext()) {
                final long nextKey = iterator.nextLong();
                if (nextKey >= ceilingKey) {
                    generateValues(count, currentFloor, currentCeiling, resultArray, offset, random);
                    offset += count;
                    count = 0;
                    firstKey = nextKey;
                    currentFloor = getFloor(firstKey);
                    ceilingKey = getCeilingKey(firstKey);
                    currentCeiling = getCeilingValue(ceilingKey);
                }
                count++;
            }
        }

        generateValues(count, currentFloor, currentCeiling, resultArray, offset, random);

        currentRowSet.insert(toAdd);
        final MutableInt offset2 = new MutableInt(0);
        toAdd.forAllRowKeys(idx -> currentValues.put(idx, resultArray[offset2.getAndIncrement()]));

        return IntChunk.chunkWrap(resultArray);
    }

    private int getCeilingValue(long ceilingKey) {
        return ceilingKey == Long.MAX_VALUE ? maxValue : currentValues.get(ceilingKey);
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

    private int getFloor(long firstKey) {
        if (currentRowSet.isEmpty() || firstKey < currentRowSet.firstRowKey()) {
            return minValue;
        }
        final long position = currentRowSet.find(firstKey);
        if (position >= 0) {
            return currentValues.get(firstKey);
        }
        final long floorKey = currentRowSet.get(-position - 2);
        Assert.assertion(currentValues.containsKey(floorKey), "currentValues.containsKey(floorKey)");
        return currentValues.get(floorKey);
    }

    private void generateValues(int count, int floor, int ceiling, int[] result, int offset, Random random) {
        final int[] values = new int[count];
        for (int ii = 0; ii < count; ++ii) {
            values[ii] = makeValue(floor, ceiling, random);
        }
        Arrays.sort(values);
        if (offset > 0) {
            Assert.geq(values[0], "values[0]", result[offset - 1], "result[offset - 1]");
        }
        System.arraycopy(values, 0, result, offset, values.length);
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

    private void checkSorted() {
        Assert.eq(currentRowSet.size(), "currentRowSet.size()", currentValues.size(), "currentValues.size()");
        // region check sorted mutable
        final MutableLong lastValue = new MutableLong(Long.MIN_VALUE);
        // endregion check sorted mutable
        currentRowSet.forAllRowKeys(idx -> {
            final int value = currentValues.get(idx);
            // region check sorted assertion
            Assert.leq(lastValue.longValue(), "lastValue", value, "value");
            // endregion check sorted assertion
            lastValue.setValue(value);
        });
    }
}
