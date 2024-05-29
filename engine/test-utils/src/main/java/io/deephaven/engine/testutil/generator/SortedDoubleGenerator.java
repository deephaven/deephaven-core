//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit SortedCharGenerator and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.testutil.generator;

import org.apache.commons.lang3.mutable.MutableDouble;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.Arrays;
import java.util.Random;

/**
 * Generates a sorted column of doubleacters.
 *
 * Some query operations require sorted values as input, the sorted generators creates columns that are in sorted order.
 *
 * Internally, an RowSet of current values plus a hashmap of those values is maintained. When new rows are added, the
 * values are randomly selected between the prior and next point.
 */
public class SortedDoubleGenerator implements TestDataGenerator<Double, Double> {
    final Long2DoubleOpenHashMap currentValues = new Long2DoubleOpenHashMap();
    final WritableRowSet currentRowSet = RowSetFactory.empty();

    private final double minValue;
    private final double maxValue;

    public SortedDoubleGenerator(double minValue, double maxValue) {
        if (maxValue == Double.MAX_VALUE) {
            // Because the "range + 1" code below makes it wrap.
            throw new UnsupportedOperationException("Double.MAX_VALUE not supported");
        }
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    double makeValue(double floor, double ceiling, Random random) {
        return PrimitiveGeneratorFunctions.generateDouble(random, floor, ceiling);
    }

    @Override
    public Class<Double> getType() {
        return Double.class;
    }

    @Override
    public Class<Double> getColumnType() {
        return Double.class;
    }

    @Override
    public DoubleChunk<Values> populateChunk(RowSet toAdd, Random random) {
        if (toAdd.isEmpty()) {
            return DoubleChunk.getEmptyChunk();
        }

        toAdd.forAllRowKeys(currentValues::remove);
        currentRowSet.remove(toAdd);

        final RowSet.Iterator iterator = toAdd.iterator();
        long firstKey = iterator.nextLong();

        double currentFloor = getFloor(firstKey);
        long ceilingKey = getCeilingKey(firstKey);
        double currentCeiling = getCeilingValue(ceilingKey);

        final double[] resultArray = new double[toAdd.intSize()];
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

        return DoubleChunk.chunkWrap(resultArray);
    }

    private double getCeilingValue(long ceilingKey) {
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

    private double getFloor(long firstKey) {
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

    private void generateValues(int count, double floor, double ceiling, double[] result, int offset, Random random) {
        final double[] values = new double[count];
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
        final MutableDouble lastValue = new MutableDouble(-Double.MAX_VALUE);
        // endregion check sorted mutable
        currentRowSet.forAllRowKeys(idx -> {
            final double value = currentValues.get(idx);
            // region check sorted assertion
            Assert.leq(lastValue.doubleValue(), "lastValue", value, "value");

            // endregion check sorted assertion
            lastValue.setValue(value);
        });
    }
}
