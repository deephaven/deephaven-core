package io.deephaven.engine.testutil.generator;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public abstract class AbstractSortedGenerator<T extends Comparable<? super T>> implements Generator<T, T> {
    public TreeMap<Long, T> populateMap(TreeMap<Long, T> values, RowSet toAdd, Random random) {
        final TreeMap<Long, T> result = new TreeMap<>();
        if (toAdd.size() == 0)
            return result;

        for (final RowSet.Iterator it = toAdd.iterator(); it.hasNext(); ) {
            values.remove(it.nextLong());
        }

        final RowSet.Iterator iterator = toAdd.iterator();
        long firstKey = iterator.nextLong();
        long lastKey = firstKey;
        final Map.Entry<Long, T> firstFloorEntry = values.floorEntry(firstKey);
        T currentFloor = firstFloorEntry == null ? minValue() : firstFloorEntry.getValue();

        final Map.Entry<Long, T> firstCeilingEntry = values.ceilingEntry(firstKey);
        T currentCeiling = firstCeilingEntry == null ? maxValue() : firstCeilingEntry.getValue();

        while (iterator.hasNext()) {
            final long nextKey = iterator.nextLong();
            final Map.Entry<Long, T> ceilingEntry = values.ceilingEntry(nextKey);
            final Map.Entry<Long, T> floorEntry = values.floorEntry(nextKey);

            final T floor = floorEntry == null ? minValue() : floorEntry.getValue();
            final T ceiling = ceilingEntry == null ? maxValue() : ceilingEntry.getValue();

            if (!ceiling.equals(currentCeiling) || !floor.equals(currentFloor)) {
                // we're past the end of the last run so we need to generate the values for the map
                generateValues(toAdd.intersect(RowSetFactory.fromRange(firstKey, lastKey)),
                        currentFloor,
                        currentCeiling, result, random);
                firstKey = nextKey;
                currentFloor = floor;
                currentCeiling = ceiling;
            }
            lastKey = nextKey;
        }

        generateValues(toAdd.intersect(RowSetFactory.fromRange(firstKey, lastKey)),
                currentFloor,
                currentCeiling, result, random);

        values.putAll(result);

        checkSorted(values);

        return result;
    }

    abstract T maxValue();

    abstract T minValue();

    private void checkSorted(TreeMap<Long, T> values) {
        T lastValue = minValue();
        for (Map.Entry<Long, T> valueEntry : values.entrySet()) {
            final T value = valueEntry.getValue();
            Assert.assertion(value.compareTo(lastValue) >= 0, "value >= lastValue", value, "value", lastValue,
                    "lastValue", valueEntry.getKey(), "valueEntry.getKey");
            lastValue = value;
        }
    }

    private void generateValues(RowSet toadd, T floor, T ceiling, TreeMap<Long, T> result, Random random) {
        final int count = (int) toadd.size();
        // noinspection unchecked
        final T[] values = (T[]) Array.newInstance(getType(), count);
        for (int ii = 0; ii < count; ++ii) {
            values[ii] = makeValue(floor, ceiling, random);
        }
        Arrays.sort(values);
        int ii = 0;
        for (final RowSet.Iterator it = toadd.iterator(); it.hasNext(); ) {
            result.put(it.nextLong(), values[ii++]);
        }
    }

    abstract T makeValue(T floor, T ceiling, Random random);

    public Class<T> getColumnType() {
        return getType();
    }
}
