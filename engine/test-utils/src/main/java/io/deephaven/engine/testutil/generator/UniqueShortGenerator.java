/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UniqueCharGenerator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.testutil.generator;

import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import it.unimi.dsi.fastutil.shorts.ShortOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2ShortOpenHashMap;

import java.util.Random;

/**
 * Generates unique random shortacter values.
 */
public class UniqueShortGenerator implements UniqueTestDataGenerator<Short, Short> {
    final Long2ShortOpenHashMap currentValues = new Long2ShortOpenHashMap();
    final WritableRowSet currentRowSet = RowSetFactory.empty();

    private final short to, from;
    private final double nullFraction;

    public UniqueShortGenerator(short from, short to) {
        this(from, to, 0.0);
    }

    public UniqueShortGenerator(short from, short to, double nullFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;
    }

    @Override
    public ShortChunk<Values> populateChunk(RowSet toAdd, Random random) {
        if (toAdd.isEmpty()) {
            return ShortChunk.getEmptyChunk();
        }

        final short[] result = new short[toAdd.intSize()];

        doRemoveValues(toAdd);

        final ShortOpenHashSet usedValues = new ShortOpenHashSet(currentValues.values());

        int offset = 0;
        for (final RowSet.Iterator iterator = toAdd.iterator(); iterator.hasNext();) {
            final long nextKey = iterator.nextLong();
            final short value = getNextUniqueValue(usedValues, random);
            usedValues.add(value);
            result[offset++] = value;
            currentValues.put(nextKey, value);
        }

        currentRowSet.insert(toAdd);

        return ShortChunk.chunkWrap(result);
    }

    private void doRemoveValues(RowSet toAdd) {
        toAdd.forAllRowKeys(currentValues::remove);
        currentRowSet.remove(toAdd);
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


    private short getNextUniqueValue(ShortOpenHashSet usedValues, Random random) {
        short candidate;
        int triesLeft = 20;

        do {
            if (triesLeft-- <= 0) {
                throw new RuntimeException("Could not generate unique value!");
            }

            candidate = nextValue(random);
        } while (usedValues.contains(candidate));

        return candidate;
    }

    private short nextValue(Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return QueryConstants.NULL_SHORT;
            }
        }

        return PrimitiveGeneratorFunctions.generateShort(random, from, to);
    }

    @Override
    public boolean hasValues() {
        return currentRowSet.isNonempty();
    }

    @Override
    public Short getRandomValue(final Random random) {
        final int size = currentRowSet.intSize();
        final int randpos = random.nextInt(size);
        final long randKey = currentRowSet.get(randpos);
        return TypeUtils.box(currentValues.get(randKey));
    }

    @Override
    public Class<Short> getType() {
        return Short.class;
    }

    @Override
    public Class<Short> getColumnType() {
        return getType();
    }
}
