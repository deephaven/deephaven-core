/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UniqueCharGenerator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.testutil.generator;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import java.util.Random;

/**
 * Generates unique random longacter values.
 */
public class UniqueLongGenerator implements UniqueTestDataGenerator<Long, Long> {
    final Long2LongOpenHashMap currentValues = new Long2LongOpenHashMap();
    final WritableRowSet currentRowSet = RowSetFactory.empty();

    private final long to, from;
    private final double nullFraction;

    public UniqueLongGenerator(long from, long to) {
        this(from, to, 0.0);
    }

    public UniqueLongGenerator(long from, long to, double nullFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;
    }

    @Override
    public LongChunk<Values> populateChunk(RowSet toAdd, Random random) {
        if (toAdd.isEmpty()) {
            return LongChunk.getEmptyChunk();
        }

        final long[] result = new long[toAdd.intSize()];

        doRemoveValues(toAdd);

        final LongOpenHashSet usedValues = new LongOpenHashSet(currentValues.values());

        int offset = 0;
        for (final RowSet.Iterator iterator = toAdd.iterator(); iterator.hasNext();) {
            final long nextKey = iterator.nextLong();
            final long value = getNextUniqueValue(usedValues, random);
            usedValues.add(value);
            result[offset++] = value;
            currentValues.put(nextKey, value);
        }

        currentRowSet.insert(toAdd);

        return LongChunk.chunkWrap(result);
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


    private long getNextUniqueValue(LongOpenHashSet usedValues, Random random) {
        long candidate;
        int triesLeft = 20;

        do {
            if (triesLeft-- <= 0) {
                throw new RuntimeException("Could not generate unique value!");
            }

            candidate = nextValue(random);
        } while (usedValues.contains(candidate));

        return candidate;
    }

    private long nextValue(Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return QueryConstants.NULL_LONG;
            }
        }

        return PrimitiveGeneratorFunctions.generateLong(random, from, to);
    }

    @Override
    public boolean hasValues() {
        return currentRowSet.isNonempty();
    }

    @Override
    public Long getRandomValue(final Random random) {
        final int size = currentRowSet.intSize();
        final int randpos = random.nextInt(size);
        final long randKey = currentRowSet.get(randpos);
        return TypeUtils.box(currentValues.get(randKey));
    }

    @Override
    public Class<Long> getType() {
        return Long.class;
    }

    @Override
    public Class<Long> getColumnType() {
        return getType();
    }
}
