//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import it.unimi.dsi.fastutil.chars.CharOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2CharOpenHashMap;

import java.util.Random;

/**
 * Generates unique random character values.
 */
public class UniqueCharGenerator implements UniqueTestDataGenerator<Character, Character> {
    final Long2CharOpenHashMap currentValues = new Long2CharOpenHashMap();
    final WritableRowSet currentRowSet = RowSetFactory.empty();

    private final char to, from;
    private final double nullFraction;

    public UniqueCharGenerator(char from, char to) {
        this(from, to, 0.0);
    }

    public UniqueCharGenerator(char from, char to, double nullFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;
    }

    @Override
    public CharChunk<Values> populateChunk(RowSet toAdd, Random random) {
        if (toAdd.isEmpty()) {
            return CharChunk.getEmptyChunk();
        }

        final char[] result = new char[toAdd.intSize()];

        doRemoveValues(toAdd);

        final CharOpenHashSet usedValues = new CharOpenHashSet(currentValues.values());

        int offset = 0;
        for (final RowSet.Iterator iterator = toAdd.iterator(); iterator.hasNext();) {
            final long nextKey = iterator.nextLong();
            final char value = getNextUniqueValue(usedValues, random);
            usedValues.add(value);
            result[offset++] = value;
            currentValues.put(nextKey, value);
        }

        currentRowSet.insert(toAdd);

        return CharChunk.chunkWrap(result);
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


    private char getNextUniqueValue(CharOpenHashSet usedValues, Random random) {
        char candidate;
        int triesLeft = 20;

        do {
            if (triesLeft-- <= 0) {
                throw new RuntimeException("Could not generate unique value!");
            }

            candidate = nextValue(random);
        } while (usedValues.contains(candidate));

        return candidate;
    }

    private char nextValue(Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return QueryConstants.NULL_CHAR;
            }
        }

        return PrimitiveGeneratorFunctions.generateChar(random, from, to);
    }

    @Override
    public boolean hasValues() {
        return currentRowSet.isNonempty();
    }

    @Override
    public Character getRandomValue(final Random random) {
        final int size = currentRowSet.intSize();
        final int randpos = random.nextInt(size);
        final long randKey = currentRowSet.get(randpos);
        return TypeUtils.box(currentValues.get(randKey));
    }

    @Override
    public Class<Character> getType() {
        return Character.class;
    }

    @Override
    public Class<Character> getColumnType() {
        return getType();
    }
}
