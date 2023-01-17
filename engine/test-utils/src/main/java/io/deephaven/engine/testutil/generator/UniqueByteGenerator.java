/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UniqueCharGenerator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.testutil.generator;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import it.unimi.dsi.fastutil.bytes.ByteOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;

import java.util.Random;

/**
 * Generates unique random byteacter values.
 */
public class UniqueByteGenerator implements UniqueTestDataGenerator<Byte, Byte> {
    final Long2ByteOpenHashMap currentValues = new Long2ByteOpenHashMap();
    final WritableRowSet currentRowSet = RowSetFactory.empty();

    private final byte to, from;
    private final double nullFraction;

    public UniqueByteGenerator(byte from, byte to) {
        this(from, to, 0.0);
    }

    public UniqueByteGenerator(byte from, byte to, double nullFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;
    }

    @Override
    public ByteChunk<Values> populateChunk(RowSet toAdd, Random random) {
        if (toAdd.isEmpty()) {
            return ByteChunk.getEmptyChunk();
        }

        final byte[] result = new byte[toAdd.intSize()];

        doRemoveValues(toAdd);

        final ByteOpenHashSet usedValues = new ByteOpenHashSet(currentValues.values());

        int offset = 0;
        for (final RowSet.Iterator iterator = toAdd.iterator(); iterator.hasNext();) {
            final long nextKey = iterator.nextLong();
            final byte value = getNextUniqueValue(usedValues, random);
            usedValues.add(value);
            result[offset++] = value;
            currentValues.put(nextKey, value);
        }

        currentRowSet.insert(toAdd);

        return ByteChunk.chunkWrap(result);
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


    private byte getNextUniqueValue(ByteOpenHashSet usedValues, Random random) {
        byte candidate;
        int triesLeft = 20;

        do {
            if (triesLeft-- <= 0) {
                throw new RuntimeException("Could not generate unique value!");
            }

            candidate = nextValue(random);
        } while (usedValues.contains(candidate));

        return candidate;
    }

    private byte nextValue(Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return QueryConstants.NULL_BYTE;
            }
        }

        return PrimitiveGeneratorFunctions.generateByte(random, from, to);
    }

    @Override
    public boolean hasValues() {
        return currentRowSet.isNonempty();
    }

    @Override
    public Byte getRandomValue(final Random random) {
        final int size = currentRowSet.intSize();
        final int randpos = random.nextInt(size);
        final long randKey = currentRowSet.get(randpos);
        return TypeUtils.box(currentValues.get(randKey));
    }

    @Override
    public Class<Byte> getType() {
        return Byte.class;
    }

    @Override
    public Class<Byte> getColumnType() {
        return getType();
    }
}
