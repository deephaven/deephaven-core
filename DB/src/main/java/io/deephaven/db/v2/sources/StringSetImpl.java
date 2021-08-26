/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.base.MathUtil;
import io.deephaven.hash.KeyedLongObjectHash;
import io.deephaven.hash.KeyedLongObjectKey;
import io.deephaven.datastructures.util.HashCodeUtil;
import io.deephaven.db.tables.StringSetWrapper;
import io.deephaven.db.tables.libs.StringSet;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.ToIntFunction;

/**
 * The StringSet implementation used by column sources.
 */
public class StringSetImpl implements StringSet, Serializable {


    public final static KeyedLongObjectKey<StringSetImpl> CACHE_KEY =
        new KeyedLongObjectKey.Strict<StringSetImpl>() {

            @Override
            public long getLongKey(@NotNull final StringSetImpl stringSet) {
                return stringSet.valueBitSet;
            }

            @Override
            public int hashLongKey(final long valueBitSet) {
                return HashCodeUtil.toHashCode(valueBitSet);
            }

            @Override
            public boolean equalLongKey(final long valueBitSet,
                @NotNull final StringSetImpl stringSet) {
                return valueBitSet == stringSet.valueBitSet;
            }
        };

    public final static KeyedLongObjectHash.ValueFactoryT<StringSetImpl, ReverseLookupColumnSource<String, ?>> VALUE_FACTORY =
        new KeyedLongObjectHash.ValueFactoryT.Strict<StringSetImpl, ReverseLookupColumnSource<String, ?>>() {

            @Override
            public StringSetImpl newValue(final long valueBitSet,
                @NotNull final ReverseLookupColumnSource<String, ?> columnSource) {
                return new StringSetImpl(columnSource, valueBitSet);
            }
        };

    private final ReversibleLookup<String> reversibleLookup;
    private final long valueBitSet;

    private int bitCount = Integer.MIN_VALUE; // [0, 64]
    private int highestOneBitIndex = Integer.MIN_VALUE; // [-1, 63]

    private transient StringSetWrapper replacement;

    // TODO: Consider caching values and sorted values.
    // TODO: If we start caching values, consider changing getEncoding to use values().
    // TODO: On this note, look at LongBitmapIndexedImmutableSetFactory.

    public StringSetImpl(@NotNull final ReversibleLookup<String> reversibleLookup,
        final long valueBitSet) {
        this.reversibleLookup = reversibleLookup;
        this.valueBitSet = valueBitSet;
    }

    private int getHighestOneBitIndex() {
        return highestOneBitIndex == Integer.MIN_VALUE
            ? (highestOneBitIndex = MathUtil.floorLog2(valueBitSet))
            : highestOneBitIndex;
    }

    private int getBitCount() {
        return bitCount == Integer.MIN_VALUE ? (bitCount = Long.bitCount(valueBitSet)) : bitCount;
    }

    private boolean isBitOn(final int bitIndex) {
        return (valueBitSet & (1L << bitIndex)) != 0;
    }

    private Object writeReplace() {
        if (replacement == null) {
            replacement = new StringSetWrapper(values());
        }
        return replacement;
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        throw new UnsupportedOperationException(
            "StringSetImpl should never be deserialized - it uses writeReplace() to serialize itself as different class entirely.");
    }

    @Override
    public final boolean contains(final String value) {
        final int highestIndex = getHighestOneBitIndex();
        final int index = reversibleLookup.rget(highestIndex, value);
        return index >= 0 && index <= highestIndex && isBitOn(index);
    }

    @Override
    public final boolean containsAny(final String... values) {
        for (final String value : values) {
            if (contains(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final boolean containsAll(final String... values) {
        for (final String value : values) {
            if (!contains(value)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public final int size() {
        return getBitCount();
    }

    @Override
    public final String[] values() {
        final String[] values = new String[size()];
        for (int bi = 0, vi = 0; bi <= getHighestOneBitIndex(); ++bi) {
            if (isBitOn(bi)) {
                values[vi++] = reversibleLookup.get(bi);
            }
        }
        return values;
    }

    @Override
    public final String[] sortedValues() {
        final String[] values = values();
        Arrays.sort(values);
        return values;
    }

    @Override
    public final boolean isEmpty() {
        return valueBitSet == 0;
    }

    @Override
    public long getEncoding(@NotNull final ToIntFunction<String> toOffset) {
        if (valueBitSet == 0) {
            return 0;
        }
        long encoding = 0;
        for (int bi = 0; bi <= getHighestOneBitIndex(); ++bi) {
            if (isBitOn(bi)) {
                final String value = reversibleLookup.get(bi);
                final int keyBitIndex = toOffset.applyAsInt(value);
                if (keyBitIndex >= Long.SIZE) {
                    throw new RuntimeException("Symbol manager returned an index " + keyBitIndex
                        + " greater than the maximum, for symbol " + value);
                }
                encoding |= (1L << keyBitIndex);
            }
        }
        return encoding;
    }

    @Override
    public final String toString() {
        return defaultToString();
    }

    @Override
    public int hashCode() {
        return defaultHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return defaultEquals(other);
    }

    @NotNull
    @Override
    public Iterator<String> iterator() {
        return Arrays.asList(values()).iterator();
    }

    public interface ReversibleLookup<DATA_TYPE> {

        /**
         * Forward lookup from the integral index.
         * 
         * @return the DATA_TYPE associated with the index.
         */

        DATA_TYPE get(long index);

        /**
         * Reverse lookup of the DATA_TYPE.
         * 
         * @return the integral index associated with the DATA_TYPE.
         */

        int rget(int highestIndex, DATA_TYPE value);
    }

}
