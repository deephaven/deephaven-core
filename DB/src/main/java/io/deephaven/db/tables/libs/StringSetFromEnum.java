/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.libs;


import io.deephaven.db.tables.StringSetWrapper;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class provides a object that converts from list of {@code String} values and their binary masks to a
 * StringSetWrapper. (The first string corresponds to the value 1, the second to 2, the third to 4, etc.) Because of the
 * use of values of 2, this conversion can handle bit sets. It is however limited to 31 possible enum values.
 */
public class StringSetFromEnum {

    protected final String[] strings;
    protected final TIntObjectHashMap<StringSet> enumsToString = new TIntObjectHashMap<>();
    protected final ReentrantReadWriteLock setLock = new ReentrantReadWriteLock();

    /**
     * Create a converter for the sequence of {@code enums}, where the i-th enum in the sequence is associated with the
     * value {@code Math.pow(2,i)} (starting with index 0 and value 1).
     */
    public StringSetFromEnum(String enums[]) {
        strings = Arrays.copyOf(enums, enums.length);
        enumsToString.put(0, new StringSetWrapper());
        for (int i = 0; i < enums.length; i++) {
            // assert(enums[i] != null); // you'd think!
            enumsToString.put((int) Math.pow(2, i), new StringSetWrapper(enums[i]));
        }
    }

    /**
     * Return a string representation of the enum bit set given by the {@code originalIndex}.
     */
    public StringSet convert(final int originalIndex) {
        setLock.readLock().lock();
        try {
            StringSet singleResult = enumsToString.get(originalIndex);
            if (singleResult != null) {
                return singleResult;
            }
        } finally {
            setLock.readLock().unlock();
        }

        // It's not a single value, so let's enumerate powers of two and create a bit set.
        int index = originalIndex;
        HashSet<String> stringValues = new HashSet<>();
        for (int count = 0; index > 0 && count < strings.length; ++count, index >>>= 1) {
            if ((index & 1) != 0) {
                if (count < strings.length && strings[count] != null) {
                    stringValues.add(strings[count]);
                }
            }
        }
        final StringSetWrapper stringSet = new StringSetWrapper(stringValues);
        setLock.writeLock().lock();
        try {
            enumsToString.put(originalIndex, stringSet);
        } finally {
            setLock.writeLock().unlock();
        }
        return stringSet;
    }
}
