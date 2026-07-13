//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.parquet.table.DictionarySizeExceededException;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;

import java.util.Arrays;

/**
 * Stores a dictionary of strings and returns their position in the dictionary, useful for encoding string columns.
 */
final public class StringDictionary {

    private static final int INITIAL_DICTIONARY_SIZE = 1 << 8;

    private final int maxKeys;
    private final int maxDictSize;
    private final Statistics<?> statistics;
    /**
     * {@code null} is not added to the dictionary. This class will return the following position value on encountering
     * a {@code null}.
     */
    private final int nullPos;

    private final Object2IntMap<String> keyToPos;

    private Binary[] encodedKeys;
    private int keyCount;
    private int dictSize;

    public StringDictionary(final int maxKeys, final int maxDictSize, final Statistics<?> statistics,
            final int nullPos) {
        this.maxKeys = maxKeys;
        this.maxDictSize = maxDictSize;
        this.statistics = statistics;
        this.nullPos = nullPos;

        // Kept as a negative value since 0 is a valid position in the dictionary.
        final int NO_ENTRY_VALUE = -1;
        // Preserve the Trove default capacity (10) and load factor (0.5f) rather than fastutil's 16/0.75f.
        final Object2IntMap<String> tmpKeyToPos = new Object2IntOpenHashMap<>(10, 0.5f);
        tmpKeyToPos.defaultReturnValue(NO_ENTRY_VALUE);
        this.keyToPos = tmpKeyToPos;

        this.encodedKeys = new Binary[Math.min(INITIAL_DICTIONARY_SIZE, maxKeys)];
        this.dictSize = this.keyCount = 0;
    }

    public int getKeyCount() {
        return keyCount;
    }

    public Binary[] getEncodedKeys() {
        return encodedKeys;
    }

    /**
     * Add a string key to the dictionary if it's not already present.
     *
     * @param key The key to add and/or find the position for
     * @return {@code key}'s position in the dictionary, or special null key position (passed in constructor) if
     *         {@code key == null}
     */
    public int add(final String key) {
        if (key == null) {
            return nullPos;
        }
        int posInDictionary = keyToPos.getInt(key);
        if (posInDictionary == keyToPos.defaultReturnValue()) {
            if (keyCount == encodedKeys.length) {
                // Copy into an array of double the size with upper limit at maxKeys
                if (keyCount == maxKeys) {
                    throw new DictionarySizeExceededException("Dictionary maximum keys exceeded");
                }
                encodedKeys = Arrays.copyOf(encodedKeys, (int) Math.min(keyCount * 2L, maxKeys));
            }
            final Binary encodedKey = Binary.fromString(key);
            dictSize += encodedKey.length();
            if (dictSize > maxDictSize) {
                throw new DictionarySizeExceededException("Dictionary maximum size exceeded");
            }
            encodedKeys[keyCount] = encodedKey;
            // Track the min/max statistics while the dictionary is being built.
            statistics.updateStats(encodedKey);
            posInDictionary = keyCount;
            keyCount++;
            keyToPos.put(key, posInDictionary);
        }
        return posInDictionary;
    }
}
