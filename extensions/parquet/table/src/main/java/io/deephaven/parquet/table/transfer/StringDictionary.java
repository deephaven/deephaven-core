/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.parquet.table.transfer;

import gnu.trove.impl.Constants;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.parquet.table.DictionarySizeExceededException;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;

import java.util.Arrays;

/**
 * Stores a dictionary of strings and returns their position in the dictionary, useful for encoding string columns.
 */
final public class StringDictionary {
    private static final int INITIAL_DICTIONARY_SIZE = 1 << 8;
    private Binary[] encodedKeys;
    private int keyCount;
    private int dictSize;
    private final int maxKeys;
    private final int maxDictSize;
    private final Statistics<?> statistics;
    private final TObjectIntHashMap<String> keyToPos;
    private static final int NULL_KEY_POS = -1;

    public StringDictionary(final int maxKeys, final int maxDictSize, final Statistics<?> statistics) {
        this.dictSize = this.keyCount = 0;
        this.maxKeys = maxKeys;
        this.maxDictSize = maxDictSize;
        this.encodedKeys = new Binary[Math.min(INITIAL_DICTIONARY_SIZE, maxKeys)];
        this.statistics = statistics;
        this.keyToPos = new TObjectIntHashMap<>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR);
    }

    public int getKeyCount() {
        return keyCount;
    }

    public Binary[] getEncodedKeys() {
        return encodedKeys;
    }

    /**
     * Add a string key to the dictionary and return its position in the dictionary. The function returns the value
     * {@link #NULL_KEY_POS} if the key is null.
     */
    public int add(final String key) {
        if (key == null) {
            // Nulls are not added to the dictionary
            return NULL_KEY_POS;
        }
        int posInDictionary = keyToPos.get(key);
        if (posInDictionary == keyToPos.getNoEntryValue()) {
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
