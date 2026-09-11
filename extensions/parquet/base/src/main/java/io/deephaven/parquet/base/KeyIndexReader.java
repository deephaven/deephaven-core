//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.jetbrains.annotations.NotNull;

/**
 * Adapter to expose dictionary key indexes.
 */
final class KeyIndexReader implements PageValueReader {

    private final DictionaryValuesReader dictionaryValuesReader;

    public KeyIndexReader(@NotNull final DictionaryValuesReader dictionaryValuesReader) {
        this.dictionaryValuesReader = dictionaryValuesReader;
    }

    @Override
    public int readInteger() {
        return dictionaryValuesReader.readValueDictionaryId();
    }
}
