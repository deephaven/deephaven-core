package io.deephaven.parquet.base;

import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.jetbrains.annotations.NotNull;

/**
 * Adapter to expose dictionary key indexes.
 */
class KeyIndexReader extends ValuesReader {

    private final DictionaryValuesReader dictionaryValuesReader;

    public KeyIndexReader(@NotNull final DictionaryValuesReader dictionaryValuesReader) {
        this.dictionaryValuesReader = dictionaryValuesReader;
    }

    @Override
    public void skip() {
        throw new UnsupportedOperationException();
    }


    public int readInteger() {
        return dictionaryValuesReader.readValueDictionaryId();
    }
}
