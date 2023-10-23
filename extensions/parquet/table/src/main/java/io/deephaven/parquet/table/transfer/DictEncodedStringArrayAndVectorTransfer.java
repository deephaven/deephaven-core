/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;
import java.util.function.Supplier;

/**
 * Base class for reading dictionary-encoded string arrays and vectors. This class updates the {@link StringDictionary}
 * with all the strings it encounters and generates an integer array of dictionary position values. This class extends
 * {@link PrimitiveArrayAndVectorTransfer} to manage the dictionary positions similar to an integer array column.
 */
abstract public class DictEncodedStringArrayAndVectorTransfer<T>
        extends PrimitiveArrayAndVectorTransfer<T, int[], IntBuffer> {
    private final StringDictionary dictionary;

    private boolean pageHasNull;
    private int[] dictEncodedValues;
    private int numDictEncodedValues;

    DictEncodedStringArrayAndVectorTransfer(@NotNull ColumnSource<?> columnSource, @NotNull RowSequence tableRowSet,
            int targetPageSize, @NotNull StringDictionary dictionary) {
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize,
                IntBuffer.allocate(targetPageSize / Integer.BYTES), Integer.BYTES);
        this.dictionary = dictionary;

        this.pageHasNull = false;
        this.dictEncodedValues = new int[targetPageSize];
        this.numDictEncodedValues = 0;
    }

    @Override
    public final int transferOnePageToBuffer() {
        // Reset state before transferring each page
        pageHasNull = false;
        return super.transferOnePageToBuffer();
    }

    /**
     * Helper method which takes a string supplier (from the array/vector transfer child classes) and number of strings,
     * fetches that many strings from the supplier, adds them to the dictionary and populates an IntBuffer with
     * dictionary position values.
     */
    final void encodeDataForBufferingHelper(@NotNull Supplier<String> strSupplier, final int numStrings,
            @NotNull final EncodedData<int[]> encodedData) {
        numDictEncodedValues = 0;
        if (numStrings > dictEncodedValues.length) {
            dictEncodedValues = new int[numStrings];
        }
        int numBytesEncoded = 0;
        for (int i = 0; i < numStrings; i++) {
            String value = strSupplier.get();
            if (value == null) {
                pageHasNull = true;
            } else {
                numBytesEncoded += Integer.BYTES;
            }
            int posInDictionary = dictionary.add(value);
            dictEncodedValues[numDictEncodedValues++] = posInDictionary;
        }
        encodedData.fillRepeated(dictEncodedValues, numBytesEncoded, numDictEncodedValues);
    }

    @Override
    final void resizeBuffer(final int length) {
        buffer = IntBuffer.allocate(length);
    }

    @Override
    final void copyToBuffer(@NotNull final EncodedData<int[]> data) {
        buffer.put(data.encodedValues, 0, data.numValues);
    }

    public final boolean pageHasNull() {
        return pageHasNull;
    }
}
