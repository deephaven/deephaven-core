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
 * with all the strings it encounters and generates an IntBuffer of dictionary position values. This class extends
 * {@link PrimitiveArrayAndVectorTransfer} to manage the dictionary positions in an {@link IntBuffer} similar to an Int
 * array/vector column.
 */
abstract public class DictEncodedStringTransferBase<T>
        extends PrimitiveArrayAndVectorTransfer<T, IntBuffer, IntBuffer> {
    private boolean pageHasNull;
    private final StringDictionary dictionary;
    private final int nullValue; // The value to store in buffer for null strings
    private IntBuffer dictEncodedValues;

    DictEncodedStringTransferBase(@NotNull ColumnSource<?> columnSource, @NotNull RowSequence tableRowSet,
            int targetPageSize, @NotNull StringDictionary dictionary, final int nullValue) {
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize,
                IntBuffer.allocate(targetPageSize / Integer.BYTES));
        this.pageHasNull = false;
        this.dictionary = dictionary;
        this.nullValue = nullValue;
        this.dictEncodedValues = IntBuffer.allocate(targetPageSize);
    }

    @Override
    final public int transferOnePageToBuffer() {
        // Reset state before transferring each page
        pageHasNull = false;
        return super.transferOnePageToBuffer();
    }

    /**
     * Helper method which takes a string supplier (from the array/vector transfer child classes) and number of strings,
     * fetches that many strings from the supplier, adds them to the dictionary and populates an IntBuffer with
     * dictionary position values.
     */
    final void dictEncodingHelper(@NotNull Supplier<String> strSupplier, int numStrings,
            @NotNull final EncodedData<IntBuffer> encodedData) {
        dictEncodedValues.clear();
        if (numStrings > dictEncodedValues.limit()) {
            dictEncodedValues = IntBuffer.allocate(numStrings);
        }
        int numBytesEncoded = 0;
        for (int i = 0; i < numStrings; i++) {
            String value = strSupplier.get();
            if (value == null) {
                dictEncodedValues.put(nullValue);
                pageHasNull = true;
                numBytesEncoded += Integer.BYTES;
            } else {
                int posInDictionary = dictionary.add(value);
                dictEncodedValues.put(posInDictionary);
            }
        }
        encodedData.fillRepeated(dictEncodedValues, numBytesEncoded, numStrings);
    }

    @Override
    final void resizeBuffer(final int length) {
        buffer = IntBuffer.allocate(length);
    }

    @Override
    final void copyToBuffer(@NotNull final IntBuffer data) {
        data.flip();
        buffer.put(data);
    }

    @Override
    final int getNumBytesBuffered() {
        return (buffer.position() + repeatCounts.position()) * Integer.BYTES;
    }

    final public boolean pageHasNull() {
        return pageHasNull;
    }
}
