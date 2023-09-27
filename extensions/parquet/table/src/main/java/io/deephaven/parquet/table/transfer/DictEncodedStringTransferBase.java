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
 * Base class for transferring dictionary-encoded strings. This class updates the {@link StringDictionary} with all the
 * strings it encounters and generates an IntBuffer of dictionary position values. This classes extends
 * {@link PrimitiveArrayAndVectorTransfer} to manage the dictionary position value buffers.
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

    /**
     * This method is used to prepare the dictionary and transfer one page of data to the buffer. This method should be
     * used instead of {@link #transferOnePageToBuffer()}.
     */
    final public void prepareDictionaryAndTransferOnePageToBuffer() {
        // Reset state before transferring each page
        pageHasNull = false;
        super.transferOnePageToBuffer();
    }

    @Override
    final public int transferOnePageToBuffer() {
        throw new UnsupportedOperationException("Use prepareDictionaryAndTransferOnePageToBuffer instead");
    }

    /**
     * Helper method which takes a string supplier and number of strings, fetches that many strings from the supplier,
     * adds them to the dictionary and populates an IntBuffer with dictionary position values.
     */
    final void dictEncodingHelper(@NotNull Supplier<String> strSupplier, int numStrings) {
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
        encodedData.fill(dictEncodedValues, numStrings, numBytesEncoded);
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
        return buffer.position() * Integer.BYTES;
    }

    final public boolean pageHasNull() {
        return pageHasNull;
    }

    final void setPageHasNull() {
        this.pageHasNull = true;
    }
}
