/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;
import java.util.function.Supplier;

abstract public class DictEncodedStringTransferBase<T>
        extends PrimitiveArrayAndVectorTransfer<T, IntBuffer, IntBuffer> {
    private boolean pageHasNull;
    private final StringDictionary dictionary;
    private final int nullPos;

    DictEncodedStringTransferBase(@NotNull ColumnSource<?> columnSource, @NotNull RowSequence tableRowSet,
            int targetPageSize, @NotNull StringDictionary dictionary, final int nullPos) {
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize,
                IntBuffer.allocate(targetPageSize / Integer.BYTES));
        this.pageHasNull = false;
        this.dictionary = dictionary;
        this.nullPos = nullPos;
    }

    final public void prepareDictionaryAndTransferOnePageToBuffer() {
        // Reset state before transferring each page
        pageHasNull = false;
        super.transferOnePageToBuffer();
    }

    @Override
    final public int transferOnePageToBuffer() {
        throw new UnsupportedOperationException("Use prepareDictionaryAndTransferOnePageToBuffer instead");
    }

    final EncodedData dictEncodingHelper(@NotNull Supplier<String> strSupplier, int numStrings) {
        final IntBuffer dictEncodedValues = IntBuffer.allocate(numStrings);
        int numBytesEncoded = 0;
        for (int i = 0; i < numStrings; i++) {
            String value = strSupplier.get();
            if (value == null) {
                dictEncodedValues.put(nullPos);
                pageHasNull = true;
                numBytesEncoded += Integer.BYTES;
                // TODO How many bytes to count null as?
            } else {
                int posInDictionary = dictionary.add(value);
                dictEncodedValues.put(posInDictionary);
            }
        }
        return new EncodedData(dictEncodedValues, numStrings, numBytesEncoded);
    }

    @Override
    final void resizeBuffer(@NotNull final int length) {
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
