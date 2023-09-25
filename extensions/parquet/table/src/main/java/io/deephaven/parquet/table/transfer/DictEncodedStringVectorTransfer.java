/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.vector.ObjectVectorColumnWrapper;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

final public class DictEncodedStringVectorTransfer
        extends PrimitiveArrayAndVectorTransfer<ObjectVectorColumnWrapper<String>, IntBuffer, IntBuffer> {
    private boolean pageHasNull;
    private final StringDictionary dictionary;
    private final int nullPos;

    public DictEncodedStringVectorTransfer(@NotNull ColumnSource<?> columnSource, @NotNull RowSequence tableRowSet,
            int targetPageSize, StringDictionary dictionary, final int nullPos) {
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize,
                IntBuffer.allocate(targetPageSize / Integer.BYTES));
        this.pageHasNull = false;
        this.dictionary = dictionary;
        this.nullPos = nullPos;
    }

    @Override
    public int transferOnePageToBuffer() {
        // Reset state before transferring each page
        pageHasNull = false;
        return super.transferOnePageToBuffer();
    }

    @Override
    EncodedData encodeDataForBuffering(@NotNull ObjectVectorColumnWrapper<String> data) {
        int numStrings = data.intSize();
        final IntBuffer dictEncodedValues = IntBuffer.allocate(numStrings);
        int numBytesEncoded = 0;
        try (CloseableIterator<String> iter = data.iterator()) {
            for (int i = 0; i < numStrings; i++) {
                String value = iter.next();
                if (value == null) {
                    dictEncodedValues.put(nullPos);
                    pageHasNull = true;
                } else {
                    int posInDictionary = dictionary.add(value);
                    dictEncodedValues.put(posInDictionary);
                    numBytesEncoded += Integer.BYTES;
                }
            }
        }
        return new EncodedData(dictEncodedValues, numStrings, numBytesEncoded);
    }

    @Override
    void resizeBuffer(@NotNull final int length) {
        buffer = IntBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final IntBuffer data) {
        data.flip();
        buffer.put(data);
    }

    @Override
    int getNumBytesBuffered() {
        return buffer.position() * Integer.BYTES;
    }

    public boolean pageHasNull() {
        return pageHasNull;
    }
}
