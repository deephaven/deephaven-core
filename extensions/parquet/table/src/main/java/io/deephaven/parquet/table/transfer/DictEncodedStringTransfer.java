//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

/**
 * Transfer object for dictionary encoded string columns. This class updates the {@link StringDictionary} with all the
 * strings it encounters and generates an IntBuffer of dictionary position values. The class extends from
 * {@link IntCastablePrimitiveTransfer} to manage the dictionary positions similar to an Int column.
 */

final class DictEncodedStringTransfer extends IntCastablePrimitiveTransfer<ObjectChunk<String, Values>> {
    private final StringDictionary dictionary;
    private boolean pageHasNull;

    DictEncodedStringTransfer(@NotNull ColumnSource<?> columnSource, @NotNull RowSequence tableRowSet,
            int targetPageSizeInBytes, StringDictionary dictionary) {
        super(columnSource, tableRowSet, targetPageSizeInBytes);
        this.dictionary = dictionary;
        this.pageHasNull = false;
    }

    @Override
    public int transferOnePageToBuffer() {
        // Reset state before transferring each page
        pageHasNull = false;
        return super.transferOnePageToBuffer();
    }

    @Override
    public void copyAllFromChunkToBuffer() {
        int chunkSize = chunk.size();
        for (int i = 0; i < chunkSize; i++) {
            String value = chunk.get(i);
            if (value == null) {
                pageHasNull = true;
            }
            int posInDictionary = dictionary.add(value);
            buffer.put(posInDictionary);
        }
    }

    public boolean pageHasNull() {
        return pageHasNull;
    }
}
