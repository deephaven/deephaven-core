/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;
import java.util.function.Supplier;

final public class DictEncodedStringTransfer extends DictEncodedStringTransferBase<String> {
    private final int nullPos;

    public DictEncodedStringTransfer(@NotNull ColumnSource<?> columnSource, @NotNull RowSequence tableRowSet,
            int targetPageSize, StringDictionary dictionary, final int nullPos) {
        super(columnSource, tableRowSet, targetPageSize, dictionary, nullPos);
        this.nullPos = nullPos;
    }

    @Override
    boolean addNullToBuffer() {
        if (!buffer.hasRemaining()) {
            return false;
        }
        pageHasNull = true;
        buffer.put(nullPos);
        return true;
    }

    @Override
    EncodedData encodeDataForBuffering(@NotNull String data) {
        Supplier<String> supplier = () -> data;
        return dictEncodingHelper(supplier, 1);
    }

    @Override
    public IntBuffer getRepeatCount() {
        throw new UnsupportedOperationException("getRepeatCount() not supported for DictEncodedStringTransfer");
    }
}
