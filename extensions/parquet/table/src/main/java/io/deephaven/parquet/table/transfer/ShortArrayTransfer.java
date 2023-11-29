/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharArrayTransfer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

final class ShortArrayTransfer extends PrimitiveArrayAndVectorTransfer<short[], short[], IntBuffer> {
    ShortArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                      final int targetPageSizeInBytes) {
        // We encode primitive shorts as primitive ints
        super(columnSource, tableRowSet, targetPageSizeInBytes / Integer.BYTES, targetPageSizeInBytes,
                IntBuffer.allocate(targetPageSizeInBytes / Integer.BYTES), Integer.BYTES);
    }

    @Override
    int getSize(final short @NotNull [] data) {
        return data.length;
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = IntBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<short[]> data) {
        for (short value : data.encodedValues) {
            buffer.put(value);
        }
    }
}
