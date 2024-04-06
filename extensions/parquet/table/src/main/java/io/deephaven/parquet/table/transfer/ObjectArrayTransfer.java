//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

/**
 * Used as a base class of transfer objects for arrays of types like strings or big integers that need specialized
 * encoding.
 */
abstract class ObjectArrayTransfer<VALUE_TYPE> extends ObjectArrayAndVectorTransfer<VALUE_TYPE[], VALUE_TYPE> {
    private final ArrayDataSupplier<VALUE_TYPE> supplier;

    ObjectArrayTransfer(final @NotNull ColumnSource<?> columnSource, final @NotNull RowSequence tableRowSet,
            final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet, targetPageSizeInBytes);
        supplier = new ArrayDataSupplier<>();
    }

    @Override
    final void encodeDataForBuffering(final VALUE_TYPE @NotNull [] data,
            @NotNull final EncodedData<Binary[]> encodedData) {
        supplier.fill(data);
        encodeDataForBufferingHelper(supplier, data.length, encodedData);
    }
}
