/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

/**
 * Used as a base class of transfer objects for arrays of types like strings or big integers that need specialized
 * encoding.
 */
abstract class ObjectArrayTransfer<T> extends ObjectArrayAndVectorTransfer<T[], T> {
    private final ArrayDataSupplier<T> supplier;

    ObjectArrayTransfer(final @NotNull ColumnSource<?> columnSource, final @NotNull RowSequence tableRowSet,
                        final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
        supplier = new ArrayDataSupplier<>();
    }

    @Override
    final void encodeDataForBuffering(final T @NotNull [] data, @NotNull final EncodedData<Binary[]> encodedData) {
        supplier.fill(data);
        objectEncodingHelper(supplier, data.length, encodedData);
    }
}
