/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;

import java.nio.Buffer;

/**
 * Used as a base class of transfer objects for vectors of primitive types.
 */
abstract class PrimitiveVectorTransfer<T extends Vector<?>, B extends Buffer>
        extends PrimitiveArrayAndVectorTransfer<T, T, B> {

    PrimitiveVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int maxValuesPerPage, final int targetPageSize, @NotNull final B buffer, final int numBytesPerValue) {
        super(columnSource, tableRowSet, maxValuesPerPage, targetPageSize, buffer, numBytesPerValue);
    }

    @Override
    final int getSize(@NotNull final T data) {
        return data.intSize();
    }
}
