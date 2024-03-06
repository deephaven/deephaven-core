//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.ObjectVector;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * Used as a base class of transfer objects for vectors of types like strings or big integers that need specialized
 * encoding.
 */
abstract class ObjectVectorTransfer<VALUE_TYPE>
        extends ObjectArrayAndVectorTransfer<ObjectVector<VALUE_TYPE>, VALUE_TYPE> {
    ObjectVectorTransfer(final @NotNull ColumnSource<?> columnSource, final @NotNull RowSequence tableRowSet,
            final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet, targetPageSizeInBytes);
    }

    @Override
    final void encodeDataForBuffering(final @NotNull ObjectVector<VALUE_TYPE> data,
            @NotNull final EncodedData<Binary[]> encodedData) {
        try (CloseableIterator<VALUE_TYPE> iter = data.iterator()) {
            Supplier<VALUE_TYPE> supplier = iter::next;
            encodeDataForBufferingHelper(supplier, data.intSize(), encodedData);
        }
    }
}
