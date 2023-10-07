/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
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
abstract class ObjectVectorTransfer<V> extends ObjectArrayAndVectorTransfer<ObjectVector<V>, V> {
    ObjectVectorTransfer(final @NotNull ColumnSource<?> columnSource, final @NotNull RowSequence tableRowSet,
                         final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
    }

    @Override
    final void encodeDataForBuffering(final @NotNull ObjectVector<V> data, @NotNull final EncodedData<Binary[]> encodedData) {
        try (CloseableIterator<V> iter = data.iterator()) {
            Supplier<V> supplier = iter::next;
            objectEncodingHelper(supplier, data.intSize(), encodedData);
        }
    }
}
