/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

/**
 * TODO Add comments
 */
abstract class ArrayAndVectorTransfer<T, E, B> extends VariableWidthTransfer<T, E, B> {
    protected final IntBuffer arrayLengths; // TODO Use better name

    ArrayAndVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int maxValuesPerPage, final int targetPageSize, @NotNull final B buffer) {
        super(columnSource, tableRowSet, maxValuesPerPage, targetPageSize, buffer);
        this.arrayLengths = IntBuffer.allocate(maxValuesPerPage);
    }

    @Override
    public final IntBuffer getRepeatCount() {
        return arrayLengths;
    }

    @Override
    final boolean addNullToBuffer() {
        // TODO Do we need to add anything to buffer?
        if (!arrayLengths.hasRemaining()) {
            return false;
        }
        arrayLengths.put(QueryConstants.NULL_INT);
        return true;
    }
}

