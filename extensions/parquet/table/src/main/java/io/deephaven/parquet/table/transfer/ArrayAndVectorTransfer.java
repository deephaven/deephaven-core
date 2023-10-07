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

/**
 * Base class for all array and vector transfer objects
 */
public abstract class ArrayAndVectorTransfer<T, E, B> extends VariableWidthTransfer<T, E, B> {
    final IntBuffer repeatCounts; // Stores the lengths of arrays/vectors

    ArrayAndVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int maxValuesPerPage, final int targetPageSize, @NotNull final B buffer) {
        super(columnSource, tableRowSet, maxValuesPerPage, targetPageSize, buffer);
        this.repeatCounts = IntBuffer.allocate(maxValuesPerPage);
    }

    @Override
    public final IntBuffer getRepeatCount() {
        return repeatCounts;
    }

    @Override
    final boolean addNullToBuffer() {
        if (!repeatCounts.hasRemaining()) {
            return false;
        }
        repeatCounts.put(QueryConstants.NULL_INT);
        return true;
    }

    @Override
    final boolean isBufferEmpty() {
        return repeatCounts.position() == 0;
    }

    /**
     * Helper class for creating a supplier of array data
     * 
     * @param <A> The type of the array
     */
    static final class ArrayDataSupplier<A> implements Supplier<A> {
        private A[] data;
        private int pos = 0;

        void fill(final @NotNull A @NotNull [] data) {
            this.data = data;
            this.pos = 0;
        }

        @Override
        public A get() {
            return data[pos++];
        }
    }
}

