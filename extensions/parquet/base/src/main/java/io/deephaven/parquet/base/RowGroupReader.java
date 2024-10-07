//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.annotations.InternalUseOnly;
import org.apache.parquet.format.RowGroup;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * Provides read access to a parquet Row Group
 */
@InternalUseOnly
public interface RowGroupReader {
    /**
     * Returns the accessor to a given Column Chunk. If {@code fieldId} is present, it will be matched over
     * {@code parquetColumnNamePath}.
     *
     * @param columnName the name of the column
     * @param parquetColumnNamePath the full column parquetColumnNamePath
     * @param fieldId the field_id to fetch
     * @return the accessor to a given Column Chunk, or null if the column is not present in this Row Group
     */
    @Nullable
    ColumnChunkReader getColumnChunk(@NotNull String columnName, @NotNull List<String> defaultPath,
            @Nullable List<String> parquetColumnNamePath, @Nullable Integer fieldId);

    long numRows();

    RowGroup getRowGroup();
}
