package io.deephaven.parquet;

import java.util.List;

/**
 * Provides read access to a parquet Row Group
 */
public interface RowGroupReader {
    /**
     * Returns the accessor to a given Column Chunk
     * @param path the full column path
     * @return the accessor to a given Column Chunk
     */
    ColumnChunkReader getColumnChunk(List<String> path);

    long numRows();
}
