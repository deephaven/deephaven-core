package io.deephaven.parquet;

import org.apache.parquet.hadoop.metadata.BlockMetaData;

public interface RowGroupWriter {
    ColumnWriter addColumn(String columnName);

    BlockMetaData getBlock();
}
