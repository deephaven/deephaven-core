package io.deephaven.parquet.table.layout;

import io.deephaven.parquet.table.ParquetTableWriter;

import java.nio.file.Path;

final class ParquetFileHelper {
    /**
     * Used as a filter to select relevant parquet files while reading all files in a directory
     */
    static boolean parquetFileFilter(Path path) {
        return path.getFileName().toString().endsWith(ParquetTableWriter.PARQUET_FILE_EXTENSION) &&
                !path.toFile().isHidden();
    }
}
