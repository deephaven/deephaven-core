package io.deephaven.parquet.table.layout;

import io.deephaven.parquet.table.ParquetTableWriter;

import java.nio.file.Path;

final class ParquetFileHelper {
    /**
     * Used as a filter to select relevant parquet files while reading all files in a directory.
     */
    static boolean fileNameMatches(final Path path) {
        final String fileName = path.getFileName().toString();
        return fileName.endsWith(ParquetTableWriter.PARQUET_FILE_EXTENSION) && fileName.charAt(0) != '.';
    }
}
