package io.deephaven.parquet.table;

import java.nio.file.Path;

public final class ParquetUtils {

    public static final String METADATA_FILE_NAME = "_metadata";
    public static final String COMMON_METADATA_FILE_NAME = "_common_metadata";
    public static final String PARQUET_FILE_EXTENSION = ".parquet";

    /**
     * Used as a filter to select relevant parquet files while reading all files in a directory.
     */
    public static boolean fileNameMatches(final Path path) {
        final String fileName = path.getFileName().toString();
        return fileName.endsWith(PARQUET_FILE_EXTENSION) && fileName.charAt(0) != '.';
    }
}
