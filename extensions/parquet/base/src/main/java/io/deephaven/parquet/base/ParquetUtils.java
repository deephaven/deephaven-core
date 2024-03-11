package io.deephaven.parquet.base;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public final class ParquetUtils {

    public static final String METADATA_FILE_NAME = "_metadata";
    public static final String COMMON_METADATA_FILE_NAME = "_common_metadata";
    public static final String PARQUET_FILE_EXTENSION = ".parquet";
    private static final String MAGIC_STR = "PAR1";
    public static final byte[] MAGIC = MAGIC_STR.getBytes(StandardCharsets.US_ASCII);

    /**
     * The number of bytes to buffer before flushing while writing parquet files and metadata files.
     */
    public static final int PARQUET_OUTPUT_BUFFER_SIZE = 1 << 18;

    /**
     * Used as a key for storing deephaven specific metadata in the key-value metadata of parquet files.
     */
    public static final String METADATA_KEY = "deephaven";

    /**
     * Used as a filter to select relevant parquet files while reading all files in a directory.
     */
    public static boolean fileNameMatches(final Path path) {
        final String fileName = path.getFileName().toString();
        return fileName.endsWith(PARQUET_FILE_EXTENSION) && fileName.charAt(0) != '.';
    }

    /**
     * @return the key value for storing each file's metadata in the common metadata file.
     */
    public static String getKeyForFilePath(final Path path) {
        final String fileName = path.getFileName().toString();
        return "deephaven_per_file_" + fileName;
    }
}
