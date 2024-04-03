//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import static io.deephaven.parquet.base.ParquetFileReader.FILE_URI_SCHEME;

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
     * @return the key value derived from the file path, used for storing each file's metadata in the combined
     *         {@value #METADATA_FILE_NAME} and {@value #COMMON_METADATA_FILE_NAME} files.
     */
    public static String getPerFileMetadataKey(final String filePath) {
        return "deephaven_per_file_" + filePath.replace(File.separatorChar, '_');
    }

    /**
     * Check if the provided file is a parquet file and none of its parents are hidden.
     */
    public static boolean isVisibleParquetFile(@NotNull final File inputFile) {
        final String fileName = inputFile.getName();
        if (!fileName.endsWith(PARQUET_FILE_EXTENSION) || fileName.charAt(0) == '.') {
            return false;
        }
        File parent = inputFile.getParentFile();
        while (parent != null) {
            final String parentName = parent.getName();
            if (!parentName.isEmpty() && parentName.charAt(0) == '.') {
                return false;
            }
            parent = parent.getParentFile();
        }
        return true;
    }
}
