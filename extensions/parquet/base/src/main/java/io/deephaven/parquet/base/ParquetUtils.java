//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static io.deephaven.base.FileUtils.URI_SEPARATOR_CHAR;

public final class ParquetUtils {

    public static final String METADATA_FILE_NAME = "_metadata";
    public static final String COMMON_METADATA_FILE_NAME = "_common_metadata";
    public static final String PARQUET_FILE_EXTENSION = ".parquet";
    public static final String METADATA_FILE_URI_SUFFIX = URI_SEPARATOR_CHAR + METADATA_FILE_NAME;
    public static final String COMMON_METADATA_FILE_URI_SUFFIX = URI_SEPARATOR_CHAR + COMMON_METADATA_FILE_NAME;
    public static final String METADATA_FILE_SUFFIX = File.separatorChar + METADATA_FILE_NAME;
    public static final String COMMON_METADATA_FILE_SUFFIX = File.separatorChar + COMMON_METADATA_FILE_NAME;
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
     * This method verifies if the source points to a parquet file or a metadata file. Provided source can be a local
     * file path or a URI. Also, it can point to a parquet file, metadata file or a directory.
     */
    public static boolean isParquetFile(@NotNull final String source) {
        boolean ret = source.endsWith(PARQUET_FILE_EXTENSION)
                || source.endsWith(METADATA_FILE_URI_SUFFIX)
                || source.endsWith(COMMON_METADATA_FILE_URI_SUFFIX);
        if (File.separatorChar != URI_SEPARATOR_CHAR) {
            ret = ret || source.endsWith(METADATA_FILE_SUFFIX) || source.endsWith(COMMON_METADATA_FILE_SUFFIX);
        }
        return ret;
    }

    /**
     * Check if the provided path points to a non-hidden parquet file, and that none of its parents (till rootDir) are
     * hidden.
     */
    public static boolean isVisibleParquetFile(@NotNull final Path rootDir, @NotNull final Path filePath) {
        final String fileName = filePath.getFileName().toString();
        if (!fileName.endsWith(PARQUET_FILE_EXTENSION) || fileName.charAt(0) == '.') {
            return false;
        }
        Path parent = filePath.getParent();
        while (parent != null && !parent.equals(rootDir)) {
            final String parentName = parent.getFileName().toString();
            if (!parentName.isEmpty() && parentName.charAt(0) == '.') {
                return false;
            }
            parent = parent.getParent();
        }
        return true;
    }
}
