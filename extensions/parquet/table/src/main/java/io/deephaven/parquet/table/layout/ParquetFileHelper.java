package io.deephaven.parquet.table.layout;

import io.deephaven.parquet.table.ParquetTableWriter;

import java.net.URI;
import java.nio.file.Path;
import java.util.regex.Pattern;

final class ParquetFileHelper {
    /**
     * Used as a filter to select relevant parquet files while reading all files in a directory.
     */
    static boolean fileNameMatches(final Path path) {
        final String fileName = path.getFileName().toString();
        return fileName.endsWith(ParquetTableWriter.PARQUET_FILE_EXTENSION) && fileName.charAt(0) != '.';
    }

    static boolean fileNameMatches(final URI uri) {
        // TODO Test this for file paths
        final String path = uri.getPath();
        final String fileName = path.substring(path.lastIndexOf('/') + 1);
        return fileName.endsWith(ParquetTableWriter.PARQUET_FILE_EXTENSION) && fileName.charAt(0) != '.';
    }

    private static final Pattern HIDDEN_FILE_PATTERN = Pattern.compile("(^|/)\\.[^/]+");

    static boolean isNonHiddenParquetURI(final URI uri) { // TODO better names for this file
        final String path = uri.getPath();
        if (!path.endsWith(ParquetTableWriter.PARQUET_FILE_EXTENSION)) {
            return false;
        }
        // Look for hidden directories or files in the path
        return !HIDDEN_FILE_PATTERN.matcher(path).find();
    }
}
