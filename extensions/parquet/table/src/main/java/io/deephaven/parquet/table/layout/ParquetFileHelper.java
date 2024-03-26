//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.layout;

import io.deephaven.parquet.table.ParquetTableWriter;

import java.net.URI;
import java.util.regex.Pattern;

final class ParquetFileHelper {
    /**
     * Used as a filter to select relevant parquet files while reading all files in a directory.
     */
    private static final Pattern HIDDEN_FILE_PATTERN = Pattern.compile("(^|/)\\.[^/]+");

    static boolean isVisibleParquetURI(final URI uri) {
        final String path = uri.getPath();
        if (!path.endsWith(ParquetTableWriter.PARQUET_FILE_EXTENSION)) {
            return false;
        }
        // Look for hidden directories or files in the path
        return !HIDDEN_FILE_PATTERN.matcher(path).find();
    }
}
