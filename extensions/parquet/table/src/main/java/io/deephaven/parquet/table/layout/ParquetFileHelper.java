//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.layout;

import io.deephaven.parquet.table.ParquetTableWriter;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.nio.file.Path;

public final class ParquetFileHelper {
    /**
     * Used as a filter to select relevant parquet files while reading all files in a directory.
     */
    static boolean fileNameMatches(final Path path) {
        final String fileName = path.getFileName().toString();
        return fileName.endsWith(ParquetTableWriter.PARQUET_FILE_EXTENSION) && fileName.charAt(0) != '.';
    }

    /**
     * Convert a parquet source to a URI.
     *
     * @param source The path or URI of parquet file or directory to examine
     * @return The URI
     */
    public static URI convertParquetSourceToURI(@NotNull final String source) {
        if (source.endsWith(".parquet")) {
            return SeekableChannelsProvider.convertToURI(source, false);
        }
        return SeekableChannelsProvider.convertToURI(source, true);
    }
}
