package io.deephaven.parquet.table.layout;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;

import static io.deephaven.parquet.table.ParquetTableWriter.PARQUET_FILE_EXTENSION;

final class ParquetFileHelper {

    private static final PathMatcher MATCHER;

    static {
        MATCHER = FileSystems.getDefault().getPathMatcher(String.format("glob:*%s", PARQUET_FILE_EXTENSION));
    }

    public static boolean fileNameMatches(Path path) {
        return MATCHER.matches(path.getFileName());
    }
}
