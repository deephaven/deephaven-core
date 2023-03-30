package io.deephaven.parquet.table.layout;

import java.nio.file.Path;

import static io.deephaven.parquet.table.ParquetTableWriter.PARQUET_FILE_EXTENSION;

final class ParquetFileHelper {

    public static boolean fileNameMatches(Path path) {
        return path.getFileName().toString().endsWith(PARQUET_FILE_EXTENSION);
    }
}
