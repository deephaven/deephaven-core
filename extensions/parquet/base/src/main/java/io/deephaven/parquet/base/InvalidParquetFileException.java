package io.deephaven.parquet.base;

/**
 * Exception thrown when trying to read an invalid Parquet file.
 */
public class InvalidParquetFileException extends ParquetFileReaderException {
    InvalidParquetFileException(String message) {
        super(message);
    }
}
