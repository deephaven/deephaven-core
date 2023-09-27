package io.deephaven.parquet.base;

public class InvalidParquetFileException extends ParquetFileReaderException {
    InvalidParquetFileException(String message) {
        super(message);
    }
}
