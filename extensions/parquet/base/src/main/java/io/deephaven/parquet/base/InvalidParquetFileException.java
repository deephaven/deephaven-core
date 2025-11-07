//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

/**
 * Exception thrown when trying to read an invalid Parquet file.
 */
public class InvalidParquetFileException extends ParquetFileReaderException {
    InvalidParquetFileException(String message) {
        super(message);
    }
}
