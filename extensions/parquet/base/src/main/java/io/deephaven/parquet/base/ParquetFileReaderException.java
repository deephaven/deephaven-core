package io.deephaven.parquet.base;

import java.io.IOException;

public class ParquetFileReaderException extends IOException {
    public ParquetFileReaderException(String message) {
        super(message);
    }
}
