//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import java.io.IOException;

public class ParquetFileReaderException extends IOException {
    public ParquetFileReaderException(String message) {
        super(message);
    }
}
