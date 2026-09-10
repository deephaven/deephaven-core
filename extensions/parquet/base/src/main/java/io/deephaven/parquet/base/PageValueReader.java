//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.io.api.Binary;

/**
 * A narrow view of {@link org.apache.parquet.column.values.ValuesReader} exposing only the read methods used by
 * {@link PageMaterializer} implementations. Each method mirrors the corresponding {@code ValuesReader} method and, like
 * {@code ValuesReader}, throws {@link UnsupportedOperationException} by default for values the implementation does not
 * support reading.
 */
public interface PageValueReader {

    default int readInteger() {
        throw new UnsupportedOperationException();
    }

    default long readLong() {
        throw new UnsupportedOperationException();
    }

    default boolean readBoolean() {
        throw new UnsupportedOperationException();
    }

    default float readFloat() {
        throw new UnsupportedOperationException();
    }

    default double readDouble() {
        throw new UnsupportedOperationException();
    }

    default Binary readBytes() {
        throw new UnsupportedOperationException();
    }
}
