//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * Adapts a {@link ValuesReader} to the narrower {@link PageValueReader} interface.
 */
final class PageValueReaderImpl implements PageValueReader {

    private final ValuesReader valuesReader;

    PageValueReaderImpl(@NotNull final ValuesReader valuesReader) {
        this.valuesReader = Objects.requireNonNull(valuesReader);
    }

    @Override
    public int readInteger() {
        return valuesReader.readInteger();
    }

    @Override
    public long readLong() {
        return valuesReader.readLong();
    }

    @Override
    public boolean readBoolean() {
        return valuesReader.readBoolean();
    }

    @Override
    public float readFloat() {
        return valuesReader.readFloat();
    }

    @Override
    public double readDouble() {
        return valuesReader.readDouble();
    }

    @Override
    public Binary readBytes() {
        return valuesReader.readBytes();
    }
}
