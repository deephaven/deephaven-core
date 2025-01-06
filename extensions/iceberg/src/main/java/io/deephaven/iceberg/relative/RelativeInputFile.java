//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.relative;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import java.util.Objects;

final class RelativeInputFile implements InputFile {
    private final String location;
    private final InputFile file;

    public RelativeInputFile(String location, InputFile file) {
        this.location = Objects.requireNonNull(location);
        this.file = Objects.requireNonNull(file);
    }

    @Override
    public long getLength() {
        return file.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
        return file.newStream();
    }

    @Override
    public String location() {
        return location;
    }

    @Override
    public boolean exists() {
        return file.exists();
    }
}
