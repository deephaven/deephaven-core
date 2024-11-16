//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.relative;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

import java.util.Objects;

final class RelativeOutputFile implements OutputFile {
    private final String location;
    private final OutputFile file;

    public RelativeOutputFile(String location, OutputFile file) {
        this.location = Objects.requireNonNull(location);
        this.file = Objects.requireNonNull(file);
    }

    @Override
    public PositionOutputStream create() {
        return file.create();
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
        return file.createOrOverwrite();
    }

    @Override
    public String location() {
        return location;
    }

    @Override
    public InputFile toInputFile() {
        return new RelativeInputFile(location, file.toInputFile());
    }
}
