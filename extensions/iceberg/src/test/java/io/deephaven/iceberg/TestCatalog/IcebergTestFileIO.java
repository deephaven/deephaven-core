//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.TestCatalog;

import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

public class IcebergTestFileIO extends InMemoryFileIO {
    private final Set<String> inputFiles;
    private final String matchPrefix;
    private final String replacePrefix;

    public IcebergTestFileIO(final String matchPrefix, final String replacePrefix) {
        this.matchPrefix = matchPrefix;
        this.replacePrefix = replacePrefix;
        inputFiles = new HashSet<>();
    }

    @Override
    public InputFile newInputFile(String s) {
        if (!inputFiles.contains(s)) {
            try {
                final String replaced = s.replace(matchPrefix, replacePrefix);
                final byte[] data = Files.readAllBytes(Path.of(replaced));
                addFile(s, data);
                inputFiles.add(s);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return super.newInputFile(s);
    }

    @Override
    public OutputFile newOutputFile(String s) {
        return null;
    }

    @Override
    public void deleteFile(String s) {}
}
