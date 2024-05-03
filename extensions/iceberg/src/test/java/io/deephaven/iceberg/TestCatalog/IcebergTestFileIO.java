package io.deephaven.iceberg.TestCatalog;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;

import java.util.HashMap;
import java.util.Map;

public class IcebergTestFileIO implements FileIO {
    public static final IcebergTestFileIO INSTANCE = new IcebergTestFileIO();

    private Map<String, IcebergTestManifestFile> manifestFiles = new HashMap<>();

    public void addManifestFile(String path, IcebergTestManifestFile manifestFile) {
        manifestFiles.put(path, manifestFile);
    }

    public static class IcebergTestInputFile implements InputFile {
        @Override
        public long getLength() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public SeekableInputStream newStream() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public String location() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public boolean exists() {
            throw new UnsupportedOperationException("Not implemented");
        }
    }

    @Override
    public InputFile newInputFile(String s) {
        if (manifestFiles.containsKey(s)) {
            final ManifestFile manifestFile = manifestFiles.get(s);
            final OutputFile f = null;
            ManifestWriter<DataFile> writer = ManifestFiles.write(manifestFile.partitionSpecId(), f);
            return new IcebergTestInputFile();
        }
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public OutputFile newOutputFile(String s) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void deleteFile(String s) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
