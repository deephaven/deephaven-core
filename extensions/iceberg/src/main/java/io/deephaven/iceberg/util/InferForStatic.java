package io.deephaven.iceberg.util;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.io.IOException;

public class InferForStatic {
    public static void what(final Table table, final Snapshot snapshot) throws IOException {


        for (final ManifestFile manifestFile : snapshot.allManifests(table.io())) {
//            try (final ManifestReader<DataFile> manifestReader = ManifestFiles.read(manifestFile, table.io())) {
//                // ...
//            }
            final int specId = manifestFile.partitionSpecId();
            final PartitionSpec spec = table.specs().get(specId);
            spec.compatibleWith()
        }

    }
}
