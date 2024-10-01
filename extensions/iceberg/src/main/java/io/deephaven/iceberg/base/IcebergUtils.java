//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.base;

import io.deephaven.engine.table.impl.locations.TableDataException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class IcebergUtils {

    /**
     * Get a stream of all {@link DataFile} objects from the given {@link Table} and {@link Snapshot}.
     *
     * @param table The {@link Table} to retrieve data files for.
     * @param snapshot The {@link Snapshot} to retrieve data files from.
     * @param fileIO The {@link FileIO} to use for reading manifest data files.
     * @return A stream of {@link DataFile} objects.
     */
    public static Stream<DataFile> getAllDataFiles(
            @NotNull final Table table,
            @NotNull final Snapshot snapshot,
            @NotNull final FileIO fileIO) {
        try {
            // Retrieve the manifest files from the snapshot
            final List<ManifestFile> manifestFiles = snapshot.allManifests(fileIO);
            return manifestFiles.stream()
                    .peek(manifestFile -> {
                        if (manifestFile.content() != ManifestContent.DATA) {
                            throw new TableDataException(
                                    String.format(
                                            "%s:%d - only DATA manifest files are currently supported, encountered %s",
                                            table, snapshot.snapshotId(), manifestFile.content()));
                        }
                    })
                    .flatMap(manifestFile -> {
                        try {
                            final ManifestReader<DataFile> reader = ManifestFiles.read(manifestFile, fileIO);
                            return StreamSupport.stream(reader.spliterator(), false).onClose(() -> {
                                try {
                                    reader.close();
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            });
                        } catch (final RuntimeException e) {
                            throw new TableDataException(
                                    String.format("%s:%d:%s - error reading manifest file", table,
                                            snapshot.snapshotId(), manifestFile),
                                    e);
                        }
                    });
        } catch (final RuntimeException e) {
            throw new TableDataException(
                    String.format("%s:%d - error retrieving manifest files", table, snapshot.snapshotId()), e);
        }
    }
}
