/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.TableLocationLookupKey;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.function.Consumer;

/**
 * Local table location scanner that can handle two-level partitioning.
 */
public class NestedPartitionedLocalTableLocationScanner implements LocalTableLocationProviderByScanner.Scanner {

    private static final ThreadLocal<TableLocationLookupKey.Reusable> reusableLocationKey = ThreadLocal.withInitial(TableLocationLookupKey.Reusable::new);

    private final File tableRootDirectory;

    /**
     * Make a scanner for multiple partitions, nested from a single root directory.
     *
     * @param tableRootDirectory The root directory to begin scans from
     */
    @VisibleForTesting
    public NestedPartitionedLocalTableLocationScanner(@NotNull final File tableRootDirectory) {
        this.tableRootDirectory = tableRootDirectory;
    }

    @Override
    public String toString() {
        return "NestedPartitionedLocalTableLocationScanner[" + tableRootDirectory + ']';
    }

    @Override
    public void scanAll(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
        final TableLocationLookupKey.Reusable locationKey = reusableLocationKey.get();
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                try (final DirectoryStream<Path> internalPartitionStream = Files.newDirectoryStream(tableRootDirectory.toPath(), Files::isDirectory)) {
                    for (final Path internalPartition : internalPartitionStream) {
                        locationKey.getInternalPartition().clear().append(internalPartition.getFileName().toString());
                        try (final DirectoryStream<Path> columnPartitionStream = Files.newDirectoryStream(internalPartition, Files::isDirectory)) {
                            for (final Path columnPartition : columnPartitionStream) {
                                locationKey.getColumnPartition().clear().append(columnPartition.getFileName().toString());
                                locationKeyObserver.accept(locationKey);
                            }
                        }
                    }
                } catch (final NoSuchFileException | FileNotFoundException ignored) {
                    // If we found nothing at all, then there's nothing to be done at this level.
                } catch (final IOException e) {
                    throw new TableDataException(this + ": Error refreshing table locations for " + tableRootDirectory, e);
                }
                return null;
            });

        } catch (final PrivilegedActionException pae) {
            if (pae.getException() instanceof TableDataException) {
                throw (TableDataException) pae.getException();
            } else {
                throw new RuntimeException(pae.getException());
            }
        }
    }

    @Override
    public String computeLocationBasePath(@NotNull final TableKey tableKey, @NotNull final TableLocationKey locationKey) {
        return tableRootDirectory.getAbsolutePath()
                + File.separatorChar + locationKey.getInternalPartition().toString()
                + File.separatorChar + locationKey.getColumnPartition().toString()
                + File.separatorChar + tableKey.getTableName().toString();
    }
}
