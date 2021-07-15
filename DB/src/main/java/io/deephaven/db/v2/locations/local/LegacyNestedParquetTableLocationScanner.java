package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.PollingTableLocationProvider;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.parquet.ParquetInstructions;
import io.deephaven.db.v2.parquet.ParquetTableWriter;
import io.deephaven.util.Utils;
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
 * Location scanner for parquet locations in a layout that is similar to legacy nested-partitioning, absent one layer of
 * directories.
 */
public class LegacyNestedParquetTableLocationScanner implements PollingTableLocationProvider.Scanner {

    private static final String PARQUET_FILE_EXTENSION = ParquetTableWriter.PARQUET_FILE_EXTENSION;

    private static final ThreadLocal<TableLocationLookupKey.Reusable> reusableLocationKey = ThreadLocal.withInitial(TableLocationLookupKey.Reusable::new);

    private final File tableRootDirectory;
    private final ParquetInstructions readInstructions;

    public LegacyNestedParquetTableLocationScanner(@NotNull final File tableRootDirectory,
                                                   @NotNull final ParquetInstructions readInstructions) {
        this.tableRootDirectory = tableRootDirectory;
        this.readInstructions = readInstructions;
    }

    @Override
    public String toString() {
        return "LegacyNestedParquetTableLocationScanner[" + tableRootDirectory + ']';
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
    @NotNull
    public final TableLocation makeLocation(@NotNull final TableLocationKey locationKey) {
        final File parquetFile = new File(scanner.computeLocationBasePath(tableKey, tableLocationKey) + PARQUET_FILE_EXTENSION);
        if (Utils.fileExistsPrivileged(parquetFile)) {
            return new ReadOnlyParquetTableLocation(tableKey, tableLocationKey, parquetFile, supportsSubscriptions(), readInstructions);
        } else {
            throw new UnsupportedOperationException(this + ": Unrecognized data format in location " + tableLocationKey);
        }
    }
}
