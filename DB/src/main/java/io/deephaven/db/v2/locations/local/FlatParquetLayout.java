package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.impl.TableLocationKeyFinder;
import io.deephaven.db.v2.locations.parquet.local.ParquetTableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

import static io.deephaven.db.v2.parquet.ParquetTableWriter.PARQUET_FILE_EXTENSION;

/**
 * Parquet {@link TableLocationKeyFinder location finder} that will discover multiple files in a single directory.
 */
public final class FlatParquetLayout implements TableLocationKeyFinder<ParquetTableLocationKey> {

    private final File tableRootDirectory;

    /**
     * @param tableRootDirectory The directory to search for .parquet files.
     */
    public FlatParquetLayout(@NotNull final File tableRootDirectory) {
        this.tableRootDirectory = tableRootDirectory;
    }

    public String toString() {
        return FlatParquetLayout.class.getSimpleName() + '[' + tableRootDirectory + ']';
    }

    @Override
    public void findKeys(@NotNull final Consumer<ParquetTableLocationKey> locationKeyObserver) {
        PrivilegedFileAccessUtil.doFilesystemAction(() -> {
            try (final DirectoryStream<Path> parquetFileStream =
                    Files.newDirectoryStream(tableRootDirectory.toPath(), "*" + PARQUET_FILE_EXTENSION)) {
                for (final Path parquetFilePath : parquetFileStream) {
                    locationKeyObserver.accept(new ParquetTableLocationKey(parquetFilePath.toFile(), 0, null));
                }
            } catch (final IOException e) {
                throw new TableDataException("Error finding parquet locations under " + tableRootDirectory, e);
            }
        });
    }
}
