package io.deephaven.parquet.table.layout;

import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.engine.table.impl.locations.local.PrivilegedFileAccessUtil;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

import static io.deephaven.parquet.table.ParquetTableWriter.PARQUET_FILE_EXTENSION;

/**
 * Parquet {@link TableLocationKeyFinder location finder} that will discover multiple files in a single directory.
 */
public final class ParquetFlatPartitionedLayout implements TableLocationKeyFinder<ParquetTableLocationKey> {

    private final File tableRootDirectory;

    /**
     * @param tableRootDirectory The directory to search for .parquet files.
     */
    public ParquetFlatPartitionedLayout(@NotNull final File tableRootDirectory) {
        this.tableRootDirectory = tableRootDirectory;
    }

    public String toString() {
        return ParquetFlatPartitionedLayout.class.getSimpleName() + '[' + tableRootDirectory + ']';
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
