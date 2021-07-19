package io.deephaven.db.v2.locations.parquet.local;

import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.local.PrivilegedFileAccessUtil;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

import static io.deephaven.db.v2.parquet.ParquetTableWriter.PARQUET_FILE_EXTENSION;

/**
 * {@link ParquetTableLocationScanner.LocationKeyFinder Parquet location finder} that will discover multiple files
 * in a single directory.
 */
public final class FlatLayout implements ParquetTableLocationScanner.LocationKeyFinder {

    private final File tableRootDirectory;

    /**
     * @param tableRootDirectory The directory to search for .parquet files.
     */
    public FlatLayout(@NotNull final File tableRootDirectory) {
        this.tableRootDirectory = tableRootDirectory;
    }

    @Override
    public void findKeys(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
        PrivilegedFileAccessUtil.doFilesystemAction(() -> {
            try (final DirectoryStream<Path> parquetFileStream = Files.newDirectoryStream(tableRootDirectory.toPath(), "*" + PARQUET_FILE_EXTENSION)) {
                for (final Path parquetFilePath : parquetFileStream) {
                    locationKeyObserver.accept(new ParquetTableLocationKey(parquetFilePath.toFile(), null));
                }
            } catch (final IOException e) {
                throw new TableDataException("FlatLayout: Error finding parquet locations under " + tableRootDirectory, e);
            }
        });
    }
}
