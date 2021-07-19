package io.deephaven.db.v2.locations.parquet.local;

import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.local.PrivilegedFileAccessUtil;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.function.Consumer;

/**
 * {@link ParquetTableLocationScanner.LocationKeyFinder Parquet location finder} that will discover a single file.
 */
public final class SingleFileLayout implements ParquetTableLocationScanner.LocationKeyFinder {

    private final File parquetFile;

    /**
     * @param parquetFile The single parquet file to find
     */
    public SingleFileLayout(@NotNull final File parquetFile) {
        this.parquetFile = parquetFile;
    }

    @Override
    public void findKeys(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
        PrivilegedFileAccessUtil.doFilesystemAction(() -> {
            locationKeyObserver.accept(new ParquetTableLocationKey(parquetFile, null));
        });
    }
}
