package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.impl.TableLocationKeyFinder;
import io.deephaven.db.v2.locations.parquet.local.ParquetTableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.function.Consumer;

/**
 * Parquet {@link TableLocationKeyFinder location finder} that will discover a single file.
 */
public final class SingleParquetFileLayout
    implements TableLocationKeyFinder<ParquetTableLocationKey> {

    private final File parquetFile;

    /**
     * @param parquetFile The single parquet file to find
     */
    public SingleParquetFileLayout(@NotNull final File parquetFile) {
        this.parquetFile = parquetFile;
    }

    public String toString() {
        return SingleParquetFileLayout.class.getSimpleName() + '[' + parquetFile + ']';
    }

    @Override
    public void findKeys(@NotNull final Consumer<ParquetTableLocationKey> locationKeyObserver) {
        PrivilegedFileAccessUtil.doFilesystemAction(
            () -> locationKeyObserver.accept(new ParquetTableLocationKey(parquetFile, 0, null)));
    }
}
