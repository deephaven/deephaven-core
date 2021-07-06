package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.function.Consumer;

/**
 * Local table location scanner that wraps a {@link TableLocationMetadataIndex}.
 */
public class IndexedLocalTableLocationScanner implements LocalTableLocationProviderByScanner.Scanner {

    private final File tableRootDirectory;
    private final TableLocationMetadataIndex tableLocationMetadataIndex;

    @VisibleForTesting
    public IndexedLocalTableLocationScanner(@NotNull final File tableRootDirectory,
                                            @NotNull final TableLocationMetadataIndex tableLocationMetadataIndex) {
        this.tableRootDirectory = tableRootDirectory;
        this.tableLocationMetadataIndex = tableLocationMetadataIndex;
    }

    @Override
    public String toString() {
        return "IndexedLocalTableLocationScanner[" + tableRootDirectory + ']';
    }

    @Override
    public void scanAll(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
        for (@NotNull final TableLocationKey tableLocationKey : tableLocationMetadataIndex.getTableLocationSnapshots()) {
            locationKeyObserver.accept(tableLocationKey);
        }
    }

    @Override
    public String computeLocationBasePath(@NotNull final TableKey tableKey, @NotNull final TableLocationKey locationKey) {
        return tableRootDirectory.getAbsolutePath()
                + File.separatorChar + locationKey.getInternalPartition().toString()
                + File.separatorChar + locationKey.getColumnPartition().toString()
                + File.separatorChar + tableKey.getTableName().toString();
    }

    @Override
    public TableLocationMetadataIndex getMetadataIndex() {
        return tableLocationMetadataIndex;
    }
}
