package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.*;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.function.Consumer;

/**
 * Local table location scanner that can handle a simple splayed table.
 */
public class SplayedLocalTableLocationScanner implements LocalTableLocationProviderByScanner.Scanner {

    private final File tableRootDirectory; // final possible location of the table file
    private final String tableName;
    private boolean locationExists; // true if the location has been known to exist

    /**
     * Make a scanner for a single partition in a single directory.
     *
     * @param tableRootDirectory The root directory to find the table directory in
     * @param tableName          The table's name
     */
    SplayedLocalTableLocationScanner(@NotNull final File tableRootDirectory, @NotNull final String tableName) {
        this.tableRootDirectory = tableRootDirectory;
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "SplayedLocalTableLocationScanner[" + tableRootDirectory + File.separatorChar + tableName + ']';
    }

    @Override
    public void scanAll(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
        if (!locationExists) {
            if (tableRootDirectory.exists()) {
                if (!tableRootDirectory.isDirectory()) {
                    throw new IllegalStateException("Table location '" + tableRootDirectory + "' exists but is not a directory");
                }
                locationExists = true;
            }
        }
        if (locationExists) {
            locationKeyObserver.accept(SimpleTableLocationKey.getInstance());
        }
    }

    @Override
    public String computeLocationBasePath(@NotNull final TableKey tableKey, @NotNull final TableLocationKey locationKey) {
        if (TableLocationKey.COMPARATOR.compare(locationKey, SimpleTableLocationKey.getInstance()) != 0) {
            throw new UnsupportedOperationException(this + ": Unsupported location key: " + locationKey);
        }
        return tableRootDirectory.getAbsolutePath() + File.separatorChar + tableKey.getTableName();
    }
}
