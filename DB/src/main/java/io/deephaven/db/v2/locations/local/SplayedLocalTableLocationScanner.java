package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.*;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.function.Consumer;

/**
 * Local table location scanner that can handle a simple splayed table.
 */
public class SplayedLocalTableLocationScanner implements LocalTableLocationProvider.Scanner {

    private final File tableLocationDirectory; // final possible location of the table file
    private boolean locationExists; // true if the location has been known to exist

    /**
     * Make a scanner for a single partition in a single directory.
     *
     * @param tableRootDirectory The root directory to find the table directory in
     * @param tableName          The table's name
     */
    SplayedLocalTableLocationScanner(@NotNull final File tableRootDirectory, @NotNull final String tableName) {
        this.tableLocationDirectory = new File(tableRootDirectory, tableName);
    }

    @Override
    public String toString() {
        return "SplayedLocalTableLocationScanner[" + tableLocationDirectory + ']';
    }

    @Override
    public void scanAll(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
        if (!locationExists) {
            if (tableLocationDirectory.exists()) {
                if (!tableLocationDirectory.isDirectory()) {
                    throw new IllegalStateException("Table location '" + tableLocationDirectory + "' exists but is not a directory");
                }
                locationExists = true;
            }
        }
        if (locationExists) {
            locationKeyObserver.accept(SimpleTableLocationKey.getInstance());
        }
    }

    @Override
    public File computeLocationDirectory(@NotNull final TableKey tableKey, @NotNull final TableLocationKey locationKey) {
        if (TableLocationKey.COMPARATOR.compare(locationKey, SimpleTableLocationKey.getInstance()) != 0) {
            throw new UnsupportedOperationException(this + ": Unsupported location key: " + locationKey);
        }
        return tableLocationDirectory;
    }
}
