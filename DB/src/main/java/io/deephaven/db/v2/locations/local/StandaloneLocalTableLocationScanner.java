package io.deephaven.db.v2.locations.local;

import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.locations.SimpleTableLocationKey;
import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.function.Consumer;

/**
 * Local table location scanner that can handle a simple splayed table.
 */
public class StandaloneLocalTableLocationScanner implements LocalTableLocationProviderByScanner.Scanner {

    private final File tableParentDirectory;

    public StandaloneLocalTableLocationScanner(@NotNull final File tableParentDirectory) {
        this.tableParentDirectory = Require.neqNull(tableParentDirectory, "tableParentDirectory");
    }

    @Override
    public String toString() {
        return "StandaloneLocalTableLocationScanner[" + tableParentDirectory + ']';
    }

    @Override
    public void scanAll(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
        locationKeyObserver.accept(SimpleTableLocationKey.getInstance());
    }

    @Override
    public String computeLocationBasePath(@NotNull final TableKey tableKey, @NotNull final TableLocationKey locationKey) {
        if (TableLocationKey.COMPARATOR.compare(locationKey, SimpleTableLocationKey.getInstance()) != 0) {
            throw new UnsupportedOperationException(this + ": Unsupported location key: " + locationKey);
        }
        return tableParentDirectory.getAbsolutePath() + File.separatorChar + tableKey.getTableName();
    }
}
