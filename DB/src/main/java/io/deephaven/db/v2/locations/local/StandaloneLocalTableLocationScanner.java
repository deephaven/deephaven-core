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
public class StandaloneLocalTableLocationScanner implements LocalTableLocationProvider.Scanner {

    private final File tableDirectory;

    public StandaloneLocalTableLocationScanner(@NotNull final File tableDirectory) {
        this.tableDirectory = Require.neqNull(tableDirectory, "tableDirectory");
    }

    @Override
    public String toString() {
        return "StandaloneLocalTableLocationScanner[" + tableDirectory + ']';
    }

    @Override
    public void scanAll(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
        locationKeyObserver.accept(SimpleTableLocationKey.getInstance());
    }

    @Override
    public File computeLocationDirectory(@NotNull final TableKey tableKey, @NotNull final TableLocationKey locationKey) {
        if (TableLocationKey.COMPARATOR.compare(locationKey, SimpleTableLocationKey.getInstance()) != 0) {
            throw new UnsupportedOperationException(this + ": Unsupported location key: " + locationKey);
        }
        return tableDirectory;
    }
}
