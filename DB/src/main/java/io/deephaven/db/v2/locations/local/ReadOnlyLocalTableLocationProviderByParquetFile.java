package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.*;
import io.deephaven.db.v2.locations.util.TableDataRefreshService;
import io.deephaven.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.io.File;

public class ReadOnlyLocalTableLocationProviderByParquetFile extends LocalTableLocationProvider {

    private final File fileLocation;

    public ReadOnlyLocalTableLocationProviderByParquetFile(
            @NotNull final TableKey tableKey,
            @NotNull final File fileLocation,
            final boolean supportsSubscriptions,
            @NotNull final TableDataRefreshService refreshService) {
        super(tableKey, supportsSubscriptions, refreshService);
        this.fileLocation = fileLocation;
    }

    @Override
    public String getImplementationName() {
        return ReadOnlyLocalTableLocationProviderByParquetFile.class.getSimpleName();
    }

    @Override
    public void refresh() {
        handleTableLocationKey(SimpleTableLocationKey.getInstance());
        setInitialized();
    }

    @Override
    @NotNull
    protected final TableLocation<?> makeTableLocation(@NotNull final TableLocationKey locationKey) {
        return new DeferredTableLocation.DataDriven<>(getTableKey(), locationKey, this::makeLocation);
    }

    private TableLocation<?> makeLocation(@NotNull final TableKey tableKey, @NotNull final TableLocationKey tableLocationKey) {
        if (Utils.fileExistsPrivileged(fileLocation)) {
            return new ReadOnlyParquetTableLocation(tableKey, tableLocationKey, fileLocation, supportsSubscriptions());
        } else {
            throw new UnsupportedOperationException(this + ": Unrecognized data format in location " + tableLocationKey);
        }
    }
}
