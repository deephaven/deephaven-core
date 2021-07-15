package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.*;
import io.deephaven.db.v2.locations.util.TableDataRefreshService;
import io.deephaven.db.v2.parquet.ParquetInstructions;
import io.deephaven.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.io.File;

// TODO-RWC: Get rid of deferred locations. Operate on keys until coalesce. Do keys have a map of partitioning info, or do we build it outside?

public class ReadOnlyLocalTableLocationProviderByParquetFile extends AbstractTableLocationProvider {

    private final File fileLocation;
    private final ParquetInstructions readInstructions;

    public ReadOnlyLocalTableLocationProviderByParquetFile(
            @NotNull final TableKey tableKey,
            @NotNull final File fileLocation,
            @NotNull final ParquetInstructions readInstructions) {
        super(tableKey, false);
        this.fileLocation = fileLocation;
        this.readInstructions = readInstructions;
    }

    @Override
    public String getImplementationName() {
        return ReadOnlyLocalTableLocationProviderByParquetFile.class.getSimpleName();
    }

    @Override
    public void refresh() {
        handleTableLocationKey(StandaloneTableLocationKey.getInstance());
        setInitialized();
    }

    @Override
    @NotNull
    protected final TableLocation makeTableLocation(@NotNull final TableLocationKey locationKey) {
        return new ReadOnlyParquetTableLocation(StandaloneTableKey.getInstance(), locationKey, fileLocation, false, readInstructions);
    }
}
