//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.engine.table.impl.TableUpdateMode;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.TableLocationFactory;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.jetbrains.annotations.NotNull;

/**
 * <p>
 * Static {@link TableLocationProvider} implementation that delegates {@link TableLocationKey location key} discovery to
 * a {@link TableLocationKeyFinder} and {@link TableLocation location} creation to a {@link TableLocationFactory}.
 * </p>
 */
public class IcebergStaticTableLocationProvider<TK extends TableKey, TLK extends TableLocationKey>
        extends IcebergTableLocationProviderBase<TK, TLK> {

    private static final String IMPLEMENTATION_NAME = IcebergStaticTableLocationProvider.class.getSimpleName();

    public IcebergStaticTableLocationProvider(
            @NotNull final TK tableKey,
            @NotNull final IcebergBaseLayout locationKeyFinder,
            @NotNull final TableLocationFactory<TK, TLK> locationFactory,
            @NotNull final TableIdentifier tableIdentifier) {
        super(tableKey,
                locationKeyFinder,
                locationFactory,
                false,
                null,
                tableIdentifier,
                TableUpdateMode.STATIC, // The set of locations is static
                TableUpdateMode.STATIC // Individual locations cannot add or remove rows
        );
    }

    // ------------------------------------------------------------------------------------------------------------------
    // AbstractTableLocationProvider implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    protected synchronized void doInitialization() {
        if (!isInitialized()) {
            refresh();
        }
    }

    @Override
    public synchronized void refresh() {
        beginTransaction(this);
        locationKeyFinder.findKeys(tlk -> handleTableLocationKeyAdded(tlk, this));
        endTransaction(this);
        setInitialized();
    }

    @Override
    public void update() {
        throw new UnsupportedOperationException("A static table location provider cannot be updated");
    }

    @Override
    public void update(long snapshotId) {
        throw new UnsupportedOperationException("A static table location provider cannot be updated");
    }

    @Override
    public void update(Snapshot snapshot) {
        throw new UnsupportedOperationException("A static table location provider cannot be updated");
    }
}
