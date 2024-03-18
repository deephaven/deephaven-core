//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.locations;

import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.engine.table.impl.locations.impl.StandaloneTableKey;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import org.jetbrains.annotations.NotNull;

public final class TableBackedTableLocation extends AbstractTableLocation {

    private final UpdateSourceRegistrar registrar;

    private Runnable token;

    public TableBackedTableLocation(
            @NotNull final UpdateSourceRegistrar registrar,
            @NotNull TableBackedTableLocationKey tableLocationKey) {
        super(StandaloneTableKey.getInstance(), tableLocationKey, tableLocationKey.table.isRefreshing());
        this.registrar = registrar;
    }

    public QueryTable table() {
        return ((TableBackedTableLocationKey) getKey()).table;
    }

    @Override
    protected void activateUnderlyingDataSource() {
        registrar.addSource(token = this::refresh); // handleUpdate ignores "unchanged" state
        refresh();
        activationSuccessful(token);
    }

    @Override
    protected void deactivateUnderlyingDataSource() {
        registrar.removeSource(token);
        token = null;
    }

    @Override
    protected <T> boolean matchSubscriptionToken(final T token) {
        return token == this.token;
    }

    @Override
    public void refresh() {
        if (table().isFailed()) {
            if (token == null) {
                throw new TableDataException("Can't refresh from a failed table");
            } else {
                activationFailed(token, new TableDataException("Can't maintain subscription to a failed table"));
            }
        } else {
            handleUpdate(table().getRowSet().copy(), -1L);
        }
    }

    @Override
    protected @NotNull ColumnLocation makeColumnLocation(@NotNull final String name) {
        return new TableBackedColumnLocation(this, name);
    }
}
