//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.locations;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.StandaloneTableKey;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public final class TableBackedTableLocationProvider extends AbstractTableLocationProvider {

    public static final String LOCATION_ID_ATTR = "ID";
    private final UpdateSourceRegistrar registrar;

    private final List<Table> pending = new ArrayList<>();
    private final MutableInt nextId = new MutableInt();

    public TableBackedTableLocationProvider(
            @NotNull final UpdateSourceRegistrar registrar,
            final boolean supportsSubscriptions,
            @NotNull final Table... tables) {
        super(StandaloneTableKey.getInstance(), supportsSubscriptions);
        this.registrar = registrar;
        processPending(Arrays.stream(tables));
    }

    private void processPending(@NotNull final Stream<Table> tableStream) {
        tableStream
                .map(table -> (QueryTable) table.coalesce()
                        .withAttributes(Map.of(LOCATION_ID_ATTR, nextId.getAndIncrement())))
                .peek(table -> Assert.assertion(table.isAppendOnly(), "table is append only"))
                .map(TableBackedTableLocationKey::new)
                .forEach(this::handleTableLocationKey);
    }

    public synchronized void addPending(@NotNull final Table toAdd) {
        pending.add(toAdd);
    }

    @Override
    protected void activateUnderlyingDataSource() {
        activationSuccessful(this);
    }

    @Override
    protected <T> boolean matchSubscriptionToken(T token) {
        return token == this;
    }

    @Override
    protected void deactivateUnderlyingDataSource() {}

    @Override
    public void refresh() {
        if (pending.isEmpty()) {
            return;
        }

        processPending(pending.stream());
        pending.clear();
    }

    @Override
    protected @NotNull TableLocation makeTableLocation(@NotNull TableLocationKey locationKey) {
        return new TableBackedTableLocation(registrar, (TableBackedTableLocationKey) locationKey);
    }
}
