//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.locations;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateMode;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.StandaloneTableKey;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public final class TableBackedTableLocationProvider extends AbstractTableLocationProvider {

    public static final String LOCATION_ID_ATTR = "ID";
    private final UpdateSourceRegistrar registrar;

    private final String callSite;

    private final List<TableBackedTableLocationKey> pending = new ArrayList<>();
    private final AtomicInteger nextId = new AtomicInteger();

    public TableBackedTableLocationProvider(
            @NotNull final UpdateSourceRegistrar registrar,
            final boolean supportsSubscriptions,
            final TableUpdateMode updateMode,
            final TableUpdateMode locationUpdateMode,
            @NotNull final Table... tables) {
        super(StandaloneTableKey.getInstance(), supportsSubscriptions, updateMode, locationUpdateMode);
        this.registrar = registrar;

        callSite = QueryPerformanceRecorder.getCallerLine();

        for (final Table table : tables) {
            add(table);
        }
    }

    private TableBackedTableLocationKey makeTableLocationKey(
            @NotNull final Table table,
            @Nullable final Map<String, Comparable<?>> partitions) {
        final boolean needToClearCallsite = QueryPerformanceRecorder.setCallsite(callSite);
        final QueryTable coalesced = (QueryTable) table.coalesce();
        Assert.assertion(coalesced.isAppendOnly(), "table is append only");
        final QueryTable withId =
                (QueryTable) coalesced.withAttributes(Map.of(LOCATION_ID_ATTR, nextId.getAndIncrement()));
        if (needToClearCallsite) {
            QueryPerformanceRecorder.clearCallsite();
        }
        return new TableBackedTableLocationKey(partitions, withId);
    }

    /**
     * Enqueue a table that belongs to the supplied {@code partitions} to be added upon the next {@link #refresh()
     * refresh}.
     * 
     * @param toAdd The {@link Table} to add
     * @param partitions The partitions the newly-added table-backed location belongs to
     */
    public void addPending(
            @NotNull final Table toAdd,
            @Nullable final Map<String, Comparable<?>> partitions) {
        final TableBackedTableLocationKey tlk = makeTableLocationKey(toAdd, partitions);
        synchronized (pending) {
            pending.add(tlk);
        }
    }

    /**
     * Enqueue a table that belongs to no partitions to be added upon the next {@link #refresh() refresh}.
     *
     * @param toAdd The {@link Table} to add
     */
    public void addPending(@NotNull final Table toAdd) {
        addPending(toAdd, null);
    }

    private void processPending() {
        synchronized (pending) {
            if (pending.isEmpty()) {
                return;
            }
            pending.forEach(this::handleTableLocationKeyAdded);
            pending.clear();
        }
    }

    /**
     * Add a table that belongs to the supplied {@code partitions}.
     *
     * @param toAdd The {@link Table} to add
     * @param partitions The partitions the newly-added table-backed location belongs to
     */
    public void add(
            @NotNull final Table toAdd,
            @Nullable final Map<String, Comparable<?>> partitions) {
        handleTableLocationKeyAdded(makeTableLocationKey(toAdd, partitions));
    }

    /**
     * Add a table that belongs to no partitionns.
     *
     * @param toAdd The {@link Table} to add
     */
    public void add(@NotNull final Table toAdd) {
        add(toAdd, null);
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
        processPending();
    }

    @Override
    protected @NotNull TableLocation makeTableLocation(@NotNull TableLocationKey locationKey) {
        return new TableBackedTableLocation(registrar, (TableBackedTableLocationKey) locationKey);
    }
}
