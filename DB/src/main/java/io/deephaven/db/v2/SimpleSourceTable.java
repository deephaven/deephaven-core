/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableRegistrar;
import io.deephaven.db.v2.locations.TableLocationProvider;
import io.deephaven.db.v2.select.SelectColumn;

import java.util.Map;
import java.util.Set;

/**
 * Simple source table with no partitioning support.
 */
public class SimpleSourceTable extends SourceTable {

    /**
     * @param tableDefinition A TableDefinition
     * @param description A human-readable description for this table
     * @param componentFactory A component factory for creating column source managers
     * @param locationProvider A TableLocationProvider, for use in discovering the locations that compose this table
     * @param liveTableRegistrar Callback for registering live tables for refreshes, null if this table is not live
     */
    public SimpleSourceTable(TableDefinition tableDefinition,
            String description,
            SourceTableComponentFactory componentFactory,
            TableLocationProvider locationProvider,
            LiveTableRegistrar liveTableRegistrar) {
        super(tableDefinition, description, componentFactory, locationProvider, liveTableRegistrar);
    }

    protected SimpleSourceTable newInstance(TableDefinition tableDefinition,
            String description,
            SourceTableComponentFactory componentFactory,
            TableLocationProvider locationProvider,
            LiveTableRegistrar liveTableRegistrar) {
        return new SimpleSourceTable(tableDefinition, description, componentFactory, locationProvider,
                liveTableRegistrar);
    }

    @Override
    protected final SourceTable redefine(TableDefinition newDefinition) {
        if (newDefinition.getColumnNames().equals(definition.getColumnNames())) {
            // Nothing changed - we have the same columns in the same order.
            return this;
        }
        return newInstance(newDefinition, description + "-retainColumns", componentFactory, locationProvider,
                liveTableRegistrar);
    }

    @Override
    protected final Table redefine(TableDefinition newDefinitionExternal, TableDefinition newDefinitionInternal,
            SelectColumn[] viewColumns, Map<String, Set<String>> columnDependency) {
        DeferredViewTable deferredViewTable = new DeferredViewTable(newDefinitionExternal, description + "-redefined",
                new QueryTableReference(redefine(newDefinitionInternal)), new String[0], viewColumns, null);
        deferredViewTable.setRefreshing(isRefreshing());
        return deferredViewTable;
    }
}
