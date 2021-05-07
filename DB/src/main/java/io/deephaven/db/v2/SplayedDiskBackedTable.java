/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableRegistrar;
import io.deephaven.db.v2.locations.TableLocationProvider;
import org.jetbrains.annotations.NotNull;

/**
 * Single-location simple source table.
 */
public class SplayedDiskBackedTable extends SimpleSourceTable {

    /**
     * @param tableDefinition    A TableDefinition (must have storageType == STORAGETYPE_SPLAYEDONDISK)
     * @param componentFactory   A component factory for creating column source managers
     * @param locationProvider   A TableLocationProvider, for use in discovering the locations that compose this table
     * @param liveTableRegistrar Callback for registering live tables for refreshes, null if this table is not live
     */
    SplayedDiskBackedTable(TableDefinition tableDefinition,
                           SourceTableComponentFactory componentFactory,
                           TableLocationProvider locationProvider,
                           LiveTableRegistrar liveTableRegistrar) {
        this(tableDefinition,
                "SplayedDiskBackedTable[" + tableDefinition.getNamespace() + ',' + tableDefinition.getName() + ']',
                componentFactory,
                locationProvider,
                liveTableRegistrar,
                true);
    }

    private SplayedDiskBackedTable(TableDefinition tableDefinition,
                                   String description,
                                   SourceTableComponentFactory componentFactory,
                                   TableLocationProvider locationProvider,
                                   LiveTableRegistrar liveTableRegistrar,
                                   boolean checkDefinition) {
        super(checkDefinition ? checkTableDefinitionRequirements(tableDefinition) : tableDefinition,
                description,
                componentFactory,
                locationProvider,
                liveTableRegistrar);
    }

    @Override
    protected SplayedDiskBackedTable newInstance(TableDefinition tableDefinition,
                                                 String description,
                                                 SourceTableComponentFactory componentFactory,
                                                 TableLocationProvider locationProvider,
                                                 LiveTableRegistrar liveTableRegistrar) {
        return new SplayedDiskBackedTable(tableDefinition, description, componentFactory, locationProvider, liveTableRegistrar, false);
    }

    private static TableDefinition checkTableDefinitionRequirements(@NotNull final TableDefinition tableDefinition) {
        Require.neqNull(tableDefinition, "tableDefinition");
        if (tableDefinition.getStorageType() != TableDefinition.STORAGETYPE_SPLAYEDONDISK) {
            throw new IllegalArgumentException("Expected storage type of " + TableDefinition.STORAGE_TYPE_FORMATTER.format(TableDefinition.STORAGETYPE_SPLAYEDONDISK) +
                    ", instead found " + TableDefinition.STORAGE_TYPE_FORMATTER.format(tableDefinition.getStorageType()));
        }
        Require.eq(tableDefinition.getStorageType(), "tableDefinition.getStorageType()", TableDefinition.STORAGETYPE_SPLAYEDONDISK, "Splayed");
        int numberOfPartitioningColumns = 0;
        for(int ci = 0; ci < tableDefinition.getColumns().length; ++ci) {
            switch(tableDefinition.getColumns()[ci].getColumnType()) {
                case ColumnDefinition.COLUMNTYPE_PARTITIONING:
                    ++numberOfPartitioningColumns;
                    break;
                case ColumnDefinition.COLUMNTYPE_GROUPING:
                case ColumnDefinition.COLUMNTYPE_NORMAL:
                    break;
                case ColumnDefinition.COLUMNTYPE_VIRTUAL:
                default:
                    throw new UnsupportedOperationException();
            }
        }
        Require.eqZero(numberOfPartitioningColumns, "numberOfPartitioningColumns");
        return tableDefinition;
    }
}
