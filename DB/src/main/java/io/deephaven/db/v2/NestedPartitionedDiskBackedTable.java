/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableRegistrar;
import io.deephaven.db.v2.locations.TableLocationProvider;
import io.deephaven.db.v2.select.SelectFilter;
import io.deephaven.util.annotations.VisibleForTesting;

import java.util.Set;

/**
 * Partition-aware source table with support for a single internal partition and a single column partition.
 */
public class NestedPartitionedDiskBackedTable extends PartitionAwareSourceTable {

    /**
     * @param tableDefinition    A TableDefinition (ust have storageType == STORAGETYPE_NESTEDPARTITIONEDONDISK)
     * @param componentFactory   A component factory for creating column source managers
     * @param locationProvider   A TableLocationProvider, for use in discovering the locations that compose this table
     * @param liveTableRegistrar Callback for registering live tables for refreshes, null if this table is not live
     * @param internalPartitions Allowed internal partitions - use ALL_INTERNAL_PARTITIONS (or an empty set) for no restrictions
     */
    @VisibleForTesting
    public NestedPartitionedDiskBackedTable(TableDefinition tableDefinition,
                                            SourceTableComponentFactory componentFactory,
                                            TableLocationProvider locationProvider,
                                            LiveTableRegistrar liveTableRegistrar,
                                            Set<String> internalPartitions) {
        //noinspection unchecked
        super(checkTableDefinitionRequirements(tableDefinition),
                NestedPartitionedDiskBackedTable.class.getSimpleName() + '[' + locationProvider + ']',
                componentFactory,
                locationProvider,
                liveTableRegistrar,
                internalPartitions);
    }

    private NestedPartitionedDiskBackedTable(TableDefinition tableDefinition,
                                             String description,
                                             SourceTableComponentFactory componentFactory,
                                             TableLocationProvider locationProvider,
                                             LiveTableRegistrar liveTableRegistrar,
                                             Set<String> internalPartitions,
                                             ColumnDefinition<String> partitioningColumnDefinition,
                                             SelectFilter... partitioningColumnFilters) {
        super(tableDefinition, description, componentFactory, locationProvider, liveTableRegistrar, internalPartitions, partitioningColumnDefinition, partitioningColumnFilters);
    }

    @Override
    protected NestedPartitionedDiskBackedTable newInstance(TableDefinition tableDefinition,
                                                           String description,
                                                           SourceTableComponentFactory componentFactory,
                                                           TableLocationProvider locationProvider,
                                                           LiveTableRegistrar liveTableRegistrar,
                                                           Set<String> internalPartitions,
                                                           ColumnDefinition<String> partitioningColumnDefinition,
                                                           SelectFilter... partitioningColumnFilters) {
        return new NestedPartitionedDiskBackedTable(tableDefinition, description, componentFactory, locationProvider, liveTableRegistrar, internalPartitions, partitioningColumnDefinition, partitioningColumnFilters);
    }

    private static TableDefinition checkTableDefinitionRequirements(TableDefinition tableDefinition) {
        Require.neqNull(tableDefinition, "tableDefinition");
        int numberOfPartitioningColumns = 0;
        for (int ci = 0; ci < tableDefinition.getColumns().length; ++ci) {
            switch (tableDefinition.getColumns()[ci].getColumnType()) {
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
        Require.eq(numberOfPartitioningColumns, "numberOfPartitioningColumns", 1);
        return tableDefinition;
    }
}
