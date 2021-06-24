/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableRegistrar;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.locations.TableLocationProvider;
import io.deephaven.db.v2.select.*;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A source table that can filter partitions before coalescing.
 */
public class PartitionAwareSourceTable extends SourceTable {

    public static final Set<String> ALL_INTERNAL_PARTITIONS = Collections.emptySet();

    private final Set<String> internalPartitions;
    private final ColumnDefinition<String> partitioningColumnDefinition;
    private final SelectFilter[] partitioningColumnFilters;

    /**
     * @param tableDefinition    A TableDefinition
     * @param description        A human-readable description for this table
     * @param componentFactory   A component factory for creating column source managers
     * @param locationProvider   A TableLocationProvider, for use in discovering the locations that compose this table
     * @param liveTableRegistrar Callback for registering live tables for refreshes, null if this table is not live
     * @param internalPartitions Allowed internal partitions - use ALL_INTERNAL_PARTITIONS (or an empty set) for no restrictions
     */
    PartitionAwareSourceTable(TableDefinition tableDefinition,
                              String description,
                              SourceTableComponentFactory componentFactory,
                              TableLocationProvider locationProvider,
                              LiveTableRegistrar liveTableRegistrar,
                              Set<String> internalPartitions) {
        //noinspection unchecked
        this(tableDefinition,
                description,
                componentFactory,
                locationProvider,
                liveTableRegistrar,
                internalPartitions,
                tableDefinition.getPartitioningColumns().get(0));
    }

    PartitionAwareSourceTable(TableDefinition tableDefinition,
                              String description,
                              SourceTableComponentFactory componentFactory,
                              TableLocationProvider locationProvider,
                              LiveTableRegistrar liveTableRegistrar,
                              Set<String> internalPartitions,
                              ColumnDefinition<String> partitioningColumnDefinition,
                              SelectFilter... partitioningColumnFilters) {
        super(tableDefinition, description, componentFactory, locationProvider, liveTableRegistrar);
        this.internalPartitions = internalPartitions;
        this.partitioningColumnDefinition = partitioningColumnDefinition;
        this.partitioningColumnFilters = partitioningColumnFilters;
    }

    protected PartitionAwareSourceTable newInstance(TableDefinition tableDefinition,
                                                    String description,
                                                    SourceTableComponentFactory componentFactory,
                                                    TableLocationProvider locationProvider,
                                                    LiveTableRegistrar liveTableRegistrar,
                                                    Set<String> internalPartitions,
                                                    ColumnDefinition<String> partitioningColumnDefinition,
                                                    SelectFilter... partitioningColumnFilters) {
        return new PartitionAwareSourceTable(tableDefinition, description, componentFactory, locationProvider, liveTableRegistrar, internalPartitions, partitioningColumnDefinition, partitioningColumnFilters);
    }

    private PartitionAwareSourceTable getFilteredTable(SelectFilter... additionalPartitioningColumnFilters) {
        SelectFilter[] resultPartitioningColumnFilters = new SelectFilter[partitioningColumnFilters.length + additionalPartitioningColumnFilters.length];
        System.arraycopy(partitioningColumnFilters, 0, resultPartitioningColumnFilters, 0, partitioningColumnFilters.length);
        System.arraycopy(additionalPartitioningColumnFilters, 0, resultPartitioningColumnFilters, partitioningColumnFilters.length, additionalPartitioningColumnFilters.length);
        return newInstance(definition,
                description + ".where(" + Arrays.deepToString(additionalPartitioningColumnFilters) + ')',
                componentFactory, locationProvider, liveTableRegistrar, internalPartitions, partitioningColumnDefinition, resultPartitioningColumnFilters);
    }

    private static class PartitionAwareQueryTableReference extends QueryTableReference {

        private PartitionAwareQueryTableReference(PartitionAwareSourceTable table) {
            super(table);
        }

        @Override
        public TableAndRemainingFilters getWithWhere(SelectFilter... selectFilters) {
            ArrayList<SelectFilter> partitionFilters = new ArrayList<>();
            ArrayList<SelectFilter> groupFilters = new ArrayList<>();
            ArrayList<SelectFilter> otherFilters = new ArrayList<>();

            List<ColumnDefinition> groupingColumns = table.getDefinition().getGroupingColumns();
            Set<String> groupingColumnNames = groupingColumns.stream().map(ColumnDefinition::getName).collect(Collectors.toSet());

            for (SelectFilter filter : selectFilters) {
                filter.init(table.definition);
                List<String> columns = filter.getColumns();
                if (filter instanceof ReindexingFilter) {
                    otherFilters.add(filter);
                } else if (((PartitionAwareSourceTable)table).isValidAgainstColumnPartitionTable(columns, filter.getColumnArrays())) {
                    partitionFilters.add(filter);
                } else if (filter.isSimpleFilter() && (columns.size() == 1) && (groupingColumnNames.contains(columns.get(0)))) {
                    groupFilters.add(filter);
                } else {
                    otherFilters.add(filter);
                }
            }

            final Table result = partitionFilters.isEmpty() ? table.coalesce() : table.where(partitionFilters.toArray(SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY));

            // put the other filters onto the end of the grouping filters, this means that the group filters should
            // go first, which should be preferable to having them second.  This is basically the first query
            // optimization that we're doing for the user, so maybe it is a good thing but maybe not.  The reason we do
            // it, is that we have deferred the filters for the users permissions, and they did not have the opportunity
            // to properly filter the data yet at this point.
            groupFilters.addAll(otherFilters);

            return new TableAndRemainingFilters(result, groupFilters.toArray(SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY));
        }

        @Override
        public Table selectDistinct(SelectColumn[] selectColumns) {
            for(final SelectColumn selectColumn : selectColumns) {
                try {
                    selectColumn.initDef(getDefinition().getColumnNameMap());
                } catch (Exception e) {
                    return null;
                }
                if (!((PartitionAwareSourceTable) table).isValidAgainstColumnPartitionTable(selectColumn.getColumns(), selectColumn.getColumnArrays())) {
                    return null;
                }
            }

            return table.selectDistinct(selectColumns);
        }
    }

    @Override
    protected final BaseTable redefine(TableDefinition newDefinition) {
        if(newDefinition.getColumnNames().equals(definition.getColumnNames())) {
            // Nothing changed - we have the same columns in the same order.
            return this;
        }
        if(newDefinition.getColumns().length == definition.getColumns().length || newDefinition.getPartitioningColumns().size() == 1) {
            // Nothing changed except ordering, *or* some columns were dropped but the partitioning column was retained.
            return newInstance(newDefinition,
                    description + "-retainColumns",
                    componentFactory, locationProvider, liveTableRegistrar, internalPartitions, partitioningColumnDefinition, partitioningColumnFilters);
        }
        // The partitioning column is gone - defer dropping it.
        List<ColumnDefinition> newColumnDefinitions = new ArrayList<>(newDefinition.getColumnList());
        newColumnDefinitions.add(partitioningColumnDefinition);
        PartitionAwareSourceTable redefined = newInstance(new TableDefinition(newColumnDefinitions),
                description + "-retainColumns",
                componentFactory, locationProvider, liveTableRegistrar, internalPartitions, partitioningColumnDefinition, partitioningColumnFilters);
        DeferredViewTable deferredViewTable = new DeferredViewTable(newDefinition, description + "-retainColumns", new PartitionAwareQueryTableReference(redefined), new String[]{partitioningColumnDefinition.getName()}, null, null);
        deferredViewTable.setRefreshing(isRefreshing());
        return deferredViewTable;
    }

    @Override
    protected final Table redefine(TableDefinition newDefinitionExternal, TableDefinition newDefinitionInternal, SelectColumn[] viewColumns, Map<String, Set<String>> columnDependency) {
        BaseTable redefined = redefine(newDefinitionInternal);
        DeferredViewTable.TableReference reference = redefined instanceof PartitionAwareSourceTable
                ? new PartitionAwareQueryTableReference((PartitionAwareSourceTable)redefined)
                : new DeferredViewTable.SimpleTableReference(redefined);
        DeferredViewTable deferredViewTable = new DeferredViewTable(newDefinitionExternal, description + "-redefined", reference, null, viewColumns, null);
        deferredViewTable.setRefreshing(isRefreshing());
        return deferredViewTable;
    }

    @Override
    protected final Collection<TableLocation> filterLocations(@NotNull final Collection<TableLocation> foundLocations) {
        return filterLocationsByColumnPartition(filterLocationsByInternalPartition(foundLocations));
    }

    private Collection<TableLocation> filterLocationsByInternalPartition(@NotNull final Collection<TableLocation> foundLocations) {
        if(internalPartitions.isEmpty()) {
            return foundLocations;
        }
        return foundLocations.stream().filter(l -> internalPartitions.contains(l.getInternalPartition().toString())).collect(Collectors.toList());
    }

    private static final String LOCATION_COLUMN_NAME = "__PartitionAwareSourceTable_TableLocation";

    private Collection<TableLocation> filterLocationsByColumnPartition(@NotNull final Collection<TableLocation> foundLocations) {
        if(partitioningColumnFilters.length == 0) {
            return foundLocations;
        }
        final String[] columnPartitions = foundLocations.stream().map(l -> l.getColumnPartition().toString()).toArray(String[]::new);
        //noinspection unchecked
        Table filteredColumnPartitionTable = TableTools.newTable(
                columnPartitions.length,
                Arrays.asList(partitioningColumnDefinition.getName(), LOCATION_COLUMN_NAME),
                Arrays.asList(
                        ArrayBackedColumnSource.getMemoryColumnSourceUntyped(columnPartitions),
                        ArrayBackedColumnSource.getMemoryColumnSource(foundLocations, TableLocation.class, null)))
                .where(partitioningColumnFilters);
        if(filteredColumnPartitionTable.size() == columnPartitions.length) {
            return foundLocations;
        }
        final Iterable<TableLocation> iterable = () -> filteredColumnPartitionTable.columnIterator(LOCATION_COLUMN_NAME);
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
    }

    @Override
    public final Table where(SelectFilter... filters) {
        if (filters.length == 0) {
            return QueryPerformanceRecorder.withNugget(description + ".coalesce()", this::coalesce);
        }

        ArrayList<SelectFilter> partitionFilters = new ArrayList<>();
        ArrayList<SelectFilter> groupFilters = new ArrayList<>();
        ArrayList<SelectFilter> otherFilters = new ArrayList<>();

        List<ColumnDefinition> groupingColumns = definition.getGroupingColumns();
        Set<String> groupingColumnNames = groupingColumns.stream().map(ColumnDefinition::getName).collect(Collectors.toSet());

        for (SelectFilter filter : filters) {
            filter.init(definition);
            List<String> columns = filter.getColumns();
            if (filter instanceof ReindexingFilter) {
                otherFilters.add(filter);
            } else if (isValidAgainstColumnPartitionTable(columns, filter.getColumnArrays())) {
                partitionFilters.add(filter);
            } else if (filter.isSimpleFilter() && (columns.size() == 1) && (groupingColumnNames.contains(columns.get(0)))) {
                groupFilters.add(filter);
            } else {
                otherFilters.add(filter);
            }
        }

        // if there was nothing that actually required the partition, defer the result.  This is different than V1, and
        // is actually different than the old behavior as well.
        if (partitionFilters.isEmpty()) {
            DeferredViewTable deferredViewTable = new DeferredViewTable(definition, description + "-withDeferredFilters", new PartitionAwareQueryTableReference(this), null, null, filters);
            deferredViewTable.setRefreshing(isRefreshing());
            return deferredViewTable;
        }

        SelectFilter[] partitionFilterArray = partitionFilters.toArray(new SelectFilter[partitionFilters.size()]);
        final String filteredTableDescription = "getFilteredTable(" + Arrays.toString(partitionFilterArray) + ")";
        SourceTable filteredTable = QueryPerformanceRecorder.withNugget(filteredTableDescription, () -> getFilteredTable(partitionFilterArray));

        copyAttributes(filteredTable, CopyAttributeOperation.Filter);

        // Apply the group filters before other filters.
        groupFilters.addAll(otherFilters);

        if (groupFilters.isEmpty()) {
            return QueryPerformanceRecorder.withNugget(description + filteredTableDescription + ".coalesce()", filteredTable::coalesce);
        }

        return QueryPerformanceRecorder.withNugget(description + ".coalesce().where(" + groupFilters + ")", () -> filteredTable.coalesce().where(groupFilters.toArray(SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY)));
    }

    @Override
    public final Table selectDistinct(SelectColumn... columns) {
        for (SelectColumn selectColumn : columns) {
            selectColumn.initDef(definition.getColumnNameMap());
            if (!isValidAgainstColumnPartitionTable(selectColumn.getColumns(), selectColumn.getColumnArrays())) {
                return super.selectDistinct(columns);
            }
        }
        initializeAvailableLocations();
        final String[] columnPartitions = columnSourceManager.allLocations().stream().filter(l -> {
            l.refresh();
            final long size = l.getSize();
            return size != TableLocation.NULL_SIZE && size > 0;
        }).map(TableLocation::getColumnPartition).toArray(String[]::new);
        return TableTools.newTable(TableTools.col(partitioningColumnDefinition.getName(), columnPartitions)).selectDistinct(columns);
        // TODO: Refactor the above to listen for new locations, and for size changes to non-existent or 0-size locations.
        // TODO: Maybe just get rid of this implementation and coalesce? Partitioning columns are automatically grouped.  Needs lazy region allocation.
    }

    private boolean isValidAgainstColumnPartitionTable(List<String> columnNames, List<String> columnArrayNames) {
        return columnNames.size() == 1 && columnNames.get(0).equals(partitioningColumnDefinition.getName()) && columnArrayNames.size() <= 0;
    }
}
