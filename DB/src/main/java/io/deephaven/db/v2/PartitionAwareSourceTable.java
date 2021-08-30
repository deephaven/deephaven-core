/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableRegistrar;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.locations.ImmutableTableLocationKey;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.TableLocationProvider;
import io.deephaven.db.v2.select.*;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A source table that can filter partitions before coalescing. Refer to {@link TableLocationKey} for an explanation of
 * partitioning.
 */
public class PartitionAwareSourceTable extends SourceTable {

    private final Map<String, ColumnDefinition<?>> partitioningColumnDefinitions;
    private final SelectFilter[] partitioningColumnFilters;

    /**
     * @param tableDefinition A TableDefinition
     * @param description A human-readable description for this table
     * @param componentFactory A component factory for creating column source managers
     * @param locationProvider A TableLocationProvider, for use in discovering the locations that compose this table
     * @param liveTableRegistrar Callback for registering live tables for refreshes, null if this table is not live
     */
    public PartitionAwareSourceTable(@NotNull final TableDefinition tableDefinition,
            @NotNull final String description,
            @NotNull final SourceTableComponentFactory componentFactory,
            @NotNull final TableLocationProvider locationProvider,
            @Nullable final LiveTableRegistrar liveTableRegistrar) {
        this(tableDefinition,
                description,
                componentFactory,
                locationProvider,
                liveTableRegistrar,
                extractPartitioningColumnDefinitions(tableDefinition));
    }

    PartitionAwareSourceTable(@NotNull final TableDefinition tableDefinition,
            @NotNull final String description,
            @NotNull final SourceTableComponentFactory componentFactory,
            @NotNull final TableLocationProvider locationProvider,
            @Nullable final LiveTableRegistrar liveTableRegistrar,
            @NotNull final Map<String, ColumnDefinition<?>> partitioningColumnDefinitions,
            @Nullable final SelectFilter... partitioningColumnFilters) {
        super(tableDefinition, description, componentFactory, locationProvider, liveTableRegistrar);
        this.partitioningColumnDefinitions = partitioningColumnDefinitions;
        this.partitioningColumnFilters = partitioningColumnFilters;
    }

    protected PartitionAwareSourceTable newInstance(@NotNull final TableDefinition tableDefinition,
            @NotNull final String description,
            @NotNull final SourceTableComponentFactory componentFactory,
            @NotNull final TableLocationProvider locationProvider,
            @Nullable final LiveTableRegistrar liveTableRegistrar,
            @NotNull final Map<String, ColumnDefinition<?>> partitioningColumnDefinitions,
            @Nullable final SelectFilter... partitioningColumnFilters) {
        return new PartitionAwareSourceTable(tableDefinition, description, componentFactory, locationProvider,
                liveTableRegistrar, partitioningColumnDefinitions, partitioningColumnFilters);
    }

    private PartitionAwareSourceTable getFilteredTable(
            @NotNull final SelectFilter... additionalPartitioningColumnFilters) {
        SelectFilter[] resultPartitioningColumnFilters =
                new SelectFilter[partitioningColumnFilters.length + additionalPartitioningColumnFilters.length];
        System.arraycopy(partitioningColumnFilters, 0, resultPartitioningColumnFilters, 0,
                partitioningColumnFilters.length);
        System.arraycopy(additionalPartitioningColumnFilters, 0, resultPartitioningColumnFilters,
                partitioningColumnFilters.length, additionalPartitioningColumnFilters.length);
        return newInstance(definition,
                description + ".where(" + Arrays.deepToString(additionalPartitioningColumnFilters) + ')',
                componentFactory, locationProvider, liveTableRegistrar, partitioningColumnDefinitions,
                resultPartitioningColumnFilters);
    }

    private static Map<String, ColumnDefinition<?>> extractPartitioningColumnDefinitions(
            @NotNull final TableDefinition tableDefinition) {
        return tableDefinition.getColumnStream()
                .filter(ColumnDefinition::isPartitioning)
                .collect(Collectors.toMap(ColumnDefinition::getName, Function.identity(), Assert::neverInvoked,
                        LinkedHashMap::new));
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

            List<ColumnDefinition<?>> groupingColumns = table.getDefinition().getGroupingColumns();
            Set<String> groupingColumnNames =
                    groupingColumns.stream().map(ColumnDefinition::getName).collect(Collectors.toSet());

            for (SelectFilter filter : selectFilters) {
                filter.init(table.definition);
                List<String> columns = filter.getColumns();
                if (filter instanceof ReindexingFilter) {
                    otherFilters.add(filter);
                } else if (((PartitionAwareSourceTable) table).isValidAgainstColumnPartitionTable(columns,
                        filter.getColumnArrays())) {
                    partitionFilters.add(filter);
                } else if (filter.isSimpleFilter() && (columns.size() == 1)
                        && (groupingColumnNames.contains(columns.get(0)))) {
                    groupFilters.add(filter);
                } else {
                    otherFilters.add(filter);
                }
            }

            final Table result = partitionFilters.isEmpty() ? table.coalesce()
                    : table.where(partitionFilters.toArray(SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY));

            // put the other filters onto the end of the grouping filters, this means that the group filters should
            // go first, which should be preferable to having them second. This is basically the first query
            // optimization that we're doing for the user, so maybe it is a good thing but maybe not. The reason we do
            // it, is that we have deferred the filters for the users permissions, and they did not have the opportunity
            // to properly filter the data yet at this point.
            groupFilters.addAll(otherFilters);

            return new TableAndRemainingFilters(result,
                    groupFilters.toArray(SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY));
        }

        @Override
        public Table selectDistinct(SelectColumn[] selectColumns) {
            for (final SelectColumn selectColumn : selectColumns) {
                try {
                    selectColumn.initDef(getDefinition().getColumnNameMap());
                } catch (Exception e) {
                    return null;
                }
                if (!((PartitionAwareSourceTable) table).isValidAgainstColumnPartitionTable(selectColumn.getColumns(),
                        selectColumn.getColumnArrays())) {
                    return null;
                }
            }

            return table.selectDistinct(selectColumns);
        }
    }

    @Override
    protected final BaseTable redefine(@NotNull final TableDefinition newDefinition) {
        if (newDefinition.getColumnNames().equals(definition.getColumnNames())) {
            // Nothing changed - we have the same columns in the same order.
            return this;
        }
        if (newDefinition.getColumns().length == definition.getColumns().length
                || newDefinition.getPartitioningColumns().size() == partitioningColumnDefinitions.size()) {
            // Nothing changed except ordering, *or* some columns were dropped but the partitioning column was retained.
            return newInstance(newDefinition,
                    description + "-retainColumns",
                    componentFactory, locationProvider, liveTableRegistrar, partitioningColumnDefinitions,
                    partitioningColumnFilters);
        }
        // Some partitioning columns are gone - defer dropping them.
        final List<ColumnDefinition<?>> newColumnDefinitions = new ArrayList<>(newDefinition.getColumnList());
        final Map<String, ColumnDefinition<?>> retainedPartitioningColumnDefinitions =
                extractPartitioningColumnDefinitions(newDefinition);
        final Collection<ColumnDefinition<?>> droppedPartitioningColumnDefinitions =
                partitioningColumnDefinitions.values()
                        .stream().filter(cd -> !retainedPartitioningColumnDefinitions.containsKey(cd.getName()))
                        .collect(Collectors.toList());
        newColumnDefinitions.addAll(droppedPartitioningColumnDefinitions);
        final PartitionAwareSourceTable redefined = newInstance(new TableDefinition(newColumnDefinitions),
                description + "-retainColumns",
                componentFactory, locationProvider, liveTableRegistrar, partitioningColumnDefinitions,
                partitioningColumnFilters);
        final DeferredViewTable deferredViewTable = new DeferredViewTable(newDefinition, description + "-retainColumns",
                new PartitionAwareQueryTableReference(redefined),
                droppedPartitioningColumnDefinitions.stream().map(ColumnDefinition::getName).toArray(String[]::new),
                null, null);
        deferredViewTable.setRefreshing(isRefreshing());
        return deferredViewTable;
    }

    @Override
    protected final Table redefine(TableDefinition newDefinitionExternal, TableDefinition newDefinitionInternal,
            SelectColumn[] viewColumns, Map<String, Set<String>> columnDependency) {
        BaseTable redefined = redefine(newDefinitionInternal);
        DeferredViewTable.TableReference reference = redefined instanceof PartitionAwareSourceTable
                ? new PartitionAwareQueryTableReference((PartitionAwareSourceTable) redefined)
                : new DeferredViewTable.SimpleTableReference(redefined);
        DeferredViewTable deferredViewTable = new DeferredViewTable(newDefinitionExternal, description + "-redefined",
                reference, null, viewColumns, null);
        deferredViewTable.setRefreshing(isRefreshing());
        return deferredViewTable;
    }

    private static final String LOCATION_KEY_COLUMN_NAME = "__PartitionAwareSourceTable_TableLocationKey__";

    private static <T> ColumnSource<? super T> makePartitionSource(@NotNull final ColumnDefinition<T> columnDefinition,
            @NotNull final Collection<ImmutableTableLocationKey> locationKeys) {
        final Class<? super T> dataType = columnDefinition.getDataType();
        final String partitionKey = columnDefinition.getName();
        final WritableSource<? super T> result =
                ArrayBackedColumnSource.getMemoryColumnSource(locationKeys.size(), dataType, null);
        final MutableLong nextIndex = new MutableLong(0L);
        // noinspection unchecked
        locationKeys.stream()
                .map(lk -> (T) lk.getPartitionValue(partitionKey))
                .forEach((final T partitionValue) -> result.set(nextIndex.getAndIncrement(), partitionValue));
        return result;
    }

    @Override
    protected final Collection<ImmutableTableLocationKey> filterLocationKeys(
            @NotNull final Collection<ImmutableTableLocationKey> foundLocationKeys) {
        if (partitioningColumnFilters.length == 0) {
            return foundLocationKeys;
        }
        // TODO (https://github.com/deephaven/deephaven-core/issues/867): Refactor around a ticking partition table
        final List<String> partitionTableColumnNames = Stream.concat(
                partitioningColumnDefinitions.keySet().stream(),
                Stream.of(LOCATION_KEY_COLUMN_NAME)).collect(Collectors.toList());
        final List<ColumnSource<?>> partitionTableColumnSources =
                new ArrayList<>(partitioningColumnDefinitions.size() + 1);
        for (final ColumnDefinition<?> columnDefinition : partitioningColumnDefinitions.values()) {
            partitionTableColumnSources.add(makePartitionSource(columnDefinition, foundLocationKeys));
        }
        partitionTableColumnSources.add(ArrayBackedColumnSource.getMemoryColumnSource(foundLocationKeys,
                ImmutableTableLocationKey.class, null));
        final Table filteredColumnPartitionTable = TableTools
                .newTable(foundLocationKeys.size(), partitionTableColumnNames, partitionTableColumnSources)
                .where(partitioningColumnFilters);
        if (filteredColumnPartitionTable.size() == foundLocationKeys.size()) {
            return foundLocationKeys;
        }
        final Iterable<ImmutableTableLocationKey> iterable =
                () -> filteredColumnPartitionTable.columnIterator(LOCATION_KEY_COLUMN_NAME);
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

        List<ColumnDefinition<?>> groupingColumns = definition.getGroupingColumns();
        Set<String> groupingColumnNames =
                groupingColumns.stream().map(ColumnDefinition::getName).collect(Collectors.toSet());

        for (SelectFilter filter : filters) {
            filter.init(definition);
            List<String> columns = filter.getColumns();
            if (filter instanceof ReindexingFilter) {
                otherFilters.add(filter);
            } else if (isValidAgainstColumnPartitionTable(columns, filter.getColumnArrays())) {
                partitionFilters.add(filter);
            } else if (filter.isSimpleFilter() && (columns.size() == 1)
                    && (groupingColumnNames.contains(columns.get(0)))) {
                groupFilters.add(filter);
            } else {
                otherFilters.add(filter);
            }
        }

        // if there was nothing that actually required the partition, defer the result. This is different than V1, and
        // is actually different than the old behavior as well.
        if (partitionFilters.isEmpty()) {
            DeferredViewTable deferredViewTable =
                    new DeferredViewTable(definition, description + "-withDeferredFilters",
                            new PartitionAwareQueryTableReference(this), null, null, filters);
            deferredViewTable.setRefreshing(isRefreshing());
            return deferredViewTable;
        }

        SelectFilter[] partitionFilterArray = partitionFilters.toArray(SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY);
        final String filteredTableDescription = "getFilteredTable(" + Arrays.toString(partitionFilterArray) + ")";
        SourceTable filteredTable = QueryPerformanceRecorder.withNugget(filteredTableDescription,
                () -> getFilteredTable(partitionFilterArray));

        copyAttributes(filteredTable, CopyAttributeOperation.Filter);

        // Apply the group filters before other filters.
        groupFilters.addAll(otherFilters);

        if (groupFilters.isEmpty()) {
            return QueryPerformanceRecorder.withNugget(description + filteredTableDescription + ".coalesce()",
                    filteredTable::coalesce);
        }

        return QueryPerformanceRecorder.withNugget(description + ".coalesce().where(" + groupFilters + ")",
                () -> filteredTable.coalesce()
                        .where(groupFilters.toArray(SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY)));
    }

    @Override
    public final Table selectDistinct(@NotNull final SelectColumn... columns) {
        for (SelectColumn selectColumn : columns) {
            selectColumn.initDef(definition.getColumnNameMap());
            if (!isValidAgainstColumnPartitionTable(selectColumn.getColumns(), selectColumn.getColumnArrays())) {
                return super.selectDistinct(columns);
            }
        }
        initializeAvailableLocations();
        final List<ImmutableTableLocationKey> existingLocationKeys =
                columnSourceManager.allLocations().stream().filter(tl -> {
                    tl.refresh();
                    final long size = tl.getSize();
                    // noinspection ConditionCoveredByFurtherCondition
                    return size != TableLocation.NULL_SIZE && size > 0;
                }).map(TableLocation::getKey).collect(Collectors.toList());
        final List<String> partitionTableColumnNames = new ArrayList<>(partitioningColumnDefinitions.keySet());
        final List<ColumnSource<?>> partitionTableColumnSources = new ArrayList<>(partitioningColumnDefinitions.size());
        for (final ColumnDefinition<?> columnDefinition : partitioningColumnDefinitions.values()) {
            partitionTableColumnSources.add(makePartitionSource(columnDefinition, existingLocationKeys));
        }
        return TableTools
                .newTable(existingLocationKeys.size(), partitionTableColumnNames, partitionTableColumnSources)
                .selectDistinct(columns);
        // TODO (https://github.com/deephaven/deephaven-core/issues/867): Refactor around a ticking partition table
        // TODO: Maybe just get rid of this implementation and coalesce? Partitioning columns are automatically grouped.
        // Needs lazy region allocation.
    }

    private boolean isValidAgainstColumnPartitionTable(@NotNull final List<String> columnNames,
            @NotNull final List<String> columnArrayNames) {
        if (columnArrayNames.size() > 0) {
            return false;
        }
        return columnNames.stream().allMatch(partitioningColumnDefinitions::containsKey);
    }
}
