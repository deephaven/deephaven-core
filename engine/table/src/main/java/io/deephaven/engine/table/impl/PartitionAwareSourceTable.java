/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.*;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
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
public class PartitionAwareSourceTable extends SourceTable<PartitionAwareSourceTable> {

    private final Map<String, ColumnDefinition<?>> partitioningColumnDefinitions;
    private final WhereFilter[] partitioningColumnFilters;

    /**
     * @param tableDefinition A TableDefinition
     * @param description A human-readable description for this table
     * @param componentFactory A component factory for creating column source managers
     * @param locationProvider A TableLocationProvider, for use in discovering the locations that compose this table
     * @param updateSourceRegistrar Callback for registering live tables for refreshes, null if this table is not live
     */
    public PartitionAwareSourceTable(@NotNull final TableDefinition tableDefinition,
            @NotNull final String description,
            @NotNull final SourceTableComponentFactory componentFactory,
            @NotNull final TableLocationProvider locationProvider,
            @Nullable final UpdateSourceRegistrar updateSourceRegistrar) {
        this(tableDefinition,
                description,
                componentFactory,
                locationProvider,
                updateSourceRegistrar,
                extractPartitioningColumnDefinitions(tableDefinition));
    }

    PartitionAwareSourceTable(@NotNull final TableDefinition tableDefinition,
            @NotNull final String description,
            @NotNull final SourceTableComponentFactory componentFactory,
            @NotNull final TableLocationProvider locationProvider,
            @Nullable final UpdateSourceRegistrar updateSourceRegistrar,
            @NotNull final Map<String, ColumnDefinition<?>> partitioningColumnDefinitions,
            @Nullable final WhereFilter... partitioningColumnFilters) {
        super(tableDefinition, description, componentFactory, locationProvider, updateSourceRegistrar);
        this.partitioningColumnDefinitions = partitioningColumnDefinitions;
        this.partitioningColumnFilters = partitioningColumnFilters;
    }

    protected PartitionAwareSourceTable newInstance(@NotNull final TableDefinition tableDefinition,
            @NotNull final String description,
            @NotNull final SourceTableComponentFactory componentFactory,
            @NotNull final TableLocationProvider locationProvider,
            @Nullable final UpdateSourceRegistrar updateSourceRegistrar,
            @NotNull final Map<String, ColumnDefinition<?>> partitioningColumnDefinitions,
            @Nullable final WhereFilter... partitioningColumnFilters) {
        return new PartitionAwareSourceTable(tableDefinition, description, componentFactory, locationProvider,
                updateSourceRegistrar, partitioningColumnDefinitions, partitioningColumnFilters);
    }

    private PartitionAwareSourceTable getFilteredTable(
            @NotNull final WhereFilter... additionalPartitioningColumnFilters) {
        WhereFilter[] resultPartitioningColumnFilters =
                new WhereFilter[partitioningColumnFilters.length + additionalPartitioningColumnFilters.length];
        System.arraycopy(partitioningColumnFilters, 0, resultPartitioningColumnFilters, 0,
                partitioningColumnFilters.length);
        System.arraycopy(additionalPartitioningColumnFilters, 0, resultPartitioningColumnFilters,
                partitioningColumnFilters.length, additionalPartitioningColumnFilters.length);
        return newInstance(definition,
                description + ".where(" + Arrays.deepToString(additionalPartitioningColumnFilters) + ')',
                componentFactory, locationProvider, updateSourceRegistrar, partitioningColumnDefinitions,
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
        protected TableAndRemainingFilters getWithWhere(WhereFilter... whereFilters) {
            ArrayList<WhereFilter> partitionFilters = new ArrayList<>();
            ArrayList<WhereFilter> groupFilters = new ArrayList<>();
            ArrayList<WhereFilter> otherFilters = new ArrayList<>();

            List<ColumnDefinition<?>> groupingColumns = table.getDefinition().getGroupingColumns();
            Set<String> groupingColumnNames =
                    groupingColumns.stream().map(ColumnDefinition::getName).collect(Collectors.toSet());

            for (WhereFilter filter : whereFilters) {
                // note: our filters are already initialized
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
                    : table.where(partitionFilters.toArray(WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY));

            // put the other filters onto the end of the grouping filters, this means that the group filters should
            // go first, which should be preferable to having them second. This is basically the first query
            // optimization that we're doing for the user, so maybe it is a good thing but maybe not. The reason we do
            // it, is that we have deferred the filters for the users permissions, and they did not have the opportunity
            // to properly filter the data yet at this point.
            groupFilters.addAll(otherFilters);

            return new TableAndRemainingFilters(result,
                    groupFilters.toArray(WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY));
        }

        @Override
        public Table selectDistinctInternal(Collection<? extends Selectable> columns) {
            final SelectColumn[] selectColumns = SelectColumn.from(columns);
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
    protected PartitionAwareSourceTable copy() {
        final PartitionAwareSourceTable result = newInstance(definition, description, componentFactory, locationProvider,
                updateSourceRegistrar, partitioningColumnDefinitions, partitioningColumnFilters);
        LiveAttributeMap.copyAttributes(this, result, ak -> true);
        return result;
    }

    @Override
    protected final BaseTable redefine(@NotNull final TableDefinition newDefinition) {
        if (newDefinition.getColumnNames().equals(definition.getColumnNames())) {
            // Nothing changed - we have the same columns in the same order.
            return this;
        }
        if (newDefinition.numColumns() == definition.numColumns()
                || newDefinition.getPartitioningColumns().size() == partitioningColumnDefinitions.size()) {
            // Nothing changed except ordering, *or* some columns were dropped but the partitioning column was retained.
            return newInstance(newDefinition,
                    description + "-retainColumns",
                    componentFactory, locationProvider, updateSourceRegistrar, partitioningColumnDefinitions,
                    partitioningColumnFilters);
        }
        // Some partitioning columns are gone - defer dropping them.
        final List<ColumnDefinition<?>> newColumnDefinitions = new ArrayList<>(newDefinition.getColumns());
        final Map<String, ColumnDefinition<?>> retainedPartitioningColumnDefinitions =
                extractPartitioningColumnDefinitions(newDefinition);
        final Collection<ColumnDefinition<?>> droppedPartitioningColumnDefinitions =
                partitioningColumnDefinitions.values()
                        .stream().filter(cd -> !retainedPartitioningColumnDefinitions.containsKey(cd.getName()))
                        .collect(Collectors.toList());
        newColumnDefinitions.addAll(droppedPartitioningColumnDefinitions);
        final PartitionAwareSourceTable redefined = newInstance(TableDefinition.of(newColumnDefinitions),
                description + "-retainColumns",
                componentFactory, locationProvider, updateSourceRegistrar, partitioningColumnDefinitions,
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
        final WritableColumnSource<? super T> result =
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
    public final Table where(final Collection<? extends Filter> filters) {
        if (filters.isEmpty()) {
            return QueryPerformanceRecorder.withNugget(description + ".coalesce()", this::coalesce);
        }
        final WhereFilter[] whereFilters = WhereFilter.from(filters);

        ArrayList<WhereFilter> partitionFilters = new ArrayList<>();
        ArrayList<WhereFilter> groupFilters = new ArrayList<>();
        ArrayList<WhereFilter> otherFilters = new ArrayList<>();

        List<ColumnDefinition<?>> groupingColumns = definition.getGroupingColumns();
        Set<String> groupingColumnNames =
                groupingColumns.stream().map(ColumnDefinition::getName).collect(Collectors.toSet());

        for (WhereFilter whereFilter : whereFilters) {
            whereFilter.init(definition);
            List<String> columns = whereFilter.getColumns();
            if (whereFilter instanceof ReindexingFilter) {
                otherFilters.add(whereFilter);
            } else if (isValidAgainstColumnPartitionTable(columns, whereFilter.getColumnArrays())) {
                partitionFilters.add(whereFilter);
            } else if (whereFilter.isSimpleFilter() && (columns.size() == 1)
                    && (groupingColumnNames.contains(columns.get(0)))) {
                groupFilters.add(whereFilter);
            } else {
                otherFilters.add(whereFilter);
            }
        }

        // if there was nothing that actually required the partition, defer the result.
        if (partitionFilters.isEmpty()) {
            DeferredViewTable deferredViewTable =
                    new DeferredViewTable(definition, description + "-withDeferredFilters",
                            new PartitionAwareQueryTableReference(this), null, null, whereFilters);
            deferredViewTable.setRefreshing(isRefreshing());
            return deferredViewTable;
        }

        WhereFilter[] partitionFilterArray = partitionFilters.toArray(WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY);
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
                        .where(groupFilters.toArray(WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY)));
    }

    @Override
    public final Table selectDistinct(Collection<? extends Selectable> columns) {
        final SelectColumn[] selectColumns = SelectColumn.from(columns);
        for (SelectColumn selectColumn : selectColumns) {
            selectColumn.initDef(definition.getColumnNameMap());
            if (!isValidAgainstColumnPartitionTable(selectColumn.getColumns(), selectColumn.getColumnArrays())) {
                // Be sure to invoke the super-class version of this method, rather than the array-based one that
                // delegates to this method.
                return super.selectDistinct(Arrays.asList(selectColumns));
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
                .selectDistinct(selectColumns);
        // TODO (https://github.com/deephaven/deephaven-core/issues/867): Refactor around a ticking partition table
        // TODO: Maybe just get rid of this implementation and coalesce? Partitioning columns are automatically grouped.
        // Needs lazy region allocation.
    }

    private boolean isValidAgainstColumnPartitionTable(@NotNull final Collection<String> columnNames,
            @NotNull final Collection<String> columnArrayNames) {
        if (columnArrayNames.size() > 0) {
            return false;
        }
        return columnNames.stream().allMatch(partitioningColumnDefinitions::containsKey);
    }

    private boolean isValidAgainstColumnPartitionTable(Collection<ColumnName> columns) {
        return columns.stream().map(ColumnName::name).allMatch(partitioningColumnDefinitions::containsKey);
    }
}
