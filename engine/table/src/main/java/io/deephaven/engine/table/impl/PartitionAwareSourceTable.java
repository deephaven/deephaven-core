//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.Selectable;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.util.mutable.MutableLong;
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
            @NotNull final List<WhereFilter> additionalPartitioningColumnFilters) {
        final WhereFilter[] resultPartitioningColumnFilters = Stream.concat(
                Arrays.stream(partitioningColumnFilters),
                additionalPartitioningColumnFilters.stream())
                .toArray(WhereFilter[]::new);
        final PartitionAwareSourceTable filtered = newInstance(definition,
                description + ".where(" + additionalPartitioningColumnFilters + ')',
                componentFactory, locationProvider, updateSourceRegistrar, partitioningColumnDefinitions,
                resultPartitioningColumnFilters);
        copyAttributes(filtered, CopyAttributeOperation.Filter);
        return filtered;
    }

    private static Map<String, ColumnDefinition<?>> extractPartitioningColumnDefinitions(
            @NotNull final TableDefinition tableDefinition) {
        return tableDefinition.getColumnStream()
                .filter(ColumnDefinition::isPartitioning)
                .collect(Collectors.toMap(ColumnDefinition::getName, Function.identity(), Assert::neverInvoked,
                        LinkedHashMap::new));
    }

    private static class PartitionAwareTableReference extends DeferredViewTable.TableReference {

        private PartitionAwareTableReference(PartitionAwareSourceTable table) {
            super(table);
        }

        @Override
        protected TableAndRemainingFilters getWithWhere(WhereFilter... whereFilters) {
            final List<WhereFilter> partitionFilters = new ArrayList<>();
            final List<WhereFilter> otherFilters = new ArrayList<>();
            for (WhereFilter whereFilter : whereFilters) {
                if (!(whereFilter instanceof ReindexingFilter)
                        && ((PartitionAwareSourceTable) table).isValidAgainstColumnPartitionTable(
                                whereFilter.getColumns(), whereFilter.getColumnArrays())) {
                    partitionFilters.add(whereFilter);
                } else {
                    otherFilters.add(whereFilter);
                }
            }

            final Table result = partitionFilters.isEmpty()
                    ? table
                    : table.where(Filter.and(partitionFilters));

            return new TableAndRemainingFilters(result.coalesce(),
                    otherFilters.toArray(WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY));
        }

        @Override
        public Table selectDistinctInternal(Collection<? extends Selectable> columns) {
            final List<SelectColumn> selectColumns = Arrays.asList(SelectColumn.from(columns));
            try {
                SelectAndViewAnalyzer.initializeSelectColumns(table.getDefinition().getColumnNameMap(),
                        selectColumns.toArray(SelectColumn[]::new));
            } catch (Exception e) {
                return null;
            }

            final Set<String> partitioningDerivedColumnNames = new HashSet<>();
            for (final SelectColumn selectColumn : selectColumns) {
                if (!((PartitionAwareSourceTable) table).isValidAgainstColumnPartitionTable(
                        selectColumn.getColumns(), selectColumn.getColumnArrays(), partitioningDerivedColumnNames)) {
                    return null;
                }
                partitioningDerivedColumnNames.add(selectColumn.getName());
            }
            return table.selectDistinct(selectColumns);
        }
    }

    @Override
    protected PartitionAwareSourceTable copy() {
        final PartitionAwareSourceTable result =
                newInstance(definition, description, componentFactory, locationProvider,
                        updateSourceRegistrar, partitioningColumnDefinitions, partitioningColumnFilters);
        LiveAttributeMap.copyAttributes(this, result, ak -> true);
        return result;
    }

    @Override
    protected final BaseTable<?> redefine(@NotNull final TableDefinition newDefinition) {
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
        return new DeferredViewTable(newDefinition, description + "-retainColumns",
                new PartitionAwareTableReference(redefined),
                droppedPartitioningColumnDefinitions.stream().map(ColumnDefinition::getName).toArray(String[]::new),
                null, null);
    }

    @Override
    protected final Table redefine(TableDefinition newDefinitionExternal, TableDefinition newDefinitionInternal,
            SelectColumn[] viewColumns) {
        BaseTable<?> redefined = redefine(newDefinitionInternal);
        DeferredViewTable.TableReference reference = redefined instanceof PartitionAwareSourceTable
                ? new PartitionAwareTableReference((PartitionAwareSourceTable) redefined)
                : new DeferredViewTable.TableReference(redefined);
        return new DeferredViewTable(newDefinitionExternal, description + "-redefined",
                reference, null, viewColumns, null);
    }

    private static final String KEY_SUPPLIER_COLUMN_NAME = "__PartitionAwareSourceTable_KeySupplier__";

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
    protected final Collection<LiveSupplier<ImmutableTableLocationKey>> filterLocationKeys(
            @NotNull final Collection<LiveSupplier<ImmutableTableLocationKey>> foundLocationKeys) {
        if (partitioningColumnFilters.length == 0) {
            return foundLocationKeys;
        }

        final Collection<ImmutableTableLocationKey> immutableTableLocationKeys = foundLocationKeys.stream()
                .map(LiveSupplier::get)
                .collect(Collectors.toList());

        // TODO (https://github.com/deephaven/deephaven-core/issues/867): Refactor around a ticking partition table
        final List<String> partitionTableColumnNames = Stream.concat(
                partitioningColumnDefinitions.keySet().stream(),
                Stream.of(KEY_SUPPLIER_COLUMN_NAME)).collect(Collectors.toList());
        final List<ColumnSource<?>> partitionTableColumnSources =
                new ArrayList<>(partitioningColumnDefinitions.size() + 1);
        for (final ColumnDefinition<?> columnDefinition : partitioningColumnDefinitions.values()) {
            partitionTableColumnSources.add(makePartitionSource(columnDefinition, immutableTableLocationKeys));
        }
        // Add the key suppliers to the table
        // noinspection unchecked,rawtypes
        partitionTableColumnSources.add(ArrayBackedColumnSource.getMemoryColumnSource(
                (Collection<LiveSupplier>) (Collection) foundLocationKeys,
                LiveSupplier.class,
                null));

        final Table filteredColumnPartitionTable = TableTools
                .newTable(foundLocationKeys.size(), partitionTableColumnNames, partitionTableColumnSources)
                .where(Filter.and(partitioningColumnFilters));
        if (filteredColumnPartitionTable.size() == foundLocationKeys.size()) {
            return foundLocationKeys;
        }

        // Return the filtered keys
        final Iterable<LiveSupplier<ImmutableTableLocationKey>> iterable =
                () -> filteredColumnPartitionTable.columnIterator(KEY_SUPPLIER_COLUMN_NAME);
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
    }

    @Override
    public Table where(Filter filter) {
        return whereImpl(WhereFilter.fromInternal(filter));
    }

    private Table whereImpl(final WhereFilter[] whereFilters) {
        if (whereFilters.length == 0) {
            return prepareReturnThis();
        }

        final QueryCompilerRequestProcessor.BatchProcessor compilationProcessor = QueryCompilerRequestProcessor.batch();
        final List<WhereFilter> partitionFilters = new ArrayList<>();
        final List<WhereFilter> otherFilters = new ArrayList<>();
        for (WhereFilter whereFilter : whereFilters) {
            whereFilter.init(definition, compilationProcessor);
            if (!(whereFilter instanceof ReindexingFilter)
                    && isValidAgainstColumnPartitionTable(whereFilter.getColumns(), whereFilter.getColumnArrays())) {
                partitionFilters.add(whereFilter);
            } else {
                otherFilters.add(whereFilter);
            }
        }
        compilationProcessor.compile();

        // If we have no partition filters, we defer all filters.
        if (partitionFilters.isEmpty()) {
            return new DeferredViewTable(definition, getDescription() + "-withDeferredFilters",
                    new PartitionAwareTableReference(this), null, null,
                    otherFilters.toArray(WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY));
        }

        // If we have any partition filters, we first create a new instance that filters the location keys accordingly,
        // then coalesce, and then apply the remaining filters to the coalesced result.
        final Table withPartitionsFiltered = QueryPerformanceRecorder.withNugget(
                "getFilteredTable(" + partitionFilters + ")", () -> getFilteredTable(partitionFilters));
        final Table coalesced = withPartitionsFiltered.coalesce();
        return otherFilters.isEmpty()
                ? coalesced
                : coalesced.where(Filter.and(otherFilters));
    }

    @Override
    public final Table selectDistinct(@NotNull final Collection<? extends Selectable> columns) {
        final List<SelectColumn> selectColumns = Arrays.asList(SelectColumn.from(columns));
        SelectAndViewAnalyzer.initializeSelectColumns(
                definition.getColumnNameMap(), selectColumns.toArray(SelectColumn[]::new));

        final Set<String> partitioningDerivedColumnNames = new HashSet<>();
        for (final SelectColumn selectColumn : selectColumns) {
            if (!isValidAgainstColumnPartitionTable(
                    selectColumn.getColumns(), selectColumn.getColumnArrays(), partitioningDerivedColumnNames)) {
                // Be sure to invoke the super-class version of this method, rather than the array-based one that
                // delegates to this method.
                return super.selectDistinct(selectColumns);
            }
            partitioningDerivedColumnNames.add(selectColumn.getName());
        }

        // Ensure that the location table is available and populated with non-null, non-empty locations.
        initialize();

        // Apply our selectDistinct() to the location table.
        return columnSourceManager.locationTable().selectDistinct(selectColumns);
    }

    private boolean isValidAgainstColumnPartitionTable(
            @NotNull final Collection<String> columnNames,
            @NotNull final Collection<String> columnArrayNames) {
        return isValidAgainstColumnPartitionTable(columnNames, columnArrayNames, Collections.emptySet());
    }

    private boolean isValidAgainstColumnPartitionTable(
            @NotNull final Collection<String> columnNames,
            @NotNull final Collection<String> columnArrayNames,
            @NotNull final Collection<String> partitioningDerivedColumnNames) {
        if (!columnArrayNames.isEmpty()) {
            return false;
        }
        return columnNames.stream().allMatch(
                columnName -> partitioningColumnDefinitions.containsKey(columnName)
                        || partitioningDerivedColumnNames.contains(columnName));
    }
}
