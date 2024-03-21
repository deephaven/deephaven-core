//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.partitioned;

import com.google.auto.service.AutoService;
import io.deephaven.api.ColumnName;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.qst.type.Type;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Engine-specific implementation of {@link PartitionedTableFactory.Creator}.
 */
@SuppressWarnings("unused")
public enum PartitionedTableCreatorImpl implements PartitionedTableFactory.Creator {

    INSTANCE;

    @SuppressWarnings("unused")
    @AutoService(PartitionedTableFactory.CreatorProvider.class)
    public static final class ProviderImpl implements PartitionedTableFactory.CreatorProvider {

        @Override
        public PartitionedTableFactory.Creator get() {
            return INSTANCE;
        }
    }

    public static final ColumnName CONSTITUENT = ColumnName.of("__CONSTITUENT__");
    private static final TableDefinition CONSTRUCTED_PARTITIONED_TABLE_DEFINITION =
            TableDefinition.of(ColumnDefinition.of(CONSTITUENT.name(), Type.find(Table.class)));

    @Override
    public PartitionedTable of(
            @NotNull final Table table,
            @NotNull final Collection<String> keyColumnNames,
            final boolean uniqueKeys,
            @NotNull final String constituentColumnName,
            @NotNull final TableDefinition constituentDefinition,
            final boolean constituentChangesPermitted) {
        // Validate key columns
        if (!table.hasColumns(keyColumnNames)) {
            throw new IllegalArgumentException("Partitioned table " + table
                    + " does not have all key columns in " + keyColumnNames + "; instead has "
                    + table.getDefinition().getColumnNamesAsString());
        }

        // Validate constituent columns
        final ColumnDefinition<?> constituentColumnDefinition =
                table.getDefinition().getColumn(constituentColumnName);
        if (constituentColumnDefinition == null) {
            throw new IllegalArgumentException("Partitioned table " + table
                    + " has no column named " + constituentColumnName);
        }
        if (!Table.class.isAssignableFrom(constituentColumnDefinition.getDataType())) {
            throw new IllegalArgumentException("Constituent column " + constituentColumnName
                    + " has unsupported data type " + constituentColumnDefinition.getDataType());
        }

        return new PartitionedTableImpl(
                table,
                keyColumnNames,
                uniqueKeys,
                constituentColumnName,
                constituentDefinition,
                constituentChangesPermitted && table.isRefreshing(),
                true);
    }

    @Override
    public PartitionedTable of(@NotNull final Table table) {
        final UpdateGraph updateGraph = table.getUpdateGraph();
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            return internalOf(table);
        }
    }

    private PartitionedTable internalOf(@NotNull final Table table) {
        final Map<Boolean, List<ColumnDefinition<?>>> splitColumns = table.getDefinition().getColumnStream().collect(
                Collectors.partitioningBy(cd -> Table.class.isAssignableFrom(cd.getDataType())));
        final List<ColumnDefinition<?>> tableColumns = splitColumns.get(true);
        final List<ColumnDefinition<?>> keyColumns = splitColumns.get(false);

        if (tableColumns.size() != 1) {
            throw new IllegalArgumentException("Underlying table " + table
                    + " has multiple possible constituent columns: "
                    + tableColumns.stream().map(ColumnDefinition::getName).collect(Collectors.joining(", ")));
        }
        final String constituentColumnName = tableColumns.get(0).getName();
        final MutableObject<Table> firstConstituentHolder = new MutableObject<>();
        if (table.isRefreshing()) {
            ConstructSnapshot.callDataSnapshotFunction(
                    table,
                    ConstructSnapshot.makeSnapshotControl(false, true, (NotificationStepSource) table),
                    ((usePrev, beforeClockValue) -> readFirstConstituent(firstConstituentHolder, table,
                            constituentColumnName, usePrev)));
        } else {
            readFirstConstituent(firstConstituentHolder, table, constituentColumnName, false);
        }
        final Table firstConstituent = firstConstituentHolder.getValue();
        if (firstConstituent == null) {
            throw new IllegalArgumentException("Underlying table " + table
                    + " has no constituent tables in column: " + constituentColumnName);
        }

        return new PartitionedTableImpl(
                table,
                keyColumns.stream().map(ColumnDefinition::getName).collect(Collectors.toList()),
                false,
                constituentColumnName,
                firstConstituent.getDefinition(),
                table.isRefreshing(),
                true);
    }

    private static boolean readFirstConstituent(
            @NotNull final MutableObject<Table> resultHolder,
            @NotNull final Table table,
            @NotNull final String constituentColumnName,
            final boolean usePrev) {
        resultHolder.setValue(readFirstConstituent(table, constituentColumnName, usePrev));
        return true;
    }

    private static Table readFirstConstituent(
            @NotNull final Table table,
            @NotNull final String constituentColumnName,
            final boolean usePrev) {
        return (Table) (usePrev
                ? table.getColumnSource(constituentColumnName).getPrev(table.getRowSet().firstRowKeyPrev())
                : table.getColumnSource(constituentColumnName).get(table.getRowSet().firstRowKey()));
    }

    @Override
    public PartitionedTable ofTables(
            @NotNull final TableDefinition constituentDefinition,
            @NotNull final Table... constituents) {
        return constituentsToPartitionedTable(constituentDefinition, constituents);
    }

    @Override
    public PartitionedTable ofTables(@NotNull final Table... constituents) {
        return constituentsToPartitionedTable(null, constituents);
    }

    private PartitionedTableImpl constituentsToPartitionedTable(
            @Nullable final TableDefinition constituentDefinition,
            @NotNull final Table... constituents) {
        final Table[] constituentsToUse = Arrays.stream(constituents).filter(Objects::nonNull).toArray(Table[]::new);
        if (constituentsToUse.length == 0) {
            throw new IllegalArgumentException("No non-null constituents provided");
        }

        final TableDefinition constituentDefinitionToUse =
                constituentDefinition == null ? constituentsToUse[0].getDefinition() : constituentDefinition;

        // noinspection resource
        final TrackingRowSet rowSet = RowSetFactory.flat(constituentsToUse.length).toTracking();
        final Map<String, ColumnSource<?>> columnSources =
                Map.of(CONSTITUENT.name(), InMemoryColumnSource.getImmutableMemoryColumnSource(constituentsToUse));

        final Table table;
        // validate that the update graph is consistent
        final UpdateGraph updateGraph = constituents[0].getUpdateGraph(constituents);
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            table = new QueryTable(
                    CONSTRUCTED_PARTITIONED_TABLE_DEFINITION,
                    rowSet,
                    columnSources) {
                {
                    setFlat();
                }
            };

            for (final Table constituent : constituentsToUse) {
                table.addParentReference(constituent);
            }

            return new PartitionedTableImpl(
                    table,
                    Collections.emptyList(),
                    false,
                    CONSTITUENT.name(),
                    constituentDefinitionToUse,
                    false,
                    true);
        }
    }
}
