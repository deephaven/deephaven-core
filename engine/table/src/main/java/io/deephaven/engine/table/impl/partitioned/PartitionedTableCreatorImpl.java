package io.deephaven.engine.table.impl.partitioned;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.PartitionedTableFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Set;
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

    public PartitionedTable of(
            @NotNull final Table table,
            @NotNull final Set<String> keyColumnNames,
            @NotNull final String constituentColumnName,
            @NotNull final TableDefinition constituentDefinition) {
        // Validate key columns
        if (!table.hasColumns(keyColumnNames)) {
            throw new IllegalArgumentException("Underlying table " + table
                    + " does not have all key columns in " + keyColumnNames + "; instead has "
                    + table.getDefinition().getColumnNamesAsString());
        }

        // Validate constituent columns
        final ColumnDefinition<?> constituentColumnDefinition =
                table.getDefinition().getColumn(constituentColumnName);
        if (constituentColumnDefinition == null) {
            throw new IllegalArgumentException("Underlying table " + table
                    + " has no column named " + constituentColumnName);
        }
        if (!Table.class.isAssignableFrom(constituentColumnDefinition.getDataType())) {
            throw new IllegalArgumentException("constituent column " + constituentColumnName
                    + " has unsupported data type " + constituentColumnDefinition.getDataType());
        }

        return new PartitionedTableImpl(table, keyColumnNames, constituentColumnName, constituentDefinition);
    }

    public PartitionedTable of(@NotNull final Table table) {
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
                constituentColumnName,
                firstConstituent.getDefinition());
    }

    private static boolean readFirstConstituent(
            @NotNull final MutableObject<Table> resultHolder,
            @NotNull final Table table,
            @NotNull final String constituentColumnName,
            final boolean usePrev) {
        resultHolder.setValue((Table) (usePrev
                ? table.getColumnSource(constituentColumnName).getPrev(table.getRowSet().firstRowKeyPrev())
                : table.getColumnSource(constituentColumnName).get(table.getRowSet().firstRowKey())));
        return true;
    }
}
