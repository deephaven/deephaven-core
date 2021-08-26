package io.deephaven.db.plot.filters;

import io.deephaven.db.plot.ChartImpl;
import io.deephaven.db.plot.util.tables.SwappableTable;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.function.Function;

public class SelectableDataSetSwappableTable<KEY_TYPE, VALUE_TYPE>
        implements SelectableDataSet<KEY_TYPE, VALUE_TYPE>, Serializable {

    private final SwappableTable swappableTable;

    public SelectableDataSetSwappableTable(final SwappableTable swappableTable) {
        this.swappableTable = swappableTable;
    }

    @Override
    public TableDefinition getTableDefinition() {
        return swappableTable.getTableDefinition();
    }

    @Override
    public SelectableDataSet<KEY_TYPE, VALUE_TYPE> transform(@NotNull Object memoKey,
            @NotNull Function<Table, Table> transformation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SwappableTable getSwappableTable(final Comparable seriesName, final ChartImpl chart,
            Function<Table, Table> tableTransform, final String... col) {
        return swappableTable;
    }
}
