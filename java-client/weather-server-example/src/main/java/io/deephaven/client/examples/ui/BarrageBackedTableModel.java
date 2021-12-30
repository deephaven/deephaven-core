package io.deephaven.client.examples.ui;

import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.jetbrains.annotations.NotNull;

import javax.swing.table.AbstractTableModel;

/**
 * A {@link javax.swing.table.TableModel} implementation that simply wraps a {@link BarrageTable} without any fancy
 * viewports.
 */
public class BarrageBackedTableModel extends AbstractTableModel {
    @ReferentialIntegrity
    private final InstrumentedTableUpdateListenerAdapter listener;
    @NotNull
    private final BarrageTable table;

    public BarrageBackedTableModel(final @NotNull BarrageTable table) {
        this.table = table;

        table.listenForUpdates(listener = new InstrumentedTableUpdateListenerAdapter(table, false) {
            @Override
            public void onUpdate(TableUpdate upstream) {
                // We are not going to do anything fancy, just tick the whole table. More sophisticated implementations
                // could selectively update individual rows and cells based on the contents of the update.
                fireTableDataChanged();
            }
        });
    }

    @Override
    public int getRowCount() {
        return table.intSize();
    }

    @Override
    public int getColumnCount() {
        return table.getColumnSourceMap().size();
    }

    @Override
    public String getColumnName(int columnIndex) {
        return table.getDefinition().getColumnNames().get(columnIndex);
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        return table.getDefinition().getColumnList().get(columnIndex).getDataType();
    }

    @Override
    public boolean isCellEditable(int rowIndex, int columnIndex) {
        return false;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        // This is inefficient, but an efficient implementation is out of scope for this example. Typically,
        // we want read data from ColumnSources through the chunking API. Here we'll be expedient
        // and settle for the O(n log n) experience.

        // Swing thinks about data in row positions, but the ColumnSource#get API uses row keys. Note
        // how we convert from row position to row key using `Index#get`.
        return table.getColumnSource(table.getDefinition().getColumnNames().get(columnIndex))
                .get(table.getRowSet().get(rowIndex));
    }

    @Override
    public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
        throw new UnsupportedOperationException("BarrageTables are not mutable this way");
    }
}
