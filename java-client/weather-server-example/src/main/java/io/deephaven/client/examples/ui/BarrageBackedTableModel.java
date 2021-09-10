package io.deephaven.client.examples.ui;

import io.deephaven.db.v2.InstrumentedShiftAwareListenerAdapter;
import io.deephaven.grpc_api_client.table.BarrageTable;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.jetbrains.annotations.NotNull;

import javax.swing.table.AbstractTableModel;

public class BarrageBackedTableModel extends AbstractTableModel {
    @ReferentialIntegrity
    private final InstrumentedShiftAwareListenerAdapter listener;
    @NotNull
    private final BarrageTable table;

    public BarrageBackedTableModel(final @NotNull BarrageTable table) {
        this.table = table;

        table.listenForUpdates(listener = new InstrumentedShiftAwareListenerAdapter(table, false) {
            @Override
            public void onUpdate(Update upstream) {
                // We are not going to do anything fancy,  just tick the whole table.  More sophisticated implementations
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
        // Typically you would access column data through a ColumnSource directly with get during iteration over an index
        // Swing expects tables to be "flat" in index space, so we have to find the "key" for a particular flat "position"
        // and look it up that way.
        return table.getColumnSource(table.getDefinition().getColumnNames().get(columnIndex))
                .get(table.getIndex().get(rowIndex));
    }

    @Override
    public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
        throw new UnsupportedOperationException("BarrageTables are not mutable this way");
    }
}
