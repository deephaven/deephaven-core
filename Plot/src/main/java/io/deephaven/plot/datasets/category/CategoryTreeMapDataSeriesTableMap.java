package io.deephaven.plot.datasets.category;

import io.deephaven.engine.table.Table;
import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.TableSnapshotSeries;
import io.deephaven.plot.util.tables.TableHandle;

import java.util.Collection;

public class CategoryTreeMapDataSeriesTableMap extends AbstractTableBasedCategoryDataSeries
        implements CategoryTableDataSeriesInternal, TableSnapshotSeries {

    private final TableHandle tableHandle;
    private final String idColumn;
    private final String parentColumn;
    private final String labelColumn;
    private final String valueColumn;
    private final String colorColumn;
    private final String hoverTextColumn;

    private final String textColumn;

    public CategoryTreeMapDataSeriesTableMap(AxesImpl axes, int id, Comparable name, TableHandle h, String idColumn,
            String parentColumn, String labelColumn, String valueColumn, String textColumn, String colorColumn,
            String hoverTextColumn) {
        super(axes, id, name, null);
        this.tableHandle = h;
        this.idColumn = idColumn;
        this.parentColumn = parentColumn;
        this.labelColumn = labelColumn;
        this.valueColumn = valueColumn;
        this.textColumn = textColumn;
        this.colorColumn = colorColumn;
        this.hoverTextColumn = hoverTextColumn;
    }

    public CategoryTreeMapDataSeriesTableMap(CategoryTreeMapDataSeriesTableMap series, AxesImpl axes) {
        super(series, axes);
        this.tableHandle = series.getTableHandle();
        this.idColumn = series.getCategoryCol();
        this.parentColumn = series.getParentColumn();
        this.labelColumn = series.getLabelColumn();
        this.valueColumn = series.getValueCol();
        this.textColumn = series.getTextColumn();
        this.colorColumn = series.getColorColumn();
        this.hoverTextColumn = series.getHoverTextColumn();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("size()");
    }

    @Override
    protected Table getTable() {
        return tableHandle.getTable();
    }

    public TableHandle getTableHandle() {
        return tableHandle;
    }

    @Override
    public String getCategoryCol() {
        return idColumn;
    }

    @Override
    public String getValueCol() {
        return valueColumn;
    }

    public String getTextColumn() {
        return textColumn;
    }

    public String getParentColumn() {
        return parentColumn;
    }

    public String getLabelColumn() {
        return labelColumn;
    }

    public String getColorColumn() {
        return colorColumn;
    }

    public String getHoverTextColumn() {
        return hoverTextColumn;
    }

    @Override
    public CategoryDataSeriesInternal copy(AxesImpl axes) {
        return new CategoryTreeMapDataSeriesTableMap(this, axes);
    }

    @Override
    public Collection<Comparable> categories() {
        throw new UnsupportedOperationException("categories()");
    }

    @Override
    public Number getValue(Comparable category) {
        throw new UnsupportedOperationException("getValue()");
    }

    @Override
    public long getCategoryLocation(Comparable category) {
        throw new UnsupportedOperationException("getCategoryLocation()");
    }
}
