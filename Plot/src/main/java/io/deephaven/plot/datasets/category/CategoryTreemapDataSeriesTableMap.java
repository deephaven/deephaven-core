package io.deephaven.plot.datasets.category;

import io.deephaven.engine.table.Table;
import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.TableSnapshotSeries;
import io.deephaven.plot.util.tables.TableHandle;

import java.util.Collection;

public class CategoryTreemapDataSeriesTableMap extends AbstractTableBasedCategoryDataSeries
        implements CategoryTableDataSeriesInternal, TableSnapshotSeries {

    private final TableHandle tableHandle;
    private final String idColumn;
    private final String parentColumn;
    private final String valueColumn;
    private final String labelColumn;
    private final String hoverTextColumn;
    private final String colorColumn;

    public CategoryTreemapDataSeriesTableMap(AxesImpl axes, int id, Comparable name, TableHandle h, String idColumn,
            String parentColumn, String valueColumn, String labelColumn, String hoverTextColumn, String colorColumn) {
        super(axes, id, name, null);
        this.tableHandle = h;
        this.idColumn = idColumn;
        this.parentColumn = parentColumn;
        this.valueColumn = valueColumn;
        this.labelColumn = labelColumn;
        this.hoverTextColumn = hoverTextColumn;
        this.colorColumn = colorColumn;
    }

    public CategoryTreemapDataSeriesTableMap(CategoryTreemapDataSeriesTableMap series, AxesImpl axes) {
        super(series, axes);
        this.tableHandle = series.getTableHandle();
        this.idColumn = series.getCategoryCol();
        this.parentColumn = series.getParentColumn();
        this.valueColumn = series.getValueCol();
        this.labelColumn = series.getLabelColumn();
        this.hoverTextColumn = series.getHoverTextColumn();
        this.colorColumn = series.getColorColumn();
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
        return new CategoryTreemapDataSeriesTableMap(this, axes);
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
