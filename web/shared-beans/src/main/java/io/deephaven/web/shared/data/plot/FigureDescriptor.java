package io.deephaven.web.shared.data.plot;

import io.deephaven.web.shared.data.TableMapHandle;

import java.io.Serializable;

public class FigureDescriptor implements Serializable {
    private String title;
    private String titleFont;
    private String titleColor;
    private boolean resizable;

    private ThemeDescriptor theme;
    private boolean isDefaultTheme;

    private double updateInterval;

    private int cols;
    private int rows;

    private ChartDescriptor[] charts;

    private int[] tableIds;
    private int[][] plotHandleIds;

    private TableMapHandle[] tableMaps;
    private int[][] tableMapIds;

    private String[] errors;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTitleFont() {
        return titleFont;
    }

    public void setTitleFont(String titleFont) {
        this.titleFont = titleFont;
    }

    public String getTitleColor() {
        return titleColor;
    }

    public void setTitleColor(String titleColor) {
        this.titleColor = titleColor;
    }

    public boolean isResizable() {
        return resizable;
    }

    public void setResizable(boolean resizable) {
        this.resizable = resizable;
    }

    public ThemeDescriptor getTheme() {
        return theme;
    }

    public void setTheme(ThemeDescriptor theme) {
        this.theme = theme;
    }

    public boolean isDefaultTheme() {
        return isDefaultTheme;
    }

    public void setDefaultTheme(boolean defaultTheme) {
        isDefaultTheme = defaultTheme;
    }

    public double getUpdateInterval() {
        return updateInterval;
    }

    public void setUpdateInterval(double updateInterval) {
        this.updateInterval = updateInterval;
    }

    public int getCols() {
        return cols;
    }

    public void setCols(int cols) {
        this.cols = cols;
    }

    public int getRows() {
        return rows;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }

    public ChartDescriptor[] getCharts() {
        return charts;
    }

    public void setCharts(ChartDescriptor[] charts) {
        this.charts = charts;
    }

    public TableMapHandle[] getTableMaps() {
        return tableMaps;
    }

    public void setTableMaps(TableMapHandle[] tableMaps) {
        this.tableMaps = tableMaps;
    }

    public int[][] getTableMapIds() {
        return tableMapIds;
    }

    public void setTableMapIds(int[][] tableMapIds) {
        this.tableMapIds = tableMapIds;
    }

    public int[] getTableIds() {
        return tableIds;
    }

    public void setTableIds(int[] tableIds) {
        this.tableIds = tableIds;
    }

    public int[][] getPlotHandleIds() {
        return plotHandleIds;
    }

    public void setPlotHandleIds(int[][] plotHandleIds) {
        this.plotHandleIds = plotHandleIds;
    }

    public String[] getErrors() {
        return errors;
    }

    public void setErrors(String[] errors) {
        this.errors = errors;
    }
}
