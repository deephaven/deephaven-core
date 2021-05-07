package io.deephaven.web.shared.data.plot;

import java.io.Serializable;

public class ChartDescriptor implements Serializable {
    public enum ChartType { XY, PIE, OHLC, CATEGORY, XYZ, CATEGORY_3D }

    private int colspan;
    private int rowspan;

    private SeriesDescriptor[] series;
    private MultiSeriesDescriptor[] multiSeries;
    private AxisDescriptor[] axes;

    private ChartType chartType;

    private String title;
    private String titleFont;
    private String titleColor;

    private boolean showLegend;
    private String legendFont;
    private String legendColor;

    private boolean is3d;
    //skipping initialized
    //skipping plotOrientation, instead will transpose the "position" of axis instances

    public int getColspan() {
        return colspan;
    }

    public void setColspan(int colspan) {
        this.colspan = colspan;
    }

    public int getRowspan() {
        return rowspan;
    }

    public void setRowspan(int rowspan) {
        this.rowspan = rowspan;
    }

    public SeriesDescriptor[] getSeries() {
        return series;
    }

    public void setSeries(SeriesDescriptor[] series) {
        this.series = series;
    }

    public MultiSeriesDescriptor[] getMultiSeries() {
        return multiSeries;
    }

    public void setMultiSeries(MultiSeriesDescriptor[] multiSeries) {
        this.multiSeries = multiSeries;
    }

    public AxisDescriptor[] getAxes() {
        return axes;
    }

    public void setAxes(AxisDescriptor[] axes) {
        this.axes = axes;
    }

    public ChartType getChartType() {
        return chartType;
    }

    public void setChartType(ChartType chartType) {
        this.chartType = chartType;
    }

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

    public boolean isShowLegend() {
        return showLegend;
    }

    public void setShowLegend(boolean showLegend) {
        this.showLegend = showLegend;
    }

    public String getLegendFont() {
        return legendFont;
    }

    public void setLegendFont(String legendFont) {
        this.legendFont = legendFont;
    }

    public String getLegendColor() {
        return legendColor;
    }

    public void setLegendColor(String legendColor) {
        this.legendColor = legendColor;
    }

    public boolean isIs3d() {
        return is3d;
    }

    public void setIs3d(boolean is3d) {
        this.is3d = is3d;
    }
}
