package io.deephaven.web.shared.data.plot;

import java.io.Serializable;

public class MultiSeriesDescriptor implements Serializable {

    // from wrapping AxesImpl
    private SeriesPlotStyle plotStyle;

    // from AbstractSeriesInternal
    private String name;


    private String lineColorDefault;
    private String[] lineColorKeys;
    private String[] lineColorValues;

    // these are "shapeColor" in the plain series
    private String pointColorDefault;
    private String[] pointColorKeys;
    private String[] pointColorValues;

    private Boolean linesVisibleDefault;
    private String[] linesVisibleKeys;
    private Boolean[] linesVisibleValues;

    // these are "shapesVisible" in the plain series
    private Boolean pointsVisibleDefault;
    private String[] pointsVisibleKeys;
    private Boolean[] pointsVisibleValues;

    private Boolean gradientVisibleDefault;
    private String[] gradientVisibleKeys;
    private Boolean[] gradientVisibleValues;

    private String pointLabelFormatDefault;
    private String[] pointLabelFormatKeys;
    private String[] pointLabelFormatValues;

    private String xToolTipPatternDefault;
    private String[] xToolTipPatternKeys;
    private String[] xToolTipPatternValues;

    private String yToolTipPatternDefault;
    private String[] yToolTipPatternKeys;
    private String[] yToolTipPatternValues;

    // these are "shapeLabel" in the plain series
    private String pointLabelDefault;
    private String[] pointLabelKeys;
    private String[] pointLabelValues;

    // these are "shapeSize" in the plain series
    private Double pointSizeDefault;
    private String[] pointSizeKeys;
    private Double[] pointSizeValues;

    // these are "shape" in the plain series
    private String pointShapeDefault;
    private String[] pointShapeKeys;
    private String[] pointShapeValues;



    private MultiSeriesSourceDescriptor[] dataSources;

    public SeriesPlotStyle getPlotStyle() {
        return plotStyle;
    }

    public void setPlotStyle(SeriesPlotStyle plotStyle) {
        this.plotStyle = plotStyle;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLineColorDefault() {
        return lineColorDefault;
    }

    public void setLineColorDefault(String lineColorDefault) {
        this.lineColorDefault = lineColorDefault;
    }

    public String[] getLineColorKeys() {
        return lineColorKeys;
    }

    public void setLineColorKeys(String[] lineColorKeys) {
        this.lineColorKeys = lineColorKeys;
    }

    public String[] getLineColorValues() {
        return lineColorValues;
    }

    public void setLineColorValues(String[] lineColorValues) {
        this.lineColorValues = lineColorValues;
    }

    public String getPointColorDefault() {
        return pointColorDefault;
    }

    public void setPointColorDefault(String pointColorDefault) {
        this.pointColorDefault = pointColorDefault;
    }

    public String[] getPointColorKeys() {
        return pointColorKeys;
    }

    public void setPointColorKeys(String[] pointColorKeys) {
        this.pointColorKeys = pointColorKeys;
    }

    public String[] getPointColorValues() {
        return pointColorValues;
    }

    public void setPointColorValues(String[] pointColorValues) {
        this.pointColorValues = pointColorValues;
    }

    public Boolean getLinesVisibleDefault() {
        return linesVisibleDefault;
    }

    public void setLinesVisibleDefault(Boolean linesVisibleDefault) {
        this.linesVisibleDefault = linesVisibleDefault;
    }

    public String[] getLinesVisibleKeys() {
        return linesVisibleKeys;
    }

    public void setLinesVisibleKeys(String[] linesVisibleKeys) {
        this.linesVisibleKeys = linesVisibleKeys;
    }

    public Boolean[] getLinesVisibleValues() {
        return linesVisibleValues;
    }

    public void setLinesVisibleValues(Boolean[] linesVisibleValues) {
        this.linesVisibleValues = linesVisibleValues;
    }

    public Boolean getPointsVisibleDefault() {
        return pointsVisibleDefault;
    }

    public void setPointsVisibleDefault(Boolean pointsVisibleDefault) {
        this.pointsVisibleDefault = pointsVisibleDefault;
    }

    public String[] getPointsVisibleKeys() {
        return pointsVisibleKeys;
    }

    public void setPointsVisibleKeys(String[] pointsVisibleKeys) {
        this.pointsVisibleKeys = pointsVisibleKeys;
    }

    public Boolean[] getPointsVisibleValues() {
        return pointsVisibleValues;
    }

    public void setPointsVisibleValues(Boolean[] pointsVisibleValues) {
        this.pointsVisibleValues = pointsVisibleValues;
    }

    public Boolean getGradientVisibleDefault() {
        return gradientVisibleDefault;
    }

    public void setGradientVisibleDefault(Boolean gradientVisibleDefault) {
        this.gradientVisibleDefault = gradientVisibleDefault;
    }

    public String[] getGradientVisibleKeys() {
        return gradientVisibleKeys;
    }

    public void setGradientVisibleKeys(String[] gradientVisibleKeys) {
        this.gradientVisibleKeys = gradientVisibleKeys;
    }

    public Boolean[] getGradientVisibleValues() {
        return gradientVisibleValues;
    }

    public void setGradientVisibleValues(Boolean[] gradientVisibleValues) {
        this.gradientVisibleValues = gradientVisibleValues;
    }

    public String getPointLabelFormatDefault() {
        return pointLabelFormatDefault;
    }

    public void setPointLabelFormatDefault(String pointLabelFormatDefault) {
        this.pointLabelFormatDefault = pointLabelFormatDefault;
    }

    public String[] getPointLabelFormatKeys() {
        return pointLabelFormatKeys;
    }

    public void setPointLabelFormatKeys(String[] pointLabelFormatKeys) {
        this.pointLabelFormatKeys = pointLabelFormatKeys;
    }

    public String[] getPointLabelFormatValues() {
        return pointLabelFormatValues;
    }

    public void setPointLabelFormatValues(String[] pointLabelFormatValues) {
        this.pointLabelFormatValues = pointLabelFormatValues;
    }

    public String getXToolTipPatternDefault() {
        return xToolTipPatternDefault;
    }

    public void setXToolTipPatternDefault(String xToolTipPatternDefault) {
        this.xToolTipPatternDefault = xToolTipPatternDefault;
    }

    public String[] getXToolTipPatternKeys() {
        return xToolTipPatternKeys;
    }

    public void setXToolTipPatternKeys(String[] xToolTipPatternKeys) {
        this.xToolTipPatternKeys = xToolTipPatternKeys;
    }

    public String[] getXToolTipPatternValues() {
        return xToolTipPatternValues;
    }

    public void setXToolTipPatternValues(String[] xToolTipPatternValues) {
        this.xToolTipPatternValues = xToolTipPatternValues;
    }

    public String getYToolTipPatternDefault() {
        return yToolTipPatternDefault;
    }

    public void setYToolTipPatternDefault(String yToolTipPatternDefault) {
        this.yToolTipPatternDefault = yToolTipPatternDefault;
    }

    public String[] getYToolTipPatternKeys() {
        return yToolTipPatternKeys;
    }

    public void setYToolTipPatternKeys(String[] yToolTipPatternKeys) {
        this.yToolTipPatternKeys = yToolTipPatternKeys;
    }

    public String[] getYToolTipPatternValues() {
        return yToolTipPatternValues;
    }

    public void setYToolTipPatternValues(String[] yToolTipPatternValues) {
        this.yToolTipPatternValues = yToolTipPatternValues;
    }

    public String getPointLabelDefault() {
        return pointLabelDefault;
    }

    public void setPointLabelDefault(String pointLabelDefault) {
        this.pointLabelDefault = pointLabelDefault;
    }

    public String[] getPointLabelKeys() {
        return pointLabelKeys;
    }

    public void setPointLabelKeys(String[] pointLabelKeys) {
        this.pointLabelKeys = pointLabelKeys;
    }

    public String[] getPointLabelValues() {
        return pointLabelValues;
    }

    public void setPointLabelValues(String[] pointLabelValues) {
        this.pointLabelValues = pointLabelValues;
    }

    public Double getPointSizeDefault() {
        return pointSizeDefault;
    }

    public void setPointSizeDefault(Double pointSizeDefault) {
        this.pointSizeDefault = pointSizeDefault;
    }

    public String[] getPointSizeKeys() {
        return pointSizeKeys;
    }

    public void setPointSizeKeys(String[] pointSizeKeys) {
        this.pointSizeKeys = pointSizeKeys;
    }

    public Double[] getPointSizeValues() {
        return pointSizeValues;
    }

    public void setPointSizeValues(Double[] pointSizeValues) {
        this.pointSizeValues = pointSizeValues;
    }

    public String getPointShapeDefault() {
        return pointShapeDefault;
    }

    public void setPointShapeDefault(String pointShapeDefault) {
        this.pointShapeDefault = pointShapeDefault;
    }

    public String[] getPointShapeKeys() {
        return pointShapeKeys;
    }

    public void setPointShapeKeys(String[] pointShapeKeys) {
        this.pointShapeKeys = pointShapeKeys;
    }

    public String[] getPointShapeValues() {
        return pointShapeValues;
    }

    public void setPointShapeValues(String[] pointShapeValues) {
        this.pointShapeValues = pointShapeValues;
    }

    public MultiSeriesSourceDescriptor[] getDataSources() {
        return dataSources;
    }

    public void setDataSources(MultiSeriesSourceDescriptor[] dataSources) {
        this.dataSources = dataSources;
    }
}
