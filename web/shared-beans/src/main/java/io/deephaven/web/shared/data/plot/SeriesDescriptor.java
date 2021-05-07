package io.deephaven.web.shared.data.plot;

import java.io.Serializable;

public class SeriesDescriptor implements Serializable {

    // from wrapping AxesImpl
    private SeriesPlotStyle plotStyle;

    // from AbstractSeriesInternal
    private String name;

    // from AbstractDataSeries
    private Boolean linesVisible;
    private Boolean shapesVisible;

    private boolean gradientVisible = false;
    private String lineColor = null;
//    private String lineStyle = null;
    private String pointLabelFormat = null;
    private String xToolTipPattern = null;
    private String yToolTipPattern = null;


    // from AbstractXYDataSeries
    // defaults to fall back on if no value is provided
    //TODO also wrap arrays other than just tables
    private String shapeLabel;
    private Double shapeSize;
    private String shapeColor;
    private String shape;

    // handles all data for this series, regardless of subclass, sort of handled in AbstractSeriesInternal
    private SourceDescriptor[] dataSources;

    public SeriesPlotStyle getPlotStyle() {
        return plotStyle;
    }

    public void setPlotStyle(SeriesPlotStyle plotStyle) {
        this.plotStyle = plotStyle;
    }

    public SourceDescriptor[] getDataSources() {
        return dataSources;
    }

    public void setDataSources(SourceDescriptor[] dataSources) {
        this.dataSources = dataSources;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getLinesVisible() {
        return linesVisible;
    }

    public void setLinesVisible(Boolean linesVisible) {
        this.linesVisible = linesVisible;
    }

    public Boolean getShapesVisible() {
        return shapesVisible;
    }

    public void setShapesVisible(Boolean shapesVisible) {
        this.shapesVisible = shapesVisible;
    }

    public boolean isGradientVisible() {
        return gradientVisible;
    }

    public void setGradientVisible(boolean gradientVisible) {
        this.gradientVisible = gradientVisible;
    }

    public String getLineColor() {
        return lineColor;
    }

    public void setLineColor(String lineColor) {
        this.lineColor = lineColor;
    }

//    public String getLineStyle() {
//        return lineStyle;
//    }

//    public void setLineStyle(String lineStyle) {
//        this.lineStyle = lineStyle;
//    }

    public String getPointLabelFormat() {
        return pointLabelFormat;
    }

    public void setPointLabelFormat(String pointLabelFormat) {
        this.pointLabelFormat = pointLabelFormat;
    }

    public String getXToolTipPattern() {
        return xToolTipPattern;
    }

    public void setXToolTipPattern(String xToolTipPattern) {
        this.xToolTipPattern = xToolTipPattern;
    }

    public String getYToolTipPattern() {
        return yToolTipPattern;
    }

    public void setYToolTipPattern(String yToolTipPattern) {
        this.yToolTipPattern = yToolTipPattern;
    }

    public String getShapeLabel() {
        return shapeLabel;
    }

    public void setShapeLabel(String shapeLabel) {
        this.shapeLabel = shapeLabel;
    }

    public Double getShapeSize() {
        return shapeSize;
    }

    public void setShapeSize(Double shapeSize) {
        this.shapeSize = shapeSize;
    }

    public String getShapeColor() {
        return shapeColor;
    }

    public void setShapeColor(String shapeColor) {
        this.shapeColor = shapeColor;
    }

    public String getShape() {
        return shape;
    }

    public void setShape(String shape) {
        this.shape = shape;
    }
}
