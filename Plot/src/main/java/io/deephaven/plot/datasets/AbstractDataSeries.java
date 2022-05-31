/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets;

import io.deephaven.plot.*;
import io.deephaven.gui.color.Color;
import io.deephaven.gui.color.Paint;

import java.io.Serializable;

import static io.deephaven.plot.util.PlotUtils.intToColor;

/**
 * Base class for {@link DataSeriesInternal}.
 */
public abstract class AbstractDataSeries extends AbstractSeriesInternal implements DataSeriesInternal, Serializable {

    private static final long serialVersionUID = 8733895549099825055L;

    // want different defaults for different PlotStyles
    private Boolean linesVisible = null;
    private Boolean shapesVisible = null;


    private boolean gradientVisible = false;
    private Paint lineColor = null;
    private Paint errorBarColor = null;
    private LineStyle lineStyle = null;
    private String pointLabelFormat = null;
    private String xToolTipPattern = null;
    private String yToolTipPattern = null;


    /**
     * Creates an AbstractDataSeries instance.
     *
     * @param axes axes on which this dataset will be plotted
     * @param id data series id
     * @param name series name
     */
    public AbstractDataSeries(AxesImpl axes, int id, Comparable name, AbstractDataSeries series) {
        super(axes, id, name);

        if (series != null) {
            this.pointLabelFormat = series.pointLabelFormat;
            this.linesVisible = series.linesVisible;
            this.shapesVisible = series.shapesVisible;
            this.gradientVisible = series.gradientVisible;
            this.lineColor = series.lineColor;
            this.errorBarColor = series.errorBarColor;
            this.lineStyle = series.lineStyle;
            this.pointLabelFormat = series.pointLabelFormat;
            this.xToolTipPattern = series.xToolTipPattern;
            this.yToolTipPattern = series.yToolTipPattern;
        }
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    protected AbstractDataSeries(final AbstractDataSeries series, final AxesImpl axes) {
        super(series, axes);
        this.pointLabelFormat = series.pointLabelFormat;
        this.linesVisible = series.linesVisible;
        this.shapesVisible = series.shapesVisible;
        this.gradientVisible = series.gradientVisible;
        this.lineColor = series.lineColor;
        this.errorBarColor = series.errorBarColor;
        this.lineStyle = series.lineStyle;
        this.pointLabelFormat = series.pointLabelFormat;
        this.xToolTipPattern = series.xToolTipPattern;
        this.yToolTipPattern = series.yToolTipPattern;
    }


    ////////////////////////// internal //////////////////////////


    @Override
    public ChartImpl chart() {
        return axes().chart();
    }

    @Override
    public Paint getLineColor() {
        return lineColor;
    }

    @Override
    public Paint getErrorBarColor() {
        return errorBarColor;
    }

    @Override
    public LineStyle getLineStyle() {
        return lineStyle;
    }

    @Override
    public Boolean getLinesVisible() {
        return this.linesVisible;
    }

    @Override
    public Boolean getPointsVisible() {
        return this.shapesVisible;
    }

    @Override
    public boolean getGradientVisible() {
        return this.gradientVisible;
    }

    @Override
    public String getPointLabelFormat() {
        return pointLabelFormat;
    }

    @Override
    public String getXToolTipPattern() {
        return xToolTipPattern;
    }

    @Override
    public String getYToolTipPattern() {
        return yToolTipPattern;
    }

    // below is done as setters so that when the associated methods are implemented, they return the most precise type
    // for the builder

    ////////////////////////// visibility //////////////////////////


    /**
     * Sets the line visibility for this dataset.
     *
     * @param visible whether lines will be visible
     */
    protected void setLinesVisible(final Boolean visible) {
        this.linesVisible = visible;
    }

    /**
     * Sets the points visibility for this dataset.
     *
     * @param visible whether points will be visible
     */
    protected void setPointsVisible(final Boolean visible) {
        this.shapesVisible = visible;
    }

    /**
     * Sets the bar-gradient visibility for this dataset.
     *
     * @param visible whether the bar-gradient will be visible
     */
    protected void setGradientVisible(boolean visible) {
        this.gradientVisible = visible;
    }


    ////////////////////////// point sizes //////////////////////////


    ////////////////////////// point colors //////////////////////////


    ////////////////////////// line color //////////////////////////


    /**
     * Sets the line color for this dataset.
     *
     * @param color color
     */
    protected void setLineColor(Paint color) {
        this.lineColor = color;
    }

    /**
     * Sets the line color for this dataset.
     *
     * @param color color
     */
    protected void setLineColor(int color) {
        this.lineColor = intToColor(chart(), color);
    }

    /**
     * Sets the line color for this dataset.
     *
     * @param color color
     */
    protected void setLineColor(String color) {
        this.lineColor = Color.color(color);
    }


    ////////////////////////// error bar color //////////////////////////

    /**
     * Sets the error bar color for this dataset.
     *
     * @param color color
     */
    protected void setErrorBarColor(final Paint color) {
        this.errorBarColor = color;
    }

    /**
     * Sets the error bar color for this dataset.
     *
     * @param color color
     */
    protected void setErrorBarColor(final int color) {
        setErrorBarColor(intToColor(chart(), color));
    }

    /**
     * Sets the error bar color for this dataset.
     *
     * @param color color
     */
    protected void setErrorBarColor(final String color) {
        setErrorBarColor(Color.color(color));
    }


    ////////////////////////// line style //////////////////////////


    /**
     * Sets the line style for this dataset.
     *
     * @param style style
     */
    protected void setLineStyle(LineStyle style) {
        this.lineStyle = style;
    }


    ////////////////////////// tool tip style //////////////////////////


    /**
     * Sets the point label format for this dataset.
     *
     * @param format format
     */
    protected void setPointLabelFormat(String format) {
        this.pointLabelFormat = format;
    }

    /**
     * Sets the x-value tooltip format for this dataset.
     *
     * @param format format
     */
    protected void setXToolTipPattern(String format) {
        this.xToolTipPattern = format;
    }

    /**
     * Sets the y-value tooltip format for this dataset.
     *
     * @param format format
     */
    protected void setYToolTipPattern(String format) {
        this.yToolTipPattern = format;
    }

}
