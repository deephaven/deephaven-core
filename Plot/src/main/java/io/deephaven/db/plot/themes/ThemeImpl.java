/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.themes;

import io.deephaven.base.verify.Require;
import io.deephaven.datastructures.util.HashCodeUtil;
import io.deephaven.gui.color.Color;
import io.deephaven.db.plot.Font;
import io.deephaven.gui.color.Paint;
import io.deephaven.db.plot.Theme;
import io.deephaven.gui.color.ColorPalette;
import io.deephaven.gui.color.ColorPaletteArray;

/**
 * The theme of a chart.
 *
 * Sets the default colors and fonts of various parts of a chart, such as
 * axes, gridlines, and background colors, axes and figure title fonts.
 */
public class ThemeImpl implements Theme {

    private static final long serialVersionUID = 903287793864804850L;
    private static final ColorPaletteArray DEFAULT_SERIES_COLOR_PALETTE = new ColorPaletteArray(ColorPaletteArray.Palette.MATPLOTLIB);

    private String name = "DEFAULT";
    private Paint figureTextColor;
    private Font figureTextFont;
    private Paint figureTitleColor;
    private Font figureTitleFont;
    private Font chartTitleFont;
    private Paint chartTitleColor;
    private Paint chartBackgroundColor;
    private Paint gridLineColor;
    private boolean displayXGridLines = true;
    private boolean displayYGridLines = true;
    private Font axisTitleFont;
    private Font axisTicksFont;
    private Paint axisTitleColor;
    private Paint axisColor;    //covers both ticks and axes. I don't see a reason to split these up
    private Paint axisTickLabelColor;
    private Font legendFont;
    private Paint legendTextColor;
    private Font pointLabelFont;
    private Paint pointLabelColor;
    private ColorPalette seriesColorPalette = DEFAULT_SERIES_COLOR_PALETTE;

    public ThemeImpl() {
    }

    private ThemeImpl(final ThemeImpl theme){
        this.name = theme.name;
        this.figureTextFont = theme.figureTextFont;
        this.figureTextColor = theme.figureTextColor;
        this.figureTitleFont = theme.figureTitleFont;
        this.figureTitleColor = theme.figureTitleColor;
        this.chartTitleFont = theme.chartTitleFont;
        this.chartTitleColor = theme.chartTitleColor;
        this.chartBackgroundColor = theme.chartBackgroundColor;
        this.gridLineColor = theme.gridLineColor;
        this.axisTitleFont = theme.axisTitleFont;
        this.axisTicksFont = theme.axisTicksFont;
        this.axisTitleColor = theme.axisTitleColor;
        this.axisColor = theme.axisColor;
        this.axisTickLabelColor = theme.axisTickLabelColor;
        this.legendFont = theme.legendFont;
        this.legendTextColor = theme.legendTextColor;
        this.seriesColorPalette = theme.seriesColorPalette;
    }


    ////////////////////////// static  //////////////////////////


    ////////////////////////// internal functionality //////////////////////////

    @Override
    public String getName() { return this.name; }

    @Override
    public Font getFigureTitleFont() {
        return getFont(figureTitleFont, "No FigureTitle font or default FigureText font found in theme " + name, true);
    }

    @Override
    public Paint getFigureTitleColor() {
        return getColor(figureTitleColor, "No FigureTitle color found in theme " + name, true);
    }

    @Override
    public Font getChartTitleFont() {
        return getFont(chartTitleFont, "No ChartTitle font or default FigureText font found in theme " + name, true);
    }

    @Override
    public Paint getChartBackgroundColor() {
        return getColor(chartBackgroundColor, "No Background color found in theme " + name, false);
    }

    @Override
    public Font getLegendFont() { return getFont(legendFont, "No Legend font or default FigureText font found in theme " + name, true); }

    @Override
    public Paint getLegendTextColor() { return getColor(legendTextColor, "No Legend color found in theme " + name, true); }

    @Override
    public Font getPointLabelFont() {
        return getFont(pointLabelFont, "No PointLabel font or default FigureText font found in theme " + name, true);
    }

    @Override
    public Paint getPointLabelColor() { return getColor(pointLabelColor, "No PointLabel color found in theme " + name, true); }

    @Override
    public Paint getAxisColor() {
        return getColor(axisColor, "No Axis color found in theme " + name, false);
    }

    @Override
    public Font getAxisTitleFont() {
        return getFont(axisTitleFont, "No TickValues font or default FigureText font found in theme " + name, true);
    }

    @Override
    public Font getAxisTicksFont() {
        return getFont(axisTicksFont, "No TickValues font or default FigureText font found in theme " + name, true);
    }

    @Override
    public Paint getSeriesColor(final int i) {
        return seriesColorPalette.get(i);
    }

    private Font getFont(Font f, String errorMessage, boolean useDefault) {
        if(f != null) {
            return f;
        } else if(figureTextFont != null && useDefault){
            return figureTextFont;
        }

        throw new UnsupportedOperationException(errorMessage);
    }

    private Paint getColor(Paint p, String errorMessage, boolean useDefault) {
        if(p != null) {
            return p;
        } else if(figureTextColor != null && useDefault){
            return figureTextColor;
        }

        throw new UnsupportedOperationException(errorMessage);
    }

    @Override
    public Paint getFigureTextColor() {
        return figureTextColor;
    }

    @Override
    public Font getFigureTextFont() {
        return figureTextFont;
    }

    @Override
    public Paint getChartTitleColor() {
        return chartTitleColor;
    }

    @Override
    public Paint getGridLineColor() {
        return gridLineColor;
    }

    @Override
    public boolean getDisplayXGridLines() {
        return displayXGridLines;
    }

    @Override
    public boolean getDisplayYGridLines() {
        return displayYGridLines;
    }

    @Override
    public Paint getAxisTitleColor() {
        return axisTitleColor;
    }

    @Override
    public Paint getAxisTickLabelColor() {
        return axisTickLabelColor;
    }

    @Override
    public ColorPalette getSeriesColorPalette() {
        return seriesColorPalette;
    }


    ////////////////////////// utility functionality //////////////////////////


    @Override
    public ThemeImpl copy() { return new ThemeImpl(this); }


    ////////////////////////// setters //////////////////////////


    @Override
    public ThemeImpl name(final String name) {
        Require.neqNull(name, "name");

        this.name = name;
        return this;
    }

    @Override
    public ThemeImpl figureTextFont(final Font font) {
        this.figureTextFont = font;
        return this;
    }

    @Override
    public ThemeImpl figureTextFont(final String family, final String style, final int size) {
        return figureTitleFont(Font.font(family, style, size));
    }

    @Override
    public ThemeImpl figureTextColor(final Paint color) {
        this.figureTextColor = color;
        return this;
    }

    @Override
    public ThemeImpl figureTextColor(final String color) {
        return figureTextColor(Color.color(color));
    }

    @Override
    public ThemeImpl figureTitleFont(final Font font) {
        this.figureTitleFont = font;
        return this;
    }

    @Override
    public ThemeImpl figureTitleFont(final String family, final String style, final int size) {
        return figureTitleFont(Font.font(family, style, size));
    }

    @Override
    public ThemeImpl figureTitleColor(final Paint color) {
        this.figureTitleColor = color;
        return this;
    }

    @Override
    public ThemeImpl figureTitleColor(final String color) {
        return figureTitleColor(Color.color(color));
    }

    @Override
    public ThemeImpl chartTitleFont(final Font font) {
        this.chartTitleFont = font;
        return this;
    }

    @Override
    public ThemeImpl chartTitleFont(final String family, final String style, final int size) {
        return chartTitleFont(Font.font(family, style, size));
    }

    @Override
    public ThemeImpl chartTitleColor(final Paint color) {
        this.chartTitleColor = color;
        return this;
    }

    @Override
    public ThemeImpl chartTitleColor(final String color) {
        return chartTitleColor(Color.color(color));
    }

    @Override
    public ThemeImpl chartBackgroundColor(final Paint color) {
        this.chartBackgroundColor = color;
        return this;
    }

    @Override
    public ThemeImpl chartBackgroundColor(final String color) {
        return chartBackgroundColor(Color.color(color));
    }

    @Override
    public ThemeImpl gridLineColor(final Paint color) {
        this.gridLineColor = color;
        return this;
    }

    @Override
    public ThemeImpl gridLineColor(final String color) {
        return gridLineColor(Color.color(color));
    }

    @Override
    public ThemeImpl gridLinesVisible(final String visible) {
        return gridLinesVisible(Boolean.parseBoolean(visible));
    }

    @Override
    public ThemeImpl gridLinesVisible(final boolean visible) {
        xGridLinesVisible(visible);
        yGridLinesVisible(visible);
        return this;
    }

    @Override
    public ThemeImpl xGridLinesVisible(final String visible) {
        return xGridLinesVisible(Boolean.parseBoolean(visible));
    }

    @Override
    public ThemeImpl xGridLinesVisible(final boolean visible) {
        this.displayXGridLines = visible;
        return this;
    }

    @Override
    public ThemeImpl yGridLinesVisible(final String visible) {
        return yGridLinesVisible(Boolean.parseBoolean(visible));
    }

    @Override
    public ThemeImpl yGridLinesVisible(final boolean visible) {
        this.displayYGridLines = visible;
        return this;
    }

    @Override
    public ThemeImpl axisColor(final Paint color) {
        this.axisColor = color;
        return this;
    }

    @Override
    public ThemeImpl axisColor(final String color) {
        return axisColor(Color.color(color));
    }

    @Override
    public ThemeImpl axisTitleFont(final Font font) {
        this.axisTitleFont = font;
        return this;
    }

    @Override
    public ThemeImpl axisTitleFont(final String family, final String style, final int size) {
        return axisTitleFont(Font.font(family, style, size));
    }

    @Override
    public ThemeImpl axisTitleColor(final Paint color) {
        this.axisTitleColor = color;
        return this;
    }

    @Override
    public ThemeImpl axisTitleColor(final String color) {
        return axisTitleColor(Color.color(color));
    }

    @Override
    public ThemeImpl axisTicksFont(final Font font) {
        this.axisTicksFont = font;
        return this;
    }

    @Override
    public ThemeImpl axisTicksFont(final String family, final String style, final int size) {
        return axisTicksFont(Font.font(family, style, size));
    }

    @Override
    public ThemeImpl axisTickLabelColor(final Paint color) {
        this.axisTickLabelColor = color;
        return this;
    }

    @Override
    public ThemeImpl axisTickLabelColor(final String color) {
        return axisTickLabelColor(Color.color(color));
    }

    @Override
    public ThemeImpl legendFont(final Font font) {
        legendFont = font;
        return this;
    }

    @Override
    public ThemeImpl legendFont(final String family, final String style, final int size) {
        return legendFont(Font.font(family, style, size));
    }

    @Override
    public ThemeImpl legendTextColor(final Paint color) {
        this.legendTextColor = color;
        return this;
    }

    @Override
    public ThemeImpl legendTextColor(final String color) {
        return legendTextColor(Color.color(color));
    }

    @Override
    public ThemeImpl pointLabelFont(final Font font) {
        pointLabelFont = font;
        return this;
    }

    @Override
    public ThemeImpl pointLabelFont(final String family, final String style, final int size) {
        return pointLabelFont(Font.font(family, style, size));
    }

    @Override
    public ThemeImpl pointLabelColor(final Paint color) {
        this.pointLabelColor = color;
        return this;
    }

    @Override
    public ThemeImpl pointLabelColor(final String color) {
        return pointLabelColor(Color.color(color));
    }

    @Override
    public ThemeImpl seriesColorGenerator(ColorPalette seriesColorPalette) {
        this.seriesColorPalette = seriesColorPalette == null ? DEFAULT_SERIES_COLOR_PALETTE : seriesColorPalette;
        return this;
    }


    ///////////////////////// Object  /////////////////////////


    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Theme) {
            final ThemeImpl other = (ThemeImpl) obj;
            return nullSafeEquals(other.name, this.name)
                    && nullSafeEquals(other.figureTextColor, this.figureTextColor)
                    && nullSafeEquals(other.figureTextFont, this.figureTextFont)
                    && nullSafeEquals(other.figureTitleColor, this.figureTitleColor)
                    && nullSafeEquals(other.figureTitleFont, this.figureTitleFont)
                    && nullSafeEquals(other.chartTitleFont, this.chartTitleFont)
                    && nullSafeEquals(other.chartTitleColor, this.chartTitleColor)
                    && nullSafeEquals(other.chartBackgroundColor, this.chartBackgroundColor)
                    && nullSafeEquals(other.gridLineColor, this.gridLineColor)
                    && nullSafeEquals(other.axisTitleFont, this.axisTitleFont)
                    && nullSafeEquals(other.axisTicksFont, this.axisTicksFont)
                    && nullSafeEquals(other.axisTitleColor, this.axisTitleColor)
                    && nullSafeEquals(other.axisColor, this.axisColor)
                    && nullSafeEquals(other.axisTickLabelColor, this.axisTickLabelColor)
                    && nullSafeEquals(other.legendFont, this.legendFont)
                    && nullSafeEquals(other.legendTextColor, this.legendTextColor)
                    && nullSafeEquals(other.pointLabelFont, this.pointLabelFont)
                    && nullSafeEquals(other.pointLabelColor, this.pointLabelColor)
                    && nullSafeEquals(other.seriesColorPalette, this.seriesColorPalette);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.combineHashCodes(name, figureTextColor, figureTextFont, figureTitleColor, figureTitleFont, chartTitleFont, chartTitleColor, chartBackgroundColor, gridLineColor, axisTitleFont, axisTicksFont, axisTitleColor, axisColor, axisTickLabelColor, legendFont, legendTextColor, pointLabelFont, pointLabelColor, seriesColorPalette);
    }

    private boolean nullSafeEquals(final Object obj1, final Object obj2) {
        return obj1 == null ? obj2 == null : obj1.equals(obj2);
    }

}
