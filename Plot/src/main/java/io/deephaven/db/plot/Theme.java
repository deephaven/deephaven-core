/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot;

import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.gui.color.ColorPalette;
import io.deephaven.gui.color.Paint;

import java.io.Serializable;

/**
 * The theme of a chart.
 *
 * Sets the default colors and fonts of various parts of a chart, such as
 * axes, gridlines, and background colors, axes and figure title fonts.
 * @IncludeAll
 */
@SuppressWarnings("unused")
public interface Theme extends Cloneable, Serializable {


    ////////////////////////// utility functionality //////////////////////////


    /**
     * Deep copy of this theme.
     *
     * @return a deep copy of this theme
     */
    Theme copy();


    ////////////////////////// setters //////////////////////////


    /**
     * Sets the name of the Theme
     *
     * @param name case insensitive
     * @throws RequirementFailure {@code name} must not be null
     * @return this Theme
     */
    Theme name(final String name);

    /**
     * Defines the default Font of the Figure's text.
     * If no Font is set for one of the Theme's properties, this font is used.
     *
     * @param font font
     * @return this Theme
     */
    Theme figureTextFont(final Font font);

    /**
     * Defines the default Font of the Figure's text.
     * If no Font is set for one of the Theme's properties, this font is used.
     *
     * @param family family; if null, set to Arial
     * @param style style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Theme
     */
    Theme figureTextFont(final String family, final String style, final int size);

    /**
     * Defines the color of the default Font of the Figure's text.
     * If no Color is set for one of the Theme's properties, this color is used.
     *
     * @param color color
     * @return this Theme
     */
    Theme figureTextColor(final Paint color);

    /**
     * Defines the color of the default Font of the Figure's text.
     * If no Color is set for one of the Theme's properties, this color is used.
     *
     * @param color color
     * @return this Theme
     */
    Theme figureTextColor(final String color);

    /**
     * Sets the Font of the Figure's title.
     *
     * @param font font
     * @return this Theme
     */
    Theme figureTitleFont(final Font font);

    /**
     * Sets the Font of the Figure's title.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Theme
     */
    Theme figureTitleFont(final String family, final String style, final int size);

    /**
     * Sets the color of the Figure's title.
     *
     * @param color color
     * @return this Theme
     */
    Theme figureTitleColor(final Paint color);

    /**
     * Sets the color of the default Font of the Figure's text.
     * If no Color is set for one of the Theme's properties, this color is used.
     *
     * @param color color
     * @return this Theme
     */
    Theme figureTitleColor(final String color);

    /**
     * Sets the Font of the Chart's title.
     *
     * @param font font
     * @return this Theme
     */
    Theme chartTitleFont(final Font font);

    /**
     * Sets the Font of the Chart's title.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Theme
     */
    Theme chartTitleFont(final String family, final String style, final int size);

    /**
     * Sets the color of the Chart's title.
     *
     * @param color color
     * @return this Theme
     */
    Theme chartTitleColor(final Paint color);

    /**
     * Sets the color of the Chart's title.
     *
     * @param color color
     * @return this Theme
     */
    Theme chartTitleColor(final String color);

    /**
     * Sets the color of the Chart's background.
     *
     * @param color color
     * @return this Theme
     */
    Theme chartBackgroundColor(final Paint color);

    /**
     * Sets the color of the Chart's background.
     *
     * @param color color
     * @return this Theme
     */
    Theme chartBackgroundColor(final String color);

    /**
     * Sets the color of the Chart's grid lines.
     *
     * @param color color
     * @return this Theme
     */
    Theme gridLineColor(final Paint color);

    /**
     * Sets the color of the Chart's grid lines.
     *
     * @param color color
     * @return this Theme
     */
    Theme gridLineColor(final String color);

    /**
     * @param visible whether grid lines will be drawn
     * @return this Theme
     */
    Theme gridLinesVisible(final String visible);

    /**
     * @param visible whether grid lines will be drawn
     * @return this Theme
     */
    Theme gridLinesVisible(final boolean visible);

    /**
     * @param visible whether grid lines in the x direction will be drawn
     * @return this Theme
     */
    Theme xGridLinesVisible(final String visible);

    /**
     * @param visible whether grid lines in the x direction will be drawn
     * @return this Theme
     */
    Theme xGridLinesVisible(final boolean visible);

    /**
     * @param visible whether grid lines in the y direction will be drawn
     * @return this Theme
     */
    Theme yGridLinesVisible(final String visible);

    /**
     * @param visible whether grid lines in the y direction will be drawn
     * @return this Theme
     */
    Theme yGridLinesVisible(final boolean visible);

    /**
     * Sets the Axis's color.
     *
     * @param color color
     * @return this Theme
     */
    Theme axisColor(final Paint color);

    /**
     * Sets the Axis's color.
     *
     * @param color color
     * @return this Theme
     */
    Theme axisColor(final String color);

    /**
     * Sets the Font of the Axis's title.
     *
     * @param font font
     * @return this Theme
     */
    Theme axisTitleFont(final Font font);

    /**
     * Sets the Font of the Axis's title.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Theme
     */
    Theme axisTitleFont(final String family, final String style, final int size);

    /**
     * Sets the color of the Axis's title.
     *
     * @param color color
     * @return this Theme
     */
    Theme axisTitleColor(final Paint color);

    /**
     * Sets the color of the Axis's title.
     *
     * @param color color
     * @return this Theme
     */
    Theme axisTitleColor(final String color);

    /**
     * Sets the Font of the Axis's tick labels.
     *
     * @param font font
     * @return this Theme
     */
    Theme axisTicksFont(final Font font);

    /**
     * Sets the Font of the Axis's tick labels.
     *
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Theme
     */
    Theme axisTicksFont(final String family, final String style, final int size);

    /**
     * Sets the color of the Axis's tick labels.
     *
     * @param color color
     * @return this Theme
     */
    Theme axisTickLabelColor(final Paint color);

    /**
     * Sets the color of the Axis's tick labels.
     *
     * @param color color
     * @return this Theme
     */
    Theme axisTickLabelColor(final String color);

    /**
     * Sets the Font of the legend's text.
     *
     * @param font font
     * @return this Theme
     */
    Theme legendFont(final Font font);

    /**
     * Sets the Font of the legend's text.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Theme
     */
    Theme legendFont(final String family, final String style, final int size);

    /**
     * Sets the color of the legned's text.
     *
     * @param color color
     * @return this Theme
     */
    Theme legendTextColor(final Paint color);

    /**
     * Sets the color of the legend's text.
     *
     * @param color color
     * @return this Theme
     */
    Theme legendTextColor(final String color);

    /**
     * Sets the font used for point labels.
     *
     * @param font font
     * @return this Theme.
     */
    Theme pointLabelFont(final Font font);

    /**
     * Sets the font used for point labels.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Theme.
     */
    Theme pointLabelFont(final String family, final String style, final int size);

    /**
     * Sets the color used for point labels.
     *
     * @param color point label color
     * @return this Theme.
     */
    Theme pointLabelColor(final Paint color);

    /**
     * Sets the color used for point labels.
     *
     * @param color point label color
     * @return this Theme.
     */
    Theme pointLabelColor(final String color);

    /**
     * Sets the ColorPalette of this Theme.
     *
     * @param seriesColorPalette color palette
     * @return this Theme
     */
    Theme seriesColorGenerator(ColorPalette seriesColorPalette);


    ////////////////////////// internal functionality //////////////////////////


    /**
     * Gets the name of this theme.
     *
     * @return name of this theme
     */
    String getName();

    /**
     * Gets the font used for the Figure's title.
     *
     * @return Figure's title font
     */
    Font getFigureTitleFont();

    /**
     * Gets the color used for the title of the Figure.
     *
     * @return color of the Figure's title
     */
    Paint getFigureTitleColor();

    /**
     * Gets the font used for the Chart's title.
     *
     * @return Chart's title font
     */
    Font getChartTitleFont();

    /**
     * Gets the color used for the background of the Chart.
     *
     * @return background color of the Chart
     */
    Paint getChartBackgroundColor();

    /**
     * Gets the font used for the Chart's legend.
     *
     * @return legend's font
     */
    Font getLegendFont();

    /**
     * Gets the color used for the legend's font.
     *
     * @return color of the legend's font
     */
    Paint getLegendTextColor();

    /**
     * Gets the font used for point labels.
     *
     * @return font used for point labels.
     */
    Font getPointLabelFont();

    /**
     * Gets the color used for point labels.
     *
     * @return color used for point labels.
     */
    Paint getPointLabelColor();

    /**
     * Gets the color of the Axes.
     *
     * @return color of the Axes
     */
    Paint getAxisColor();

    /**
     * Gets the font used for the Axis's title.
     *
     * @return Axis's title's font
     */
    Font getAxisTitleFont();

    /**
     * Gets the font used for the Axis's title.
     *
     * @return Axis's title's font
     */
    Font getAxisTicksFont();

    /**
     * Gets the color of the {@code i}-th series in the Chart
     *
     * @param i color index
     * @return i-th color of the Chart's {@link ColorPalette}
     */
    Paint getSeriesColor(final int i);

    /**
     * Gets the {@link Paint} of the figure's text.
     *
     * @return {@link Paint} of the figure's text
     */
    Paint getFigureTextColor();

    /**
     * Gets the {@link Font} of the figure's text.
     *
     * @return {@link Font} of the figure's text
     */
    Font getFigureTextFont();

    /**
     * Gets the {@link Paint} used for the title of each {@link Chart}.
     *
     * @return {@link Paint} used for the title of each {@link Chart}
     */
    Paint getChartTitleColor();

    /**
     * Gets the {@link Paint} used for the background grid lines in each {@link Chart}.
     *
     * @return {@link Paint} of each {@link Chart}'s text
     */
    Paint getGridLineColor();

    /**
     * @return whether to draw the grid lines in the x direction
     */
    boolean getDisplayXGridLines();

    /**
     * @return whether to draw the grid lines in the y direction
     */
    boolean getDisplayYGridLines();

    /**
     * Gets the {@link Paint} used for the color of each {@link Axis}.
     *
     * @return {@link Paint} used for the color of each {@link Axis}
     */
    Paint getAxisTitleColor();

    /**
     * Gets the {@link Paint} used for the color of each tick label.
     *
     * @return {@link Paint} used for the color of each tick label
     */
    Paint getAxisTickLabelColor();

    /**
     * Gets the {@link ColorPalette} used for each {@link Chart}.
     *
     * @return {@link ColorPalette} used for each {@link Chart}
     */
    ColorPalette getSeriesColorPalette();
}
