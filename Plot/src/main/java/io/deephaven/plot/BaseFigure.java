/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot;

import io.deephaven.gui.color.Paint;

import java.io.Serializable;

/**
 * Container for {@link Chart}s.
 */
public interface BaseFigure extends Serializable {


    ////////////////////////// convenience //////////////////////////


    /**
     * Removes all series with {@code names} from this Figure.
     *
     * @param names series names
     * @return this Figure
     */
    BaseFigure figureRemoveSeries(final String... names);


    ////////////////////////// figure configuration //////////////////////////

    /**
     * Sets the update interval of this Figure. The plot will be redrawn at this update interval.
     *
     * @param updateIntervalMillis update interval, in milliseconds
     * @return this Figure
     */
    BaseFigure updateInterval(final long updateIntervalMillis);

    /**
     * Sets the title of this Figure
     *
     * @param title title
     * @return this Figure
     */
    BaseFigure figureTitle(String title);

    /**
     * Sets the font of this Figure's title
     *
     * @param font font
     * @return this Figure
     */
    BaseFigure figureTitleFont(final Font font);

    /**
     * Sets the font of this Figure's title
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Figure
     */
    BaseFigure figureTitleFont(final String family, final String style, final int size);

    /**
     * Sets the color of this Figure's title
     *
     * @param color color
     * @return this Figure
     */
    BaseFigure figureTitleColor(Paint color);

    /**
     * Sets the color of this Figure's title
     *
     * @param color color
     * @return this Figure
     */
    BaseFigure figureTitleColor(String color);


    ////////////////////////// chart //////////////////////////


    /**
     * Adds a new {@link Chart} to this figure.
     *
     * @throws RuntimeException if no space for the new {@link Chart} exists or can not be made
     * @return the new {@link Chart}. The {@link Chart} is placed in the next available grid space, starting at the
     *         upper left hand corner of the grid, going left to right, top to bottom. If no available space is found in
     *         the grid:
     *         <ul>
     *         <li>if this Figure was created with no specified grid size, then the Figure will resize itself to add the
     *         new {@link Chart};</li>
     *         <li>if not, a RuntimeException will be thrown.</li>
     *         </ul>
     */
    Chart newChart();


    /**
     * Adds a new {@link Chart} to this figure.
     *
     * @param index index from the Figure's grid to remove. The index starts at 0 in the upper left hand corner of the
     *        grid and increases going left to right, top to bottom. E.g. for a 2x2 Figure, the indices would be [0, 1]
     *        [2, 3].
     * @throws RuntimeException if {@code index} is outside this Figure's grid
     * @return the new {@link Chart}. The {@link Chart} is placed at the grid space indicated by the {@code index}.
     */
    Chart newChart(final int index);

    /**
     * Adds a new {@link Chart} to this figure.
     *
     * @param rowNum row index in this Figure's grid. The row index starts at 0.
     * @param colNum column index in this Figure's grid. The column index starts at 0.
     * @throws RuntimeException if the coordinates are outside the Figure's grid
     * @return the new {@link Chart}. The {@link Chart} is placed at the grid space [{@code rowNum}, {@code colNum}.
     */
    Chart newChart(final int rowNum, final int colNum);

    /**
     * Removes a chart from the Figure's grid.
     *
     * @param index index from the Figure's grid to remove. The index starts at 0 in the upper left hand corner of the
     *        grid and increases going left to right, top to bottom. E.g. for a 2x2 Figure, the indices would be [0, 1]
     *        [2, 3].
     * @return this Figure with the chart removed.
     */
    BaseFigure removeChart(final int index);

    /**
     * Removes a chart from the Figure's grid.
     *
     * @param rowNum row index in this Figure's grid. The row index starts at 0.
     * @param colNum column index in this Figure's grid. The column index starts at 0.
     * @throws RuntimeException if the coordinates are outside the Figure's grid
     * @return this Figure with the chart removed.
     */
    BaseFigure removeChart(final int rowNum, final int colNum);

    /**
     * Returns a chart from this Figure's grid.
     *
     * @param index index from the Figure's grid to remove. The index starts at 0 in the upper left hand corner of the
     *        grid and increases going left to right, top to bottom. E.g. for a 2x2 Figure, the indices would be [0, 1]
     *        [2, 3].
     * @throws RuntimeException if the index is outside the Figure's grid
     * @return selected {@link Chart}
     */
    Chart chart(final int index);

    /**
     * Returns a chart from this Figure's grid.
     *
     * @param rowNum row index in this Figure's grid. The row index starts at 0.
     * @param colNum column index in this Figure's grid. The column index starts at 0.
     * @throws RuntimeException if the coordinates are outside the Figure's grid
     * @return selected {@link Chart}
     */
    Chart chart(final int rowNum, final int colNum);


    ////////////////////////// chart rendering //////////////////////////


    /**
     * Saves the Figure as an image.
     *
     * @param saveLocation save location. Must not be null
     * @return figure
     * @throws io.deephaven.base.verify.RequirementFailure saveLocation is null
     */
    default BaseFigure save(String saveLocation) {
        return save(saveLocation, false, -1);
    }

    /**
     * Saves the Figure as an image.
     *
     * @param saveLocation save location. Must not be null
     * @param wait whether to hold the calling thread until the file is written
     * @param timeoutSeconds timeout in seconds to wait.
     * @return figure
     * @throws io.deephaven.base.verify.RequirementFailure saveLocation is null
     */
    default BaseFigure save(String saveLocation, boolean wait, long timeoutSeconds) {
        throw new UnsupportedOperationException(getClass() + " does not implement save");
    }

    /**
     * Saves the Figure as an image.
     *
     * @param saveLocation save location. Must not be null
     * @param width image width
     * @param height image height
     * @return figure
     * @throws io.deephaven.base.verify.RequirementFailure saveLocation is null
     */
    default BaseFigure save(String saveLocation, int width, int height) {
        return save(saveLocation, width, height, false, -1);
    }

    /**
     * Saves the Figure as an image.
     *
     * @param saveLocation save location. Must not be null
     * @param width image width
     * @param height image height
     * @param wait whether to hold the calling thread until the file is written
     * @param timeoutSeconds timeout in seconds to wait.
     * @return figure
     * @throws io.deephaven.base.verify.RequirementFailure saveLocation is null
     */
    default BaseFigure save(String saveLocation, int width, int height, boolean wait, long timeoutSeconds) {
        throw new UnsupportedOperationException(getClass() + " does not implement save");
    }
}
