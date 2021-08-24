/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.gui.color;

/**
 * Algorithm for generating a sequence of {@link Color}s.
 */
public interface ColorPaletteAlgorithm {

    /**
     * Gets the next {@link Color} after {@code c} in the algorithm.
     *
     * @param c color
     * @return result of plugging {@code c} in the algorithm
     */
    Color nextColor(Color c);

    /**
     * Gets the algorithm's starting {@link Color}.
     *
     * @return algorithm's starting {@link Color}
     */
    Color getInitialColor();
}
