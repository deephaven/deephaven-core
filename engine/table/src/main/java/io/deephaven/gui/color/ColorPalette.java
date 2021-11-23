/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.gui.color;

/**
 * Palette of {@link Color}s.
 */
public interface ColorPalette {

    /**
     * Gets the next {@link Color} in the palette.
     *
     * @return next {@link Color} in the palette.
     */
    Color nextColor();

    /**
     * Gets the {@link Color} at the {@code rowSet} in the palette.
     *
     * @param index rowSet
     * @return {@code rowSet}th {@link Color} in the palette.
     */
    Color get(int index);
}
