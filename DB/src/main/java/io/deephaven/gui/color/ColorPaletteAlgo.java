/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.gui.color;

import io.deephaven.base.verify.Require;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Algorithmically generated {@link ColorPalette}.
 */
public class ColorPaletteAlgo implements ColorPalette, Serializable {

    private static final long serialVersionUID = 6883652013745638231L;

    private final ColorPaletteAlgorithm colorAlgorithm;
    private final List<Color> colorCache = new ArrayList<>();

    /**
     * Creates a {@link ColorPalette} from the {@link ColorPaletteAlgorithm}.
     *
     * @param colorAlgorithm algorithm for generating a color palette.
     */
    public ColorPaletteAlgo(final ColorPaletteAlgorithm colorAlgorithm) {
        Require.neqNull(colorAlgorithm, "colorAlgorithm");
        this.colorAlgorithm = colorAlgorithm;
        if (colorAlgorithm.getInitialColor() != null) {
            this.colorCache.add(colorAlgorithm.getInitialColor());
        } else {
            this.colorCache.add(colorAlgorithm.nextColor(null));
        }
    }

    @Override
    public synchronized Color nextColor() {
        final Color current = colorCache.get(colorCache.size() - 1);
        final Color next = colorAlgorithm.nextColor(current);
        colorCache.add(next);
        return next;
    }

    @Override
    public Color get(int index) {
        if (index < 0) {
            throw new IllegalArgumentException("Negative index: " + index);
        }

        while (colorCache.size() <= index) {
            nextColor();
        }

        return colorCache.get(index);
    }

}
