/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.gui.color;

import io.deephaven.base.verify.Require;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link ColorPalette} specified by an array of {@link Color}s.
 */
public class ColorPaletteArray implements ColorPalette, Serializable {
    private static final long serialVersionUID = -9146622322078165471L;
    private final Color[] colors;
    private final AtomicInteger counter = new AtomicInteger(-1);

    /**
     * Creates a {@link ColorPalette} with the specified {@code colors}.
     *
     * @param colors colors in the palette
     */
    public ColorPaletteArray(Color[] colors) {
        Require.neqNull(colors, "colors");

        if (colors.length < 1) {
            throw new IllegalArgumentException("Color palette can not be empty");
        }

        this.colors = colors;
    }

    /**
     * Creates a {@link ColorPalette} with the specified {@link Palette} {@code palette}.
     *
     * @param palette color palette
     */
    public ColorPaletteArray(Palette palette) {
        Require.neqNull(palette, "palette");

        if (palette.colors.length < 1) {
            throw new IllegalArgumentException("Color palette can not be empty");
        }

        this.colors = palette.colors;
    }

    /**
     * Creates a {@link ColorPalette} from a standard {@link Palette}.
     *
     * @param palette color palette
     */
    public ColorPaletteArray(final String palette) {
        this(Palette.ofString(palette));
    }

    @Override
    public Color nextColor() {
        final int i = counter.incrementAndGet();
        return get(i);
    }

    @Override
    public Color get(int index) {
        return colors[index % colors.length];
    }

    /**
     * Pre-made color palettes.
     */
    public enum Palette {

        /**
         * Colors used by JavaFX plotting.
         */
        JAVAFX(new Color[] {
                new Color("#f9d900"), // yellow
                new Color("#a9e200"), // green
                new Color("#22bad9"), // light blue
                new Color("#0181e2"), // dark blue
                new Color("#2f357f"), // indigo
                new Color("#860061"), // purple
                new Color("#c62b00"), // red
                new Color("#ff5700"),}), // orange

        /**
         * Colors used in Matlab plotting.
         */
        MATLAB(new Color[] {
                new Color(0f, 0.4470f, 0.7410f), // blue
                new Color(0.85f, 0.3250f, 0.0980f), // orange
                new Color(0.9290f, 0.6940f, 0.1250f), // yellow
                new Color(0.4940f, 0.1840f, 0.5560f), // purple
                new Color(0.4660f, 0.6740f, 0.1880f), // green
                new Color(0.3010f, 0.7450f, 0.9330f), // light blue
                new Color(0.6350f, 0.0780f, 0.1840f), // red
        }),

        /**
         * Colors used in Matplotlib plotting.
         */
        MATPLOTLIB(new Color[] {
                new Color("#d62728"), // red
                new Color("#1f77b4"), // dark blue
                new Color("#2ca02c"), // dark green
                new Color("#ff7f0e"), // orange
                new Color("#9467bd"), // purple
                new Color("#8c564b"), // brown
                new Color("#17becf"), // sky blue
                new Color("#e377c2"), // pink
                new Color("#7f7f7f"), // grey
                new Color("#bcbd22"), // mustard yellow
        });

        private final Color[] colors;

        Palette(Color[] c) {
            this.colors = c;
        }

        /**
         * Gets the colors in this Palette.
         *
         * @return array of colors in this Palette.
         */
        public Color[] getColors() {
            return colors;
        }

        /**
         * Gets the Palette corresponding to the {@code name}.
         *
         * @param name case insensitive Palette descriptor
         * @throws IllegalArgumentException {@code name} can not be null
         * @return Palette corresponding to the {@code name}
         */
        public static Palette ofString(String name) {
            if (name == null) {
                throw new IllegalArgumentException("Palette name can not be null");
            }

            try {
                return valueOf(name.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new UnsupportedOperationException("Color palette " + name + " not found");
            }
        }

    }
}
