/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.gui.color;

import java.util.Arrays;
import java.util.Random;
import java.util.function.UnaryOperator;

/**
 * Pre-made {@link ColorPaletteAlgorithm}s.
 */
public enum ColorPaletteAlgorithms implements ColorPaletteAlgorithm {
    // GOLDEN algorithm taken from http://martin.ankerl.com/2009/12/09/how-to-create-random-colors-programmatically/
    /**
     * Golden ratio algorithm. Rotates hue values by the inverse golden ratio.
     */
    GOLDEN(c -> {
        final double goldenRatioConjugate = 0.618033988749895;
        java.awt.Color color = c.javaColor();
        final float[] hsbValues = java.awt.Color.RGBtoHSB(color.getRed(), color.getGreen(), color.getBlue(), null);

        hsbValues[0] += goldenRatioConjugate;
        hsbValues[0] %= 1;
        color = new java.awt.Color(java.awt.Color.HSBtoRGB(hsbValues[0], hsbValues[1], hsbValues[2]));
        return new Color(color.getRed(), color.getGreen(), color.getBlue());
    }, new Color(0, 46, 200)),

    // TRIAD_MIXING taken from http://devmag.org.za/2012/07/29/how-to-choose-colours-procedurally-algorithms/
    /**
     * Triad mixing algorithm. Randomly generates and mixes 3 colors and their RGB components.
     */
    TRIAD_MIXING(c -> {
        final Color c1 = new Color(1.0f, 0.0f, 0.0f);
        final Color c2 = new Color(0.0f, 1.0f, 0.0f);
        final Color c3 = new Color(0.0f, 0.0f, 1.0f);

        final float greyControl = 0.05f;
        final int n = randy().nextInt(3);
        float mixRatio1 = randy().nextFloat();
        float mixRatio2 = randy().nextFloat();
        float mixRatio3 = randy().nextFloat();
        switch (n) {
            case 0:
                mixRatio1 *= greyControl;
                break;
            case 1:
                mixRatio2 *= greyControl;
                break;
            case 2:
                mixRatio3 *= greyControl;
                break;
        }

        return new Color(
                (mixRatio1 * c1.javaColor().getRed() + mixRatio2 * c2.javaColor().getRed()
                        + mixRatio3 * c3.javaColor().getRed()) % 1,
                (mixRatio1 * c1.javaColor().getGreen() + mixRatio2 * c2.javaColor().getGreen()
                        + mixRatio3 * c3.javaColor().getGreen()) % 1,
                (mixRatio1 * c1.javaColor().getBlue() + mixRatio2 * c2.javaColor().getBlue()
                        + mixRatio3 * c3.javaColor().getBlue()) % 1);
    }, null);

    private static final Random randy = new Random(134235434);
    private final UnaryOperator<Color> computeNextColor;
    private final Color initialColor;

    ColorPaletteAlgorithms(UnaryOperator<Color> computeNextColor, Color initialColor) {
        this.initialColor = initialColor;
        this.computeNextColor = computeNextColor;
    }

    @Override
    public Color nextColor(Color c) {
        return this.computeNextColor.apply(c);
    }

    @Override
    public Color getInitialColor() {
        return initialColor;
    }

    private static Random randy() {
        return randy;
    }

    /**
     * Gets the algorithm corresponding to the given {@code name}.
     *
     * @param name case insensitive algorithm descriptor
     * @throws IllegalArgumentException {@code name} must not be null
     * @return algorithm corresponding to the given {@code name}
     */
    public static ColorPaletteAlgorithms colorPaletteAlgorithm(
            @SuppressWarnings("ConstantConditions") final String name) {
        if (name == null) {
            throw new IllegalArgumentException("Color palette algorithm can not be null");
        }

        try {
            return valueOf(name.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException("Color palette algorithm " + name + " not found");
        }
    }

    /**
     * Gets the names of the available ColorPaletteAlgorithms.
     *
     * @return array of the available ColorPaletteAlgorithms names
     */
    @SuppressWarnings("unused")
    public static String[] colorPaletteAlgorithmNames() {
        return Arrays.stream(values()).map(Enum::name).toArray(String[]::new);
    }
}
