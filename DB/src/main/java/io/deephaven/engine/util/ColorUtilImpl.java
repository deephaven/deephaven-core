/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util;

import io.deephaven.gui.color.Color;
import io.deephaven.gui.color.Colors;
import gnu.trove.map.hash.TObjectIntHashMap;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Formatting methods from ColorUtil. Exists so that we can statically import the ColorUtil methods without importing
 * the color fields. TODO: remove once {@link ColorUtil} field and {@link Color} field conflicts are resolved. TODO:
 * This class won't be necessary once we can import ColorUtil as static again. TODO
 * (deephaven/deephaven-core/issues/175): Move this to a new module and package
 */
public class ColorUtilImpl {
    public static final long COLOR_SET_BIT = 0x01;
    public static final long SELECTION_OVERRIDE_BIT = 0x02;
    public static final long FOREGROUND_SET_BIT = 0x04;

    ////////////////////////////////////// Background methods //////////////////////////////////////


    /**
     * Creates a table format encoding with background color equal to the input RGB and unformatted foreground.
     *
     * @param r red component
     * @param g green component
     * @param b blue component
     * @return table format encoding the RGB background color and an unformatted foreground.
     */
    private static long background(long r, long g, long b) {
        return (COLOR_SET_BIT << 56) |
                (r << 48) |
                (g << 40) |
                (b << 32);
    }

    /**
     * Creates a table format encoding with specified background color and an unformatted foreground.
     *
     * @param color color encoding
     * @return table format encoding with specified background color and unformatted foreground
     */
    private static long background(long color) {
        return color & 0xffffffff00000000L;
    }

    /**
     * Creates a table format encoding with specified background color and an unformatted foreground.
     *
     * @param color color represented by a {@link Color}
     * @return table format encoding with specified background color and unformatted foreground
     */
    public static long background(Color color) {
        return background(toLong(color));
    }

    /**
     * Creates a table format encoding with specified background color and an unformatted foreground.
     *
     * @param color the hex representation or the case-insensitive color name
     * @return table format encoding with specified background color and unformatted foreground
     */
    public static long background(String color) {
        return background(toLong(color));
    }

    /**
     * Convenience method for {@link #background(Color)}.
     * <p>
     * This variant takes color RBG components as inputs.
     * </p>
     */
    public static long bg(long r, long g, long b) {
        return background(r, g, b);
    }

    /**
     * Convenience method for {@link #background(Color)}.
     * <p>
     * This variant takes the input color encoded as a long.
     * </p>
     */
    public static long bg(long color) {
        return background(color);
    }

    /**
     * Convenience method for {@link #background(Color)}.
     * <p>
     * This variant takes the input color as a {@link Color}.
     * </p>
     */
    public static long bg(Color color) {
        return background(toLong(color));
    }

    /**
     * Convenience method for {@link #background(Color)}.
     * <p>
     * This variant takes the input color as a {@link String}. This may be the hex representation or the
     * case-insensitive color name.
     * </p>
     */
    public static long bg(String color) {
        return background(toLong(color));
    }

    /**
     * Checks if a background color should override selection.
     *
     * @param color the color to check
     * @return true if the color should override selection, false otherwise
     */
    public static boolean isBackgroundSelectionOverride(long color) {
        return (color & (SELECTION_OVERRIDE_BIT << 56)) != 0;
    }

    /**
     * Background with Override .. Overrides the default selection color.
     */
    public static long backgroundOverride(long color) {
        return bg(color) | SELECTION_OVERRIDE_BIT << 56;
    }

    /**
     * Background with Override .. Overrides the default selection color.
     */
    public static long backgroundOverride(Color color) {
        return backgroundOverride(toLong(color));
    }

    /**
     * Background with Override .. Overrides the default selection color.
     */
    public static long backgroundOverride(String color) {
        return backgroundOverride(toLong(color));
    }

    /**
     * Convenience method for {@link #backgroundOverride(Color)}.
     * <p>
     * This variant takes the input color encoded as a long.
     * </p>
     */
    public static long bgo(long color) {
        return backgroundOverride(color);
    }

    /**
     * Convenience method for {@link #backgroundOverride(Color)}.
     * <p>
     * This variant takes the input color as a {@link Color}.
     * </p>
     */
    public static long bgo(Color color) {
        return backgroundOverride(color);
    }

    /**
     * Convenience method for {@link #backgroundOverride(Color)}.
     * <p>
     * This variant takes the input color as a {@link String}. This may be the hex representation or the
     * case-insensitive color name.
     * </p>
     */
    public static long bgo(String color) {
        return backgroundOverride(color);
    }


    ////////////////////////////////////// Foreground methods //////////////////////////////////////


    /**
     * Creates a table format encoding with foreground color equal to the input RGB and unformatted background.
     *
     * @param r red component
     * @param g green component
     * @param b blue component
     * @return table format encoding the RGB foreground color and an unformatted background.
     */
    private static long foreground(long r, long g, long b) {
        return (COLOR_SET_BIT << 24) |
                (r << 16) |
                (g << 8) |
                (b) |
                FOREGROUND_SET_BIT << 24;
    }

    /**
     * Creates a table format encoding with specified foreground color and unformatted background.
     *
     * @param color color encoding
     * @return table format encoding with specified foreground color and unformatted background
     */
    private static long foreground(long color) {
        return (color & 0xffffffff00000000L) >>> 32 | FOREGROUND_SET_BIT << 24;
    }

    /**
     * Checks if the foreground has already been shifted.
     *
     * @param color the color to check
     * @return true if the the color is shifted, false otherwise
     */
    static boolean isForegroundSet(long color) {
        return (color & FOREGROUND_SET_BIT << 24) != 0;
    }

    /**
     * Creates a table format encoding with specified foreground color and unformatted background.
     *
     * @param color color represented by a {@link Color}
     * @return table format encoding with specified foreground color and unformatted background
     */
    public static long foreground(Color color) {
        return foreground(toLong(color));
    }

    /**
     * Creates a table format encoding with specified foreground color and unformatted background.
     *
     * @param color the hex representation or the case-insensitive color name
     * @return table format encoding with specified foreground color and unformatted background
     */
    public static long foreground(String color) {
        return foreground(toLong(color));
    }

    /**
     * Convenience method for {@link #foreground(Color)}.
     * <p>
     * This variant takes the input color encoded as a long. The resultant encoding is an unformatted background and a
     * foreground the same color as the background color of the input, e.g. fg(bgfg(YELLOW, RED)) will result in a
     * yellow foreground with no background formatting.
     * </p>
     */
    public static long fg(long color) {
        return foreground(color);
    }

    /**
     * Convenience method for {@link #foreground(Color)}.
     * <p>
     * This variant takes the input color as a {@link Color}.
     * </p>
     */
    public static long fg(Color color) {
        return foreground(toLong(color));
    }

    /**
     * Convenience method for {@link #foreground(Color)}.
     * <p>
     * This variant takes the input color as a {@link String}. This may be the hex representation or the
     * case-insensitive color name.
     * </p>
     */
    public static long fg(String color) {
        return foreground(toLong(color));
    }

    /**
     * Convenience method for {@link #foreground(Color)}.
     * <p>
     * This variant takes color RBG components as inputs.
     * </p>
     */
    public static long fg(long r, long g, long b) {
        return foreground(r, g, b);
    }

    /**
     * Checks if a foreground color should override selection.
     *
     * @param color the color to check
     * @return true if the color should override selection, false otherwise
     */
    public static boolean isForegroundSelectionOverride(long color) {
        return (color & (SELECTION_OVERRIDE_BIT << 24)) != 0;
    }

    /**
     * Foreground with Override .. Overrides the default selection color.
     */
    public static long foregroundOverride(long color) {
        return fg(color) | SELECTION_OVERRIDE_BIT << 24;
    }

    /**
     * Foreground with Override .. Overrides the default selection color.
     */
    public static long foregroundOverride(Color color) {
        return foregroundOverride(toLong(color));
    }

    /**
     * Foreground with Override .. Overrides the default selection color.
     */
    public static long foregroundOverride(String color) {
        return foregroundOverride(toLong(color));
    }

    /**
     * Convenience method for {@link #foregroundOverride(Color)}.
     * <p>
     * This variant takes the input color encoded as a long.
     * </p>
     */
    public static long fgo(long color) {
        return foregroundOverride(color);
    }

    /**
     * Convenience method for {@link #foregroundOverride(Color)}.
     * <p>
     * This variant takes the input color as a {@link Color}.
     * </p>
     */
    public static long fgo(Color color) {
        return foregroundOverride(color);
    }

    /**
     * Convenience method for {@link #foregroundOverride(Color)}.
     * <p>
     * This variant takes the input color as a {@link String}. This may be the hex representation or the
     * case-insensitive color name.
     * </p>
     */
    public static long fgo(String color) {
        return foregroundOverride(color);
    }

    ////////////////////////////////////// BackgroundForeground methods //////////////////////////////////////


    /**
     * Creates a table format encoding with specified foreground and background colors.
     *
     * @param bgr red component of the background color
     * @param bgg green component of the background color
     * @param bgb blue component of the background color
     * @param fgr red component of the foreground color
     * @param fgg green component of the foreground color
     * @param fgb blue component of the foreground color
     * @return table format encoding with specified foreground and background colors
     */
    private static long backgroundForeground(long bgr, long bgg, long bgb, long fgr, long fgg, long fgb) {
        return background(bgr, bgg, bgb) | foreground(fgr, fgg, fgb);
    }

    /**
     * Creates a table format encoding with specified foreground and background colors.
     *
     * @param bg background color
     * @param fg foreground color
     * @return table format encoding with specified foreground and background colors
     */
    private static long backgroundForeground(long bg, long fg) {
        return background(bg) | (isForegroundSet(fg) ? fg : foreground(fg));
    }

    /**
     * Creates a table format encoding with specified foreground and background colors.
     *
     * @param bg background color represented by a {@link Color}
     * @param fg foreground color represented by a {@link Color}
     * @return table format encoding with specified foreground and background colors
     */
    public static long backgroundForeground(Color bg, Color fg) {
        return backgroundForeground(toLong(bg), toLong(fg));
    }

    /**
     * Creates a table format encoding with specified foreground and background colors.
     *
     * @param bg background color represented by a {@link String}. This may be the hex representation or the
     *        case-insensitive color name
     * @param fg foreground color represented by a {@link String}. This may be the hex representation or the
     *        case-insensitive color name
     * @return table format encoding with specified foreground and background colors
     */
    public static long backgroundForeground(String bg, String fg) {
        return backgroundForeground(toLong(bg), toLong(fg));
    }

    /**
     * Convenience method for {@link #backgroundForeground(Color, Color)}.
     * <p>
     * This variant takes color RBG components as inputs.
     * </p>
     */
    public static long bgfg(long bgr, long bgg, long bgb, long fgr, long fgg, long fgb) {
        return backgroundForeground(bgr, bgg, bgb, fgr, fgg, fgb);
    }

    /**
     * Convenience method for {@link #backgroundForeground(Color, Color)}.
     * <p>
     * This variant takes colors encoded as longs for inputs.
     * </p>
     */
    public static long bgfg(long bg, long fg) {
        return backgroundForeground(bg, fg);
    }

    /**
     * Convenience method for {@link #backgroundForeground(Color, Color)}.
     * <p>
     * This variant takes the input colors as {@link Color}s.
     * </p>
     */
    public static long bgfg(Color bg, Color fg) {
        return backgroundForeground(toLong(bg), toLong(fg));
    }

    /**
     * Convenience method for {@link #backgroundForeground(Color, Color)}.
     * <p>
     * This variant takes the input colors as {@link String}s. This may be the hex representation or the
     * case-insensitive color name.
     * </p>
     */
    public static long bgfg(String bg, String fg) {
        return backgroundForeground(toLong(bg), toLong(fg));
    }


    ////////////////////////////////////// BackgroundForegroundAuto methods //////////////////////////////////////


    /**
     * Creates a table format encoding with specified background color and automatically chosen contrasting foreground
     * color.
     *
     * @param bgr red component of the background color
     * @param bgg green component of the background color
     * @param bgb blue component of the background color
     * @return table format encoding with background color and auto-generated foreground color
     */
    private static long backgroundForegroundAuto(long bgr, long bgg, long bgb) {
        final long bg = background(bgr, bgg, bgb);
        final long fg = yiq(bgr, bgg, bgb) >= 128 ? 0x01000000L : 0x01e0e0e0L;
        return bg | fg;
    }

    /**
     * Creates a table format encoding with specified background color and automatically chosen contrasting foreground
     * color.
     *
     * @param color background color
     * @return table format encoding with background color and auto-generated foreground color
     */
    private static long backgroundForegroundAuto(long color) {
        final long bg = background(color);
        final long fg = yiq(color) >= 128 ? 0x01000000L : 0x01e0e0e0L;
        return bg | fg;
    }

    /**
     * Creates a table format encoding with specified background color and automatically chosen contrasting foreground
     * color.
     *
     * @param color background color represented by a {@link Color}
     * @return table format encoding with background color and auto-generated foreground color
     */
    public static long backgroundForegroundAuto(Color color) {
        return backgroundForegroundAuto(toLong(color));
    }

    /**
     * Creates a table format encoding with specified background color and automatically chosen contrasting foreground
     * color.
     *
     * @param color background color represented by a {@link String}. This may be the hex representation or the
     *        case-insensitive color name
     * @return table format encoding with background color and auto-generated foreground color
     */
    public static long backgroundForegroundAuto(String color) {
        return backgroundForegroundAuto(toLong(color));
    }

    /**
     * Convenience method for {@link #backgroundForegroundAuto(Color)}
     * <p>
     * This variant takes color RBG components as inputs.
     * </p>
     */
    public static long bgfga(long bgr, long bgg, long bgb) {
        return backgroundForegroundAuto(bgr, bgg, bgb);
    }

    /**
     * Convenience method for {@link #backgroundForegroundAuto(Color)}
     * <p>
     * This variant takes the input color encoded as a long.
     * </p>
     */
    public static long bgfga(long color) {
        return backgroundForegroundAuto(color);
    }

    /**
     * Convenience method for {@link #backgroundForegroundAuto(Color)}
     *
     * <p>
     * This variant takes the input color as a {@link Color}.
     * </p>
     */
    public static long bgfga(Color color) {
        return backgroundForegroundAuto(color);
    }

    /**
     * Convenience method for {@link #backgroundForegroundAuto(Color)}
     *
     * <p>
     * This variant takes the input color as a {@link String}. This may be the hex representation or the
     * case-insensitive color name.
     * </p>
     */
    public static long bgfga(String color) {
        return backgroundForegroundAuto(color);
    }


    ////////////////////////////////////// heatmap methods //////////////////////////////////////


    /**
     * Creates a table format encoding for the heat map at {@code value}. A contrasting foreground color is
     * automatically chosen.
     *
     * @param value determines the color used by its location in the heat map's range
     * @param min minimum value of the heat map range
     * @param max maximum value of the heat map range
     * @param bg1 background color at or below the minimum value of the heat map. Encoded as a long
     * @param bg2 background color at or above the maximum value of the heat map. Encoded as a long
     * @return table format encoding with background color and auto-generated foreground color determined by a heat map
     */
    public static long heatmap(double value, double min, double max, long bg1, long bg2) {
        if (value <= min) {
            return bg1;
        } else if (value >= max) {
            return bg2;
        } else {
            double pert = (value - min) / (max - min);

            long r1 = (bg1 >> 48) & 0xFF;
            long g1 = (bg1 >> 40) & 0xFF;
            long b1 = (bg1 >> 32) & 0xFF;

            long r2 = (bg2 >> 48) & 0xFF;
            long g2 = (bg2 >> 40) & 0xFF;
            long b2 = (bg2 >> 32) & 0xFF;

            return bgfga((long) (r1 + pert * (r2 - r1)), (long) (g1 + pert * (g2 - g1)),
                    (long) (b1 + pert * (b2 - b1)));
        }
    }

    /**
     * Creates a table format encoding for the heat map at {@code value}. A contrasting foreground color is
     * automatically chosen.
     *
     * @param value determines the color used by its location in the heat map's range
     * @param min minimum value of the heat map range
     * @param max maximum value of the heat map range
     * @param bg1 background color at or below the minimum value of the heat map.
     * @param bg2 background color at or above the maximum value of the heat map.
     * @return table format encoding with background color and auto-generated foreground color determined by a heat map
     */
    public static long heatmap(double value, double min, double max, Color bg1, Color bg2) {
        return heatmap(value, min, max, toLong(bg1), toLong(bg2));
    }

    /**
     * Creates a table format encoding for the heat map at {@code value}. A contrasting foreground color is
     * automatically chosen.
     *
     * @param value determines the color used by its location in the heat map's range
     * @param min minimum value of the heat map range
     * @param max maximum value of the heat map range
     * @param bg1 background color at or below the minimum value of the heat map.
     * @param bg2 background color at or above the maximum value of the heat map.
     * @return table format encoding with background color and auto-generated foreground color determined by a heat map
     */
    public static long heatmap(double value, double min, double max, String bg1, String bg2) {
        return heatmap(value, min, max, toLong(bg1), toLong(bg2));
    }


    ////////////////////////////////////// heatmapForeground methods //////////////////////////////////////


    /**
     * Creates a table format encoding for the heat map at {@code value} with specified foreground color and unformatted
     * background.
     * <p>
     * Note that fg1 and fg2 must be encoded as foreground colors for this to work as expected.
     * </p>
     * 
     * @param value determines the color used by its location in the heat map's range
     * @param min minimum value of the heat map range.
     * @param max maximum value of the heat map range.
     * @param fg1 foreground color at or below the minimum value of the heat map. Encoded as a long
     * @param fg2 foreground color at or above the maximum value of the heat map. Encoded as a long
     * @return table format encoding with foreground color determined by a heat map
     */
    private static long heatmapForeground(double value, double min, double max, long fg1, long fg2) {
        if (value <= min) {
            return fg1;
        } else if (value >= max) {
            return fg2;
        } else {
            double pert = (value - min) / (max - min);

            long r1 = (fg1 >> 16) & 0xFF;
            long g1 = (fg1 >> 8) & 0xFF;
            long b1 = (fg1) & 0xFF;

            long r2 = (fg2 >> 16) & 0xFF;
            long g2 = (fg2 >> 8) & 0xFF;
            long b2 = (fg2) & 0xFF;

            return fg((long) (r1 + pert * (r2 - r1)), (long) (g1 + pert * (g2 - g1)), (long) (b1 + pert * (b2 - b1)));
        }
    }

    /**
     * Creates a table format encoding for the heat map at {@code value} with specified foreground color and unformatted
     * background.
     *
     * @param value determines the color used by its location in the heat map's range
     * @param min minimum value of the heat map range.
     * @param max maximum value of the heat map range.
     * @param fg1 foreground color at or below the minimum value of the heat map
     * @param fg2 foreground color at or above the maximum value of the heat map
     * @return table format encoding with foreground color determined by a heat map
     */
    public static long heatmapForeground(double value, double min, double max, Color fg1, Color fg2) {
        return heatmapForeground(value, min, max, fg(fg1), fg(fg2));
    }

    /**
     * Creates a table format encoding for the heat map at {@code value} with specified foreground color and unformatted
     * background.
     *
     * @param value determines the color used by its location in the heat map's range
     * @param min minimum value of the heat map range.
     * @param max maximum value of the heat map range.
     * @param fg1 foreground color at or below the minimum value of the heat map
     * @param fg2 foreground color at or above the maximum value of the heat map
     * @return table format encoding with foreground color determined by a heat map
     */
    public static long heatmapForeground(double value, double min, double max, String fg1, String fg2) {
        return heatmapForeground(value, min, max, fg(fg1), fg(fg2));
    }

    /**
     * Convenience method for {@link #heatmapForeground(double, double, double, Color, Color)}
     * <p>
     * This variant takes the input colors encoded as a longs. These colors must be formatted as a foreground (i.e.
     * through fg() call) for this to work as expected.
     * </p>
     */
    public static long heatmapFg(double value, double min, double max, long fg1, long fg2) {
        return heatmapForeground(value, min, max, fg1, fg2);
    }

    /**
     * See {@link #heatmapFg(double, double, double, long, long)}
     * <p>
     * This variant takes the input colors as {@link Color}s.
     * <p>
     */
    public static long heatmapFg(double value, double min, double max, Color fg1, Color fg2) {
        return heatmapForeground(value, min, max, fg(fg1), fg(fg2));
    }

    /**
     * See {@link #heatmapFg(double, double, double, long, long)}
     * <p>
     * This variant takes the input colors as {@link String}s. This may be the hex representation or the
     * case-insensitive color name.
     * <p>
     */
    public static long heatmapFg(double value, double min, double max, String fg1, String fg2) {
        return heatmapForeground(value, min, max, fg(fg1), fg(fg2));
    }


    ////////////////////////////////////// Converters //////////////////////////////////////


    /**
     * Gets the formatting long of the input color
     *
     * @param color color encoded in a long
     * @return formatting long of the input color
     */
    public static long toLong(final long color) {
        return color == NULL_LONG ? 0 : color;
    }

    /**
     * Gets the formatting long of the input color
     *
     * @param color color represented by a {@link Color}
     * @return formatting long of the input color
     */
    public static long toLong(final Color color) {
        return color == null || color == Color.NO_FORMATTING ? 0
                : backgroundForegroundAuto(color.javaColor().getRed(), color.javaColor().getGreen(),
                        color.javaColor().getBlue());
    }

    /**
     * Gets the formatting long of the input color
     *
     * @param color the hex representation or the case-insensitive color name
     * @return formatting long of the input color
     * @throws IllegalArgumentException If {@code color} is invalid
     */
    public static long toLong(final String color) {
        return color == null || color.toUpperCase().equals(Colors.NO_FORMATTING.name().toUpperCase()) ? 0
                : toLong(Color.color(color));
    }

    /**
     * Returns the {@code long} value for the color with the given name. For example,
     * {@code ColorUtil.valueOf("VIVID_BLUE") == Color.VIVID_BLUE}.
     * <p>
     * This method has been deprecated. The new method is {@link #toLong(String)}
     * <p>
     * 
     * @param colorName The name of the color to retrieve.
     * @return The {@code long} value for the given color
     * @throws IllegalArgumentException If {@code colorName} is invalid
     */
    @Deprecated
    public static long valueOf(String colorName) {
        return toLong(colorName);
    }


    ////////////////////////////////////// Internal //////////////////////////////////////


    private static long yiq(long r, long g, long b) {
        return (r * 299 + g * 587 + b * 114) / 1000;
    }

    private static long yiq(long color) {
        final long r = extractBits(color, 48, 56);
        final long g = extractBits(color, 40, 48);
        final long b = extractBits(color, 32, 40);
        return (r * 299 + g * 587 + b * 114) / 1000;
    }

    private static long extractBits(final long value, final int begin, final int end) {
        final long mask = (1 << (end - begin)) - 1;
        return (value >> begin) & mask;
    }

    /**
     * Creates distinct and unique coloration for each unique input value.
     */
    public static class DistinctFormatter {
        private final TObjectIntHashMap<Object> valueToCount = new TObjectIntHashMap<>();
        private final int noEntryValue = valueToCount.getNoEntryValue();

        public Long getColor(Object value) {
            int count;
            synchronized (valueToCount) {
                count = valueToCount.get(value);

                if (count == noEntryValue) {
                    count = valueToCount.size() + 1;

                    valueToCount.put(value, count);
                }
            }

            float hue = (float) (count * (1 + Math.sqrt(5)) / 2);

            int rgb = java.awt.Color.HSBtoRGB(hue, .68f, .68f);

            long r = (rgb >> 16) & 0xFF;
            long g = (rgb >> 8) & 0xFF;
            long b = (rgb) & 0xFF;

            return bgfga(r, g, b);
        }
    }
}
