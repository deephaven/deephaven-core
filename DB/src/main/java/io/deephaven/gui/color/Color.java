/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.gui.color;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A color. TODO (deephaven/deephaven-core/issues/175): Move this to a new module and package
 */
@SuppressWarnings("WeakerAccess")
public class Color implements Paint, Serializable {
    private static final long serialVersionUID = 989077634719561909L;


    ////////////////////////// color definition //////////////////////////


    private final java.awt.Color color;

    /**
     * Creates a Color instance represented by the {@code color} String.
     *
     * Colors are specified by name or hex value.
     *
     * Hex values are parsed as follows: first two digits set the Red component of the color; second
     * two digits set the Green component; third two the Blue. Hex values must have a "#" in front,
     * e.g. "#001122"
     *
     * For available names, see {@link Color} and {@link #colorNames}.
     *
     * @param color color; may be hex representation or case-insensitive color name
     * @throws IllegalArgumentException {@code color} may not be null
     */
    public Color(@SuppressWarnings("ConstantConditions") final String color) {
        if (color == null) {
            throw new IllegalArgumentException("Color can not be null");
        }

        if (color.contains("#")) {
            // doesn't look like this supports rgba, just rgb
            this.color = java.awt.Color.decode(color);
        } else {
            try {
                this.color = Colors.valueOf(color.toUpperCase()).color().javaColor();
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Invalid color name: " + color);
            }
        }
    }

    /**
     * Creates a Color with the specified red, green, and blue values in the range (0 - 255). Alpha
     * is defaulted to 255.
     *
     * @throws IllegalArgumentException if {@code r}, {@code g} or {@code b} values are outside of
     *         the range 0 to 255, inclusive
     * @param r the red component
     * @param g the green component
     * @param b the blue component
     */
    public Color(final int r, final int g, final int b) {
        color = new java.awt.Color(r, g, b);
    }

    /**
     * Creates a Color with the specified red, green, blue, and alpha values in the range (0 - 255).
     * The lower the alpha, the more transparent the color.
     *
     * @throws IllegalArgumentException if {@code r}, {@code g} {@code b}, or {@code a} values are
     *         outside of the range 0 to 255, inclusive
     * @param r the red component
     * @param g the green component
     * @param b the blue component
     * @param a the alpha component
     */
    public Color(final int r, final int g, final int b, final int a) {
        color = new java.awt.Color(r, g, b, a);
    }

    /**
     * Creates a Color with the specified combined {@code rgb} value consisting of the red component
     * in bits 16-23, the green component in bits 8-15, and the blue component in bits 0-7. Alpha is
     * defaulted to 255.
     *
     * @param rgb the combined RGB components
     */
    public Color(final int rgb) {
        color = new java.awt.Color(rgb);
    }

    /**
     * Creates a Color with the specified combined {@code rgba} value consisting of the alpha
     * component in bits 24-31, the red component in bits 16-23, the green component in bits 8-15,
     * and the blue component in bits 0-7. If {@code hasAlpha} is false, alpha is defaulted to 255.
     *
     * @param rgba the combined rbga components
     * @param hasAlpha if true, {@code rgba} is parsed with an alpha component. Otherwise, alpha
     *        defaults to 255
     */
    public Color(final int rgba, final boolean hasAlpha) {
        color = new java.awt.Color(rgba, hasAlpha);
    }

    /**
     * Creates a Color with the specified red, green, and blue values in the range (0.0 - 1.0).
     * Alpha is defaulted to 1.0. The lower the alpha, the more transparent the color.
     *
     * @throws IllegalArgumentException if {@code r}, {@code g} or {@code b} values are outside of
     *         the range 0.0 to 1.0, inclusive
     * @param r the red component
     * @param g the green component
     * @param b the blue component
     */
    public Color(final float r, final float g, final float b) {
        color = new java.awt.Color(r, g, b);
    }

    /**
     * Creates a Color with the specified red, green, blue, and alpha values in the range (0.0 -
     * 1.0). The lower the alpha, the more transparent the color.
     *
     * @throws IllegalArgumentException if {@code r}, {@code g}, {@code b}, {@code a} values are
     *         outside of the range 0.0 to 1.0, inclusive
     * @param r the red component
     * @param g the green component
     * @param b the blue component
     * @param a the alpha component
     */
    public Color(final float r, final float g, final float b, final float a) {
        color = new java.awt.Color(r, g, b, a);
    }


    ////////////////////////// internal functionality //////////////////////////


    @Override
    public String toString() {
        return "Color{" + color.getRed() + "," + color.getGreen() + "," + color.getBlue() + ","
            + color.getAlpha() + "}";
    }


    ////////////////////////// static helpers //////////////////////////

    /**
     * Creates a Color instance represented by the {@code color} String.
     *
     * Colors are specified by name or hex value.
     *
     * Hex values are parsed as follows: first two digits set the Red component of the color; second
     * two digits set the Green component; third two the Blue. Hex values must have a "#" in front,
     * e.g. "#001122"
     *
     * For available names, see {@link Color} and {@link #colorNames}
     *
     * @throws IllegalArgumentException {@code color} may not be null
     * @param color color; may be hex representation or case-insensitive color name
     * @return Color instance represented by the {@code color} String
     */
    public static Color color(final String color) {
        return new Color(color);
    }

    /**
     * Creates a Color with the specified red, green, blue, and alpha values.
     *
     * @throws IllegalArgumentException if {@code r}, {@code g} or {@code b} values are outside of
     *         the range 0 to 255, inclusive
     * @param r the red component in the range (0 - 255).
     * @param g the green component in the range (0 - 255).
     * @param b the blue component in the range (0 - 255).
     * @return Color with the specified RGB values. Alpha is defaulted to 255.
     */
    public static Color colorRGB(final int r, final int g, final int b) {
        return new Color(r, g, b);
    }

    /**
     * Creates a Color with the specified red, green, blue, and alpha values.
     *
     * @throws IllegalArgumentException if {@code r}, {@code g} or {@code b} values are outside of
     *         the range 0 to 255, inclusive
     * @param r the red component in the range (0 - 255).
     * @param g the green component in the range (0 - 255).
     * @param b the blue component in the range (0 - 255).
     * @param a the alpha component in the range (0 - 255).
     * @return Color with the specified RGBA values
     */
    public static Color colorRGB(final int r, final int g, final int b, final int a) {
        return new Color(r, g, b, a);
    }

    /**
     * Creates a Color with the specified red, green, blue, and alpha values.
     *
     * @param rgb the combined rbga components consisting of the alpha component in bits 24-31, the
     *        red component in bits 16-23, the green component in bits 8-15, and the blue component
     *        in bits 0-7. Alpha is defaulted to 255.
     * @return Color with the specified RGB value
     */
    public static Color colorRGB(final int rgb) {
        return new Color(rgb);
    }

    /**
     * Creates a Color with the specified red, green, blue, and alpha values.
     *
     * @param rgba the combined rbga components consisting of the alpha component in bits 24-31, the
     *        red component in bits 16-23, the green component in bits 8-15, and the blue component
     *        in bits 0-7. If {@code hasAlpha} is false, alpha is set to 255.
     * @param hasAlpha if true, {@code rgba} is parsed with an alpha component. Otherwise, alpha
     *        defaults to 255
     * @return Color with the specified RGBA value
     */
    public static Color colorRGB(final int rgba, final boolean hasAlpha) {
        return new Color(rgba, hasAlpha);
    }

    /**
     * Creates a Color with the specified red, green, blue, and alpha values.
     *
     * @throws IllegalArgumentException if {@code r}, {@code g} or {@code b} values are outside of
     *         the range 0.0 to 1.0, inclusive
     * @param r the red component in the range (0.0 - 1.0).
     * @param g the green component in the range (0.0 - 1.0).
     * @param b the blue component in the range (0.0 - 1.0).
     * @return Color with the specified RGB values. Alpha is defaulted to 1.0.
     */
    public static Color colorRGB(final float r, final float g, final float b) {
        return new Color(r, g, b);
    }

    /**
     * Creates a Color with the specified red, green, blue, and alpha values.
     *
     * @throws IllegalArgumentException if {@code r}, {@code g}, {@code b}, {@code a} values are
     *         outside of the range 0.0 to 1.0, inclusive
     * @param r the red component in the range (0.0 - 1.0).
     * @param g the green component in the range (0.0 - 1.0).
     * @param b the blue component in the range (0.0 - 1.0).
     * @param a the alpha component in the range (0.0-1.0). The lower the alpha, the more
     *        transparent the color.
     * @return Color with the specified RGBA values
     */
    public static Color colorRGB(final float r, final float g, final float b, final float a) {
        return new Color(r, g, b, a);
    }

    /**
     * Creates a Color with the specified hue, saturation, lightness, and alpha. The lower the
     * alpha, the more transparent the color.
     *
     * @throws IllegalArgumentException if {@code s} or {@code l} values are outside of the range
     *         0.0 to 100.0, inclusive
     * @param h the hue component, as a degree on the color wheel
     * @param s the saturation component, as a percentage
     * @param l the lightness component, as a percentage
     * @return Color with the specified HSL values. Alpha is defaulted to 1.0.
     */
    public static Color colorHSL(final float h, final float s, final float l) {
        return hslToColor(h, s, l);
    }

    /**
     * Creates a Color with the specified hue, saturation, lightness, and alpha. The lower the
     * alpha, the more transparent the color.
     *
     * @throws IllegalArgumentException if {@code s} or {@code l} values are outside of the range
     *         0.0 to 100.0, inclusive or if {@code a} is outside of the range 0.0 to 1.0, inclusive
     * @param h the hue component, as a degree on the color wheel
     * @param s the saturation component, as a percentage
     * @param l the lightness component, as a percentage
     * @param a the alpha component
     * @return Color with the specified HSLA values
     */
    public static Color colorHSL(final float h, final float s, final float l, final float a) {
        return hslToColor(h, s, l, a);
    }

    /**
     * Gets the names of all available colors.
     *
     * @return array of names of all available colors
     */
    public static String[] colorNames() {
        return stringToColorMap.keySet().toArray(new String[stringToColorMap.size()]);
    }


    @Override
    public java.awt.Color javaColor() {
        return color;
    }


    ////////////////////////// utility functions //////////////////////////


    /**
     * Convert HSL values to a RGB Color with a default alpha value of 1.
     *
     * @param h Hue is specified as degrees in the range 0 - 360.
     * @param s Saturation is specified as a percentage in the range 1 - 100.
     * @param l Lumanance is specified as a percentage in the range 1 - 100.
     *
     * @return the RGB Color object
     */
    private static Color hslToColor(float h, float s, float l) {
        return hslToColor(h, s, l, 1.0f);
    }

    /**
     * Convert HSL values to a RGB Color.
     *
     * @param h Hue is specified as degrees in the range 0 - 360.
     * @param s Saturation is specified as a percentage in the range 1 - 100.
     * @param l Lumanance is specified as a percentage in the range 1 - 100.
     * @param alpha the alpha value between 0 - 1
     *
     * @return the RGB Color object
     */
    private static Color hslToColor(float h, float s, float l, float alpha) {
        if (s < 0.0f || s > 100.0f) {
            String message = "Color parameter outside of expected range - Saturation";
            throw new IllegalArgumentException(message);
        }

        if (l < 0.0f || l > 100.0f) {
            String message = "Color parameter outside of expected range - Luminance";
            throw new IllegalArgumentException(message);
        }

        if (alpha < 0.0f || alpha > 1.0f) {
            String message = "Color parameter outside of expected range - Alpha";
            throw new IllegalArgumentException(message);
        }

        // Formula needs all values between 0 - 1.

        h = h % 360.0f;
        h /= 360f;
        s /= 100f;
        l /= 100f;

        float q;

        if (l < 0.5)
            q = l * (1 + s);
        else
            q = (l + s) - (s * l);

        float p = 2 * l - q;

        float r = Math.max(0, hueToRBG(p, q, h + (1.0f / 3.0f)));
        float g = Math.max(0, hueToRBG(p, q, h));
        float b = Math.max(0, hueToRBG(p, q, h - (1.0f / 3.0f)));

        r = Math.min(r, 1.0f);
        g = Math.min(g, 1.0f);
        b = Math.min(b, 1.0f);

        return new Color(r, g, b, alpha);
    }

    private static float hueToRBG(float p, float q, float h) {
        if (h < 0)
            h += 1;

        if (h > 1)
            h -= 1;

        if (6 * h < 1) {
            return p + ((q - p) * 6 * h);
        }

        if (2 * h < 1) {
            return q;
        }

        if (3 * h < 2) {
            return p + ((q - p) * 6 * ((2.0f / 3.0f) - h));
        }

        return p;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Color && ((Color) obj).color.equals(this.color);
    }

    @Override
    public int hashCode() {
        return color.hashCode();
    }


    ////////////////////// Color Enumeration ////////////////////////


    public static Color ALICEBLUE = Color.color("#F0F8FF");
    public static Color ANTIQUEWHITE = Color.color("#FAEBD7");
    public static Color AQUA = Color.color("#00FFFF");
    public static Color AQUAMARINE = Color.color("#7FFFD4");
    public static Color AZURE = Color.color("#F0FFFF");
    public static Color BEIGE = Color.color("#F5F5DC");
    public static Color BISQUE = Color.color("#FFE4C4");
    public static Color BLACK = Color.color("#000000");
    public static Color BLANCHEDALMOND = Color.color("#FFEBCD");
    public static Color BLUE = Color.color("#0000FF");
    public static Color BLUEVIOLET = Color.color("#8A2BE2");
    public static Color BROWN = Color.color("#A52A2A");
    public static Color BURLYWOOD = Color.color("#DEB887");
    public static Color CADETBLUE = Color.color("#5F9EA0");
    public static Color CHARTREUSE = Color.color("#7FFF00");
    public static Color CHOCOLATE = Color.color("#D2691E");
    public static Color CORAL = Color.color("#FF7F50");
    public static Color CORNFLOWERBLUE = Color.color("#6495ED");
    public static Color CORNSILK = Color.color("#FFF8DC");
    public static Color CRIMSON = Color.color("#DC143C");
    public static Color CYAN = Color.color("#00FFFF");
    public static Color DARKBLUE = Color.color("#00008B");
    public static Color DARKCYAN = Color.color("#008B8B");
    public static Color DARKGOLDENROD = Color.color("#B8860B");
    public static Color DARKGRAY = Color.color("#A9A9A9");
    public static Color DARKGREY = Color.color("#A9A9A9");
    public static Color DARKGREEN = Color.color("#006400");
    public static Color DARKKHAKI = Color.color("#BDB76B");
    public static Color DARKMAGENTA = Color.color("#8B008B");
    public static Color DARKOLIVEGREEN = Color.color("#556B2F");
    public static Color DARKORANGE = Color.color("#FF8C00");
    public static Color DARKORCHID = Color.color("#9932CC");
    public static Color DARKRED = Color.color("#8B0000");
    public static Color DARKSALMON = Color.color("#E9967A");
    public static Color DARKSEAGREEN = Color.color("#8FBC8F");
    public static Color DARKSLATEBLUE = Color.color("#483D8B");
    public static Color DARKSLATEGRAY = Color.color("#2F4F4F");
    public static Color DARKSLATEGREY = Color.color("#2F4F4F");
    public static Color DARKTURQUOISE = Color.color("#00CED1");
    public static Color DARKVIOLET = Color.color("#9400D3");
    public static Color DEEPPINK = Color.color("#FF1493");
    public static Color DEEPSKYBLUE = Color.color("#00BFFF");
    public static Color DIMGRAY = Color.color("#696969");
    public static Color DIMGREY = Color.color("#696969");
    public static Color DODGERBLUE = Color.color("#1E90FF");
    public static Color FIREBRICK = Color.color("#B22222");
    public static Color FLORALWHITE = Color.color("#FFFAF0");
    public static Color FORESTGREEN = Color.color("#228B22");
    public static Color FUCHSIA = Color.color("#FF00FF");
    public static Color GAINSBORO = Color.color("#DCDCDC");
    public static Color GHOSTWHITE = Color.color("#F8F8FF");
    public static Color GOLD = Color.color("#FFD700");
    public static Color GOLDENROD = Color.color("#DAA520");
    public static Color GRAY = Color.color("#808080");
    public static Color GREY = Color.color("#808080");
    public static Color GREEN = Color.color("#008000");
    public static Color GREENYELLOW = Color.color("#ADFF2F");
    public static Color HONEYDEW = Color.color("#F0FFF0");
    public static Color HOTPINK = Color.color("#FF69B4");
    public static Color INDIANRED = Color.color("#CD5C5C");
    public static Color INDIGO = Color.color("#4B0082");
    public static Color IVORY = Color.color("#FFFFF0");
    public static Color KHAKI = Color.color("#F0E68C");
    public static Color LAVENDER = Color.color("#E6E6FA");
    public static Color LAVENDERBLUSH = Color.color("#FFF0F5");
    public static Color LAWNGREEN = Color.color("#7CFC00");
    public static Color LEMONCHIFFON = Color.color("#FFFACD");
    public static Color LIGHTBLUE = Color.color("#ADD8E6");
    public static Color LIGHTCORAL = Color.color("#F08080");
    public static Color LIGHTCYAN = Color.color("#E0FFFF");
    public static Color LIGHTGOLDENRODYELLOW = Color.color("#FAFAD2");
    public static Color LIGHTGRAY = Color.color("#D3D3D3");
    public static Color LIGHTGREY = Color.color("#D3D3D3");
    public static Color LIGHTGREEN = Color.color("#90EE90");
    public static Color LIGHTPINK = Color.color("#FFB6C1");
    public static Color LIGHTSALMON = Color.color("#FFA07A");
    public static Color LIGHTSEAGREEN = Color.color("#20B2AA");
    public static Color LIGHTSKYBLUE = Color.color("#87CEFA");
    public static Color LIGHTSLATEGRAY = Color.color("#778899");
    public static Color LIGHTSLATEGREY = Color.color("#778899");
    public static Color LIGHTSTEELBLUE = Color.color("#B0C4DE");
    public static Color LIGHTYELLOW = Color.color("#FFFFE0");
    public static Color LIME = Color.color("#00FF00");
    public static Color LIMEGREEN = Color.color("#32CD32");
    public static Color LINEN = Color.color("#FAF0E6");
    public static Color MAGENTA = Color.color("#FF00FF");
    public static Color MAROON = Color.color("#800000");
    public static Color MEDIUMAQUAMARINE = Color.color("#66CDAA");
    public static Color MEDIUMBLUE = Color.color("#0000CD");
    public static Color MEDIUMORCHID = Color.color("#BA55D3");
    public static Color MEDIUMPURPLE = Color.color("#9370DB");
    public static Color MEDIUMSEAGREEN = Color.color("#3CB371");
    public static Color MEDIUMSLATEBLUE = Color.color("#7B68EE");
    public static Color MEDIUMSPRINGGREEN = Color.color("#00FA9A");
    public static Color MEDIUMTURQUOISE = Color.color("#48D1CC");
    public static Color MEDIUMVIOLETRED = Color.color("#C71585");
    public static Color MIDNIGHTBLUE = Color.color("#191970");
    public static Color MINTCREAM = Color.color("#F5FFFA");
    public static Color MISTYROSE = Color.color("#FFE4E1");
    public static Color MOCCASIN = Color.color("#FFE4B5");
    public static Color NAVAJOWHITE = Color.color("#FFDEAD");
    public static Color NAVY = Color.color("#000080");
    public static Color OLDLACE = Color.color("#FDF5E6");
    public static Color OLIVE = Color.color("#808000");
    public static Color OLIVEDRAB = Color.color("#6B8E23");
    public static Color ORANGE = Color.color("#FFA500");
    public static Color ORANGERED = Color.color("#FF4500");
    public static Color ORCHID = Color.color("#DA70D6");
    public static Color PALEGOLDENROD = Color.color("#EEE8AA");
    public static Color PALEGREEN = Color.color("#98FB98");
    public static Color PALETURQUOISE = Color.color("#AFEEEE");
    public static Color PALEVIOLETRED = Color.color("#DB7093");
    public static Color PAPAYAWHIP = Color.color("#FFEFD5");
    public static Color PEACHPUFF = Color.color("#FFDAB9");
    public static Color PERU = Color.color("#CD853F");
    public static Color PINK = Color.color("#FFC0CB");
    public static Color PLUM = Color.color("#DDA0DD");
    public static Color POWDERBLUE = Color.color("#B0E0E6");
    public static Color PURPLE = Color.color("#800080");
    public static Color REBECCAPURPLE = Color.color("#663399");
    public static Color RED = Color.color("#FF0000");
    public static Color ROSYBROWN = Color.color("#BC8F8F");
    public static Color ROYALBLUE = Color.color("#4169E1");
    public static Color SADDLEBROWN = Color.color("#8B4513");
    public static Color SALMON = Color.color("#FA8072");
    public static Color SANDYBROWN = Color.color("#F4A460");
    public static Color SEAGREEN = Color.color("#2E8B57");
    public static Color SEASHELL = Color.color("#FFF5EE");
    public static Color SIENNA = Color.color("#A0522D");
    public static Color SILVER = Color.color("#C0C0C0");
    public static Color SKYBLUE = Color.color("#87CEEB");
    public static Color SLATEBLUE = Color.color("#6A5ACD");
    public static Color SLATEGRAY = Color.color("#708090");
    public static Color SLATEGREY = Color.color("#708090");
    public static Color SNOW = Color.color("#FFFAFA");
    public static Color SPRINGGREEN = Color.color("#00FF7F");
    public static Color STEELBLUE = Color.color("#4682B4");
    public static Color TAN = Color.color("#D2B48C");
    public static Color TEAL = Color.color("#008080");
    public static Color THISTLE = Color.color("#D8BFD8");
    public static Color TOMATO = Color.color("#FF6347");
    public static Color TURQUOISE = Color.color("#40E0D0");
    public static Color VIOLET = Color.color("#EE82EE");
    public static Color WHEAT = Color.color("#F5DEB3");
    public static Color WHITE = Color.color("#FFFFFF");
    public static Color WHITESMOKE = Color.color("#F5F5F5");
    public static Color YELLOW = Color.color("#FFFF00");
    public static Color YELLOWGREEN = Color.color("#9ACD32");



    // Legacy non-html-standard colors. Duplicates removed. Conflicts resolved with a DB_ prefix.
    public static Color VIVID_RED = Color.colorRGB(231, 47, 39);
    public static Color VIVID_YELLOWRED = Color.colorRGB(238, 113, 25);
    public static Color VIVID_YELLOW = Color.colorRGB(255, 200, 8);
    public static Color VIVID_GREENYELLOW = Color.colorRGB(170, 198, 27);
    public static Color VIVID_GREEN = Color.colorRGB(19, 166, 50);
    public static Color VIVID_BLUEGREEN = Color.colorRGB(4, 148, 87);
    public static Color VIVID_BLUE = Color.colorRGB(1, 134, 141);
    public static Color VIVID_PURPLEBLUE = Color.colorRGB(3, 86, 155);
    public static Color VIVID_PURPLE = Color.colorRGB(46, 20, 141);
    public static Color VIVID_REDPURPLE = Color.colorRGB(204, 63, 92);
    public static Color STRONG_RED = Color.colorRGB(207, 46, 49);
    public static Color STRONG_YELLOWRED = Color.colorRGB(226, 132, 45);
    public static Color STRONG_YELLOW = Color.colorRGB(227, 189, 28);
    public static Color STRONG_GREENYELLOW = Color.colorRGB(162, 179, 36);
    public static Color STRONG_GREEN = Color.colorRGB(18, 154, 47);
    public static Color STRONG_BLUEGREEN = Color.colorRGB(6, 134, 84);
    public static Color STRONG_BLUE = Color.colorRGB(3, 130, 122);
    public static Color STRONG_PURPLEBLUE = Color.colorRGB(6, 113, 148);
    public static Color STRONG_PURPLE = Color.colorRGB(92, 104, 163);
    public static Color STRONG_REDPURPLE = Color.colorRGB(175, 92, 87);
    public static Color BRIGHT_RED = Color.colorRGB(231, 108, 86);
    public static Color BRIGHT_YELLOWRED = Color.colorRGB(241, 176, 102);
    public static Color BRIGHT_YELLOW = Color.colorRGB(255, 228, 15);
    public static Color BRIGHT_GREENYELLOW = Color.colorRGB(169, 199, 35);
    public static Color BRIGHT_GREEN = Color.colorRGB(88, 171, 45);
    public static Color BRIGHT_BLUEGREEN = Color.colorRGB(43, 151, 89);
    public static Color BRIGHT_BLUE = Color.colorRGB(0, 147, 159);
    public static Color BRIGHT_PURPLEBLUE = Color.colorRGB(59, 130, 157);
    public static Color BRIGHT_PURPLE = Color.colorRGB(178, 137, 166);
    public static Color BRIGHT_REDPURPLE = Color.colorRGB(209, 100, 109);
    public static Color PALE_RED = Color.colorRGB(233, 163, 144);
    public static Color PALE_YELLOWRED = Color.colorRGB(242, 178, 103);
    public static Color PALE_YELLOW = Color.colorRGB(255, 236, 79);
    public static Color PALE_GREENYELLOW = Color.colorRGB(219, 220, 93);
    public static Color PALE_GREEN = Color.colorRGB(155, 196, 113);
    public static Color PALE_BLUEGREEN = Color.colorRGB(146, 198, 131);
    public static Color PALE_BLUE = Color.colorRGB(126, 188, 209);
    public static Color PALE_PURPLEBLUE = Color.colorRGB(147, 184, 213);
    public static Color PALE_PURPLE = Color.colorRGB(197, 188, 213);
    public static Color PALE_REDPURPLE = Color.colorRGB(218, 176, 176);
    public static Color VERYPALE_RED = Color.colorRGB(236, 217, 202);
    public static Color VERYPALE_YELLOWRED = Color.colorRGB(245, 223, 181);
    public static Color VERYPALE_YELLOW = Color.colorRGB(249, 239, 189);
    public static Color VERYPALE_GREENYELLOW = Color.colorRGB(228, 235, 191);
    public static Color VERYPALE_GREEN = Color.colorRGB(221, 232, 207);
    public static Color VERYPALE_BLUEGREEN = Color.colorRGB(209, 234, 211);
    public static Color VERYPALE_BLUE = Color.colorRGB(194, 222, 242);
    public static Color VERYPALE_PURPLEBLUE = Color.colorRGB(203, 215, 232);
    public static Color VERYPALE_PURPLE = Color.colorRGB(224, 218, 230);
    public static Color VERYPALE_REDPURPLE = Color.colorRGB(235, 219, 224);
    public static Color LIGHTGRAYISH_RED = Color.colorRGB(213, 182, 166);
    public static Color LIGHTGRAYISH_YELLOWRED = Color.colorRGB(218, 196, 148);
    public static Color LIGHTGRAYISH_YELLOW = Color.colorRGB(233, 227, 143);
    public static Color LIGHTGRAYISH_GREENYELLOW = Color.colorRGB(209, 213, 165);
    public static Color LIGHTGRAYISH_GREEN = Color.colorRGB(179, 202, 157);
    public static Color LIGHTGRAYISH_BLUEGREEN = Color.colorRGB(166, 201, 163);
    public static Color LIGHTGRAYISH_BLUE = Color.colorRGB(127, 175, 166);
    public static Color LIGHTGRAYISH_PURPLEBLUE = Color.colorRGB(165, 184, 199);
    public static Color LIGHTGRAYISH_PURPLE = Color.colorRGB(184, 190, 189);
    public static Color LIGHTGRAYISH_REDPURPLE = Color.colorRGB(206, 185, 179);
    public static Color LIGHT_RED = Color.colorRGB(211, 142, 110);
    public static Color LIGHT_YELLOWRED = Color.colorRGB(215, 145, 96);
    public static Color LIGHT_YELLOW = Color.colorRGB(255, 203, 88);
    public static Color LIGHT_GREENYELLOW = Color.colorRGB(195, 202, 101);
    public static Color LIGHT_GREEN = Color.colorRGB(141, 188, 90);
    public static Color LIGHT_BLUEGREEN = Color.colorRGB(140, 195, 110);
    public static Color LIGHT_BLUE = Color.colorRGB(117, 173, 169);
    public static Color LIGHT_PURPLEBLUE = Color.colorRGB(138, 166, 187);
    public static Color LIGHT_PURPLE = Color.colorRGB(170, 165, 199);
    public static Color LIGHT_REDPURPLE = Color.colorRGB(205, 154, 149);
    public static Color GRAYISH_RED = Color.colorRGB(171, 131, 115);
    public static Color GRAYISH_YELLOWRED = Color.colorRGB(158, 128, 110);
    public static Color GRAYISH_YELLOW = Color.colorRGB(148, 133, 105);
    public static Color GRAYISH_GREENYELLOW = Color.colorRGB(144, 135, 96);
    public static Color GRAYISH_GREEN = Color.colorRGB(143, 162, 121);
    public static Color GRAYISH_BLUEGREEN = Color.colorRGB(122, 165, 123);
    public static Color GRAYISH_BLUE = Color.colorRGB(130, 154, 145);
    public static Color GRAYISH_PURPLEBLUE = Color.colorRGB(133, 154, 153);
    public static Color GRAYISH_PURPLE = Color.colorRGB(151, 150, 139);
    public static Color GRAYISH_REDPURPLE = Color.colorRGB(160, 147, 131);
    public static Color DULL_RED = Color.colorRGB(162, 88, 61);
    public static Color DULL_YELLOWRED = Color.colorRGB(167, 100, 67);
    public static Color DULL_YELLOW = Color.colorRGB(139, 117, 65);
    public static Color DULL_GREENYELLOW = Color.colorRGB(109, 116, 73);
    public static Color DULL_GREEN = Color.colorRGB(88, 126, 61);
    public static Color DULL_BLUEGREEN = Color.colorRGB(39, 122, 62);
    public static Color DULL_BLUE = Color.colorRGB(24, 89, 63);
    public static Color DULL_PURPLEBLUE = Color.colorRGB(53, 109, 98);
    public static Color DULL_PURPLE = Color.colorRGB(44, 77, 143);
    public static Color DULL_REDPURPLE = Color.colorRGB(115, 71, 79);
    public static Color DEEP_RED = Color.colorRGB(172, 36, 48);
    public static Color DEEP_YELLOWRED = Color.colorRGB(169, 87, 49);
    public static Color DEEP_YELLOW = Color.colorRGB(156, 137, 37);
    public static Color DEEP_GREENYELLOW = Color.colorRGB(91, 132, 47);
    public static Color DEEP_GREEN = Color.colorRGB(20, 114, 48);
    public static Color DEEP_BLUEGREEN = Color.colorRGB(23, 106, 43);
    public static Color DEEP_BLUE = Color.colorRGB(20, 88, 60);
    public static Color DEEP_PURPLEBLUE = Color.colorRGB(8, 87, 107);
    public static Color DEEP_PURPLE = Color.colorRGB(58, 55, 119);
    public static Color DEEP_REDPURPLE = Color.colorRGB(111, 61, 56);
    public static Color DARK_RED = Color.colorRGB(116, 47, 50);
    public static Color DARK_YELLOWRED = Color.colorRGB(115, 63, 44);
    public static Color DARK_YELLOW = Color.colorRGB(103, 91, 44);
    public static Color DARK_GREENYELLOW = Color.colorRGB(54, 88, 48);
    public static Color DARK_GREEN = Color.colorRGB(30, 98, 50);
    public static Color DARK_BLUEGREEN = Color.colorRGB(27, 86, 49);
    public static Color DARK_BLUE = Color.colorRGB(18, 83, 65);
    public static Color DARK_PURPLEBLUE = Color.colorRGB(16, 76, 84);
    public static Color DARK_PURPLE = Color.colorRGB(40, 57, 103);
    public static Color DARK_REDPURPLE = Color.colorRGB(88, 60, 50);
    public static Color DARKGRAYISH_RED = Color.colorRGB(79, 46, 43);
    public static Color DARKGRAYISH_YELLOWRED = Color.colorRGB(85, 55, 43);
    public static Color DARKGRAYISH_YELLOW = Color.colorRGB(75, 63, 45);
    public static Color DARKGRAYISH_GREENYELLOW = Color.colorRGB(44, 60, 49);
    public static Color DARKGRAYISH_GREEN = Color.colorRGB(34, 62, 51);
    public static Color DARKGRAYISH_BLUEGREEN = Color.colorRGB(31, 56, 45);
    public static Color DARKGRAYISH_BLUE = Color.colorRGB(29, 60, 47);
    public static Color DARKGRAYISH_PURPLEBLUE = Color.colorRGB(25, 62, 63);
    public static Color DARKGRAYISH_PURPLE = Color.colorRGB(34, 54, 68);
    public static Color DARKGRAYISH_REDPURPLE = Color.colorRGB(53, 52, 48);
    public static Color GRAY1 = Color.colorRGB(28, 28, 28);
    public static Color GRAY2 = Color.colorRGB(56, 56, 56);
    public static Color GRAY3 = Color.colorRGB(84, 84, 84);
    public static Color GRAY4 = Color.colorRGB(112, 112, 112);
    public static Color GRAY5 = Color.colorRGB(140, 140, 140);
    public static Color GRAY6 = Color.colorRGB(168, 168, 168);
    public static Color GRAY7 = Color.colorRGB(196, 196, 196);
    public static Color GRAY8 = Color.colorRGB(224, 224, 224);
    public static Color DB_PINK = Color.colorRGB(255, 175, 175);
    public static Color DB_ORANGE = Color.colorRGB(255, 200, 0);
    public static Color DB_GREEN = Color.colorRGB(0, 255, 0);
    public static Color NO_FORMATTING = Color.colorRGB(0, 0, 0);

    private static final Map<String, Color> stringToColorMap = new HashMap<>();

    static {
        stringToColorMap.put("ALICEBLUE", ALICEBLUE);
        stringToColorMap.put("ANTIQUEWHITE", ANTIQUEWHITE);
        stringToColorMap.put("AQUA", AQUA);
        stringToColorMap.put("AQUAMARINE", AQUAMARINE);
        stringToColorMap.put("AZURE", AZURE);
        stringToColorMap.put("BEIGE", BEIGE);
        stringToColorMap.put("BISQUE", BISQUE);
        stringToColorMap.put("BLACK", BLACK);
        stringToColorMap.put("BLANCHEDALMOND", BLANCHEDALMOND);
        stringToColorMap.put("BLUE", BLUE);
        stringToColorMap.put("BLUEVIOLET", BLUEVIOLET);
        stringToColorMap.put("BROWN", BROWN);
        stringToColorMap.put("BURLYWOOD", BURLYWOOD);
        stringToColorMap.put("CADETBLUE", CADETBLUE);
        stringToColorMap.put("CHARTREUSE", CHARTREUSE);
        stringToColorMap.put("CHOCOLATE", CHOCOLATE);
        stringToColorMap.put("CORAL", CORAL);
        stringToColorMap.put("CORNFLOWERBLUE", CORNFLOWERBLUE);
        stringToColorMap.put("CORNSILK", CORNSILK);
        stringToColorMap.put("CRIMSON", CRIMSON);
        stringToColorMap.put("CYAN", CYAN);
        stringToColorMap.put("DARKBLUE", DARKBLUE);
        stringToColorMap.put("DARKCYAN", DARKCYAN);
        stringToColorMap.put("DARKGOLDENROD", DARKGOLDENROD);
        stringToColorMap.put("DARKGRAY", DARKGRAY);
        stringToColorMap.put("DARKGREY", DARKGREY);
        stringToColorMap.put("DARKGREEN", DARKGREEN);
        stringToColorMap.put("DARKKHAKI", DARKKHAKI);
        stringToColorMap.put("DARKMAGENTA", DARKMAGENTA);
        stringToColorMap.put("DARKOLIVEGREEN", DARKOLIVEGREEN);
        stringToColorMap.put("DARKORANGE", DARKORANGE);
        stringToColorMap.put("DARKORCHID", DARKORCHID);
        stringToColorMap.put("DARKRED", DARKRED);
        stringToColorMap.put("DARKSALMON", DARKSALMON);
        stringToColorMap.put("DARKSEAGREEN", DARKSEAGREEN);
        stringToColorMap.put("DARKSLATEBLUE", DARKSLATEBLUE);
        stringToColorMap.put("DARKSLATEGRAY", DARKSLATEGRAY);
        stringToColorMap.put("DARKSLATEGREY", DARKSLATEGREY);
        stringToColorMap.put("DARKTURQUOISE", DARKTURQUOISE);
        stringToColorMap.put("DARKVIOLET", DARKVIOLET);
        stringToColorMap.put("DEEPPINK", DEEPPINK);
        stringToColorMap.put("DEEPSKYBLUE", DEEPSKYBLUE);
        stringToColorMap.put("DIMGRAY", DIMGRAY);
        stringToColorMap.put("DIMGREY", DIMGREY);
        stringToColorMap.put("DODGERBLUE", DODGERBLUE);
        stringToColorMap.put("FIREBRICK", FIREBRICK);
        stringToColorMap.put("FLORALWHITE", FLORALWHITE);
        stringToColorMap.put("FORESTGREEN", FORESTGREEN);
        stringToColorMap.put("FUCHSIA", FUCHSIA);
        stringToColorMap.put("GAINSBORO", GAINSBORO);
        stringToColorMap.put("GHOSTWHITE", GHOSTWHITE);
        stringToColorMap.put("GOLD", GOLD);
        stringToColorMap.put("GOLDENROD", GOLDENROD);
        stringToColorMap.put("GRAY", GRAY);
        stringToColorMap.put("GREY", GREY);
        stringToColorMap.put("GREEN", GREEN);
        stringToColorMap.put("GREENYELLOW", GREENYELLOW);
        stringToColorMap.put("HONEYDEW", HONEYDEW);
        stringToColorMap.put("HOTPINK", HOTPINK);
        stringToColorMap.put("INDIANRED", INDIANRED);
        stringToColorMap.put("INDIGO", INDIGO);
        stringToColorMap.put("IVORY", IVORY);
        stringToColorMap.put("KHAKI", KHAKI);
        stringToColorMap.put("LAVENDER", LAVENDER);
        stringToColorMap.put("LAVENDERBLUSH", LAVENDERBLUSH);
        stringToColorMap.put("LAWNGREEN", LAWNGREEN);
        stringToColorMap.put("LEMONCHIFFON", LEMONCHIFFON);
        stringToColorMap.put("LIGHTBLUE", LIGHTBLUE);
        stringToColorMap.put("LIGHTCORAL", LIGHTCORAL);
        stringToColorMap.put("LIGHTCYAN", LIGHTCYAN);
        stringToColorMap.put("LIGHTGOLDENRODYELLOW", LIGHTGOLDENRODYELLOW);
        stringToColorMap.put("LIGHTGRAY", LIGHTGRAY);
        stringToColorMap.put("LIGHTGREY", LIGHTGREY);
        stringToColorMap.put("LIGHTGREEN", LIGHTGREEN);
        stringToColorMap.put("LIGHTPINK", LIGHTPINK);
        stringToColorMap.put("LIGHTSALMON", LIGHTSALMON);
        stringToColorMap.put("LIGHTSEAGREEN", LIGHTSEAGREEN);
        stringToColorMap.put("LIGHTSKYBLUE", LIGHTSKYBLUE);
        stringToColorMap.put("LIGHTSLATEGRAY", LIGHTSLATEGRAY);
        stringToColorMap.put("LIGHTSLATEGREY", LIGHTSLATEGREY);
        stringToColorMap.put("LIGHTSTEELBLUE", LIGHTSTEELBLUE);
        stringToColorMap.put("LIGHTYELLOW", LIGHTYELLOW);
        stringToColorMap.put("LIME", LIME);
        stringToColorMap.put("LIMEGREEN", LIMEGREEN);
        stringToColorMap.put("LINEN", LINEN);
        stringToColorMap.put("MAGENTA", MAGENTA);
        stringToColorMap.put("MAROON", MAROON);
        stringToColorMap.put("MEDIUMAQUAMARINE", MEDIUMAQUAMARINE);
        stringToColorMap.put("MEDIUMBLUE", MEDIUMBLUE);
        stringToColorMap.put("MEDIUMORCHID", MEDIUMORCHID);
        stringToColorMap.put("MEDIUMPURPLE", MEDIUMPURPLE);
        stringToColorMap.put("MEDIUMSEAGREEN", MEDIUMSEAGREEN);
        stringToColorMap.put("MEDIUMSLATEBLUE", MEDIUMSLATEBLUE);
        stringToColorMap.put("MEDIUMSPRINGGREEN", MEDIUMSPRINGGREEN);
        stringToColorMap.put("MEDIUMTURQUOISE", MEDIUMTURQUOISE);
        stringToColorMap.put("MEDIUMVIOLETRED", MEDIUMVIOLETRED);
        stringToColorMap.put("MIDNIGHTBLUE", MIDNIGHTBLUE);
        stringToColorMap.put("MINTCREAM", MINTCREAM);
        stringToColorMap.put("MISTYROSE", MISTYROSE);
        stringToColorMap.put("MOCCASIN", MOCCASIN);
        stringToColorMap.put("NAVAJOWHITE", NAVAJOWHITE);
        stringToColorMap.put("NAVY", NAVY);
        stringToColorMap.put("OLDLACE", OLDLACE);
        stringToColorMap.put("OLIVE", OLIVE);
        stringToColorMap.put("OLIVEDRAB", OLIVEDRAB);
        stringToColorMap.put("ORANGE", ORANGE);
        stringToColorMap.put("ORANGERED", ORANGERED);
        stringToColorMap.put("ORCHID", ORCHID);
        stringToColorMap.put("PALEGOLDENROD", PALEGOLDENROD);
        stringToColorMap.put("PALEGREEN", PALEGREEN);
        stringToColorMap.put("PALETURQUOISE", PALETURQUOISE);
        stringToColorMap.put("PALEVIOLETRED", PALEVIOLETRED);
        stringToColorMap.put("PAPAYAWHIP", PAPAYAWHIP);
        stringToColorMap.put("PEACHPUFF", PEACHPUFF);
        stringToColorMap.put("PERU", PERU);
        stringToColorMap.put("PINK", PINK);
        stringToColorMap.put("PLUM", PLUM);
        stringToColorMap.put("POWDERBLUE", POWDERBLUE);
        stringToColorMap.put("PURPLE", PURPLE);
        stringToColorMap.put("REBECCAPURPLE", REBECCAPURPLE);
        stringToColorMap.put("RED", RED);
        stringToColorMap.put("ROSYBROWN", ROSYBROWN);
        stringToColorMap.put("ROYALBLUE", ROYALBLUE);
        stringToColorMap.put("SADDLEBROWN", SADDLEBROWN);
        stringToColorMap.put("SALMON", SALMON);
        stringToColorMap.put("SANDYBROWN", SANDYBROWN);
        stringToColorMap.put("SEAGREEN", SEAGREEN);
        stringToColorMap.put("SEASHELL", SEASHELL);
        stringToColorMap.put("SIENNA", SIENNA);
        stringToColorMap.put("SILVER", SILVER);
        stringToColorMap.put("SKYBLUE", SKYBLUE);
        stringToColorMap.put("SLATEBLUE", SLATEBLUE);
        stringToColorMap.put("SLATEGRAY", SLATEGRAY);
        stringToColorMap.put("SLATEGREY", SLATEGREY);
        stringToColorMap.put("SNOW", SNOW);
        stringToColorMap.put("SPRINGGREEN", SPRINGGREEN);
        stringToColorMap.put("STEELBLUE", STEELBLUE);
        stringToColorMap.put("TAN", TAN);
        stringToColorMap.put("TEAL", TEAL);
        stringToColorMap.put("THISTLE", THISTLE);
        stringToColorMap.put("TOMATO", TOMATO);
        stringToColorMap.put("TURQUOISE", TURQUOISE);
        stringToColorMap.put("VIOLET", VIOLET);
        stringToColorMap.put("WHEAT", WHEAT);
        stringToColorMap.put("WHITE", WHITE);
        stringToColorMap.put("WHITESMOKE", WHITESMOKE);
        stringToColorMap.put("YELLOW", YELLOW);
        stringToColorMap.put("YELLOWGREEN", YELLOWGREEN);
        stringToColorMap.put("VIVID_RED", VIVID_RED);
        stringToColorMap.put("VIVID_YELLOWRED", VIVID_YELLOWRED);
        stringToColorMap.put("VIVID_YELLOW", VIVID_YELLOW);
        stringToColorMap.put("VIVID_GREENYELLOW", VIVID_GREENYELLOW);
        stringToColorMap.put("VIVID_GREEN", VIVID_GREEN);
        stringToColorMap.put("VIVID_BLUEGREEN", VIVID_BLUEGREEN);
        stringToColorMap.put("VIVID_BLUE", VIVID_BLUE);
        stringToColorMap.put("VIVID_PURPLEBLUE", VIVID_PURPLEBLUE);
        stringToColorMap.put("VIVID_PURPLE", VIVID_PURPLE);
        stringToColorMap.put("VIVID_REDPURPLE", VIVID_REDPURPLE);
        stringToColorMap.put("STRONG_RED", STRONG_RED);
        stringToColorMap.put("STRONG_YELLOWRED", STRONG_YELLOWRED);
        stringToColorMap.put("STRONG_YELLOW", STRONG_YELLOW);
        stringToColorMap.put("STRONG_GREENYELLOW", STRONG_GREENYELLOW);
        stringToColorMap.put("STRONG_GREEN", STRONG_GREEN);
        stringToColorMap.put("STRONG_BLUEGREEN", STRONG_BLUEGREEN);
        stringToColorMap.put("STRONG_BLUE", STRONG_BLUE);
        stringToColorMap.put("STRONG_PURPLEBLUE", STRONG_PURPLEBLUE);
        stringToColorMap.put("STRONG_PURPLE", STRONG_PURPLE);
        stringToColorMap.put("STRONG_REDPURPLE", STRONG_REDPURPLE);
        stringToColorMap.put("BRIGHT_RED", BRIGHT_RED);
        stringToColorMap.put("BRIGHT_YELLOWRED", BRIGHT_YELLOWRED);
        stringToColorMap.put("BRIGHT_YELLOW", BRIGHT_YELLOW);
        stringToColorMap.put("BRIGHT_GREENYELLOW", BRIGHT_GREENYELLOW);
        stringToColorMap.put("BRIGHT_GREEN", BRIGHT_GREEN);
        stringToColorMap.put("BRIGHT_BLUEGREEN", BRIGHT_BLUEGREEN);
        stringToColorMap.put("BRIGHT_BLUE", BRIGHT_BLUE);
        stringToColorMap.put("BRIGHT_PURPLEBLUE", BRIGHT_PURPLEBLUE);
        stringToColorMap.put("BRIGHT_PURPLE", BRIGHT_PURPLE);
        stringToColorMap.put("BRIGHT_REDPURPLE", BRIGHT_REDPURPLE);
        stringToColorMap.put("PALE_RED", PALE_RED);
        stringToColorMap.put("PALE_YELLOWRED", PALE_YELLOWRED);
        stringToColorMap.put("PALE_YELLOW", PALE_YELLOW);
        stringToColorMap.put("PALE_GREENYELLOW", PALE_GREENYELLOW);
        stringToColorMap.put("PALE_GREEN", PALE_GREEN);
        stringToColorMap.put("PALE_BLUEGREEN", PALE_BLUEGREEN);
        stringToColorMap.put("PALE_BLUE", PALE_BLUE);
        stringToColorMap.put("PALE_PURPLEBLUE", PALE_PURPLEBLUE);
        stringToColorMap.put("PALE_PURPLE", PALE_PURPLE);
        stringToColorMap.put("PALE_REDPURPLE", PALE_REDPURPLE);
        stringToColorMap.put("VERYPALE_RED", VERYPALE_RED);
        stringToColorMap.put("VERYPALE_YELLOWRED", VERYPALE_YELLOWRED);
        stringToColorMap.put("VERYPALE_YELLOW", VERYPALE_YELLOW);
        stringToColorMap.put("VERYPALE_GREENYELLOW", VERYPALE_GREENYELLOW);
        stringToColorMap.put("VERYPALE_GREEN", VERYPALE_GREEN);
        stringToColorMap.put("VERYPALE_BLUEGREEN", VERYPALE_BLUEGREEN);
        stringToColorMap.put("VERYPALE_BLUE", VERYPALE_BLUE);
        stringToColorMap.put("VERYPALE_PURPLEBLUE", VERYPALE_PURPLEBLUE);
        stringToColorMap.put("VERYPALE_PURPLE", VERYPALE_PURPLE);
        stringToColorMap.put("VERYPALE_REDPURPLE", VERYPALE_REDPURPLE);
        stringToColorMap.put("LIGHTGRAYISH_RED", LIGHTGRAYISH_RED);
        stringToColorMap.put("LIGHTGRAYISH_YELLOWRED", LIGHTGRAYISH_YELLOWRED);
        stringToColorMap.put("LIGHTGRAYISH_YELLOW", LIGHTGRAYISH_YELLOW);
        stringToColorMap.put("LIGHTGRAYISH_GREENYELLOW", LIGHTGRAYISH_GREENYELLOW);
        stringToColorMap.put("LIGHTGRAYISH_GREEN", LIGHTGRAYISH_GREEN);
        stringToColorMap.put("LIGHTGRAYISH_BLUEGREEN", LIGHTGRAYISH_BLUEGREEN);
        stringToColorMap.put("LIGHTGRAYISH_BLUE", LIGHTGRAYISH_BLUE);
        stringToColorMap.put("LIGHTGRAYISH_PURPLEBLUE", LIGHTGRAYISH_PURPLEBLUE);
        stringToColorMap.put("LIGHTGRAYISH_PURPLE", LIGHTGRAYISH_PURPLE);
        stringToColorMap.put("LIGHTGRAYISH_REDPURPLE", LIGHTGRAYISH_REDPURPLE);
        stringToColorMap.put("LIGHT_RED", LIGHT_RED);
        stringToColorMap.put("LIGHT_YELLOWRED", LIGHT_YELLOWRED);
        stringToColorMap.put("LIGHT_YELLOW", LIGHT_YELLOW);
        stringToColorMap.put("LIGHT_GREENYELLOW", LIGHT_GREENYELLOW);
        stringToColorMap.put("LIGHT_GREEN", LIGHT_GREEN);
        stringToColorMap.put("LIGHT_BLUEGREEN", LIGHT_BLUEGREEN);
        stringToColorMap.put("LIGHT_BLUE", LIGHT_BLUE);
        stringToColorMap.put("LIGHT_PURPLEBLUE", LIGHT_PURPLEBLUE);
        stringToColorMap.put("LIGHT_PURPLE", LIGHT_PURPLE);
        stringToColorMap.put("LIGHT_REDPURPLE", LIGHT_REDPURPLE);
        stringToColorMap.put("GRAYISH_RED", GRAYISH_RED);
        stringToColorMap.put("GRAYISH_YELLOWRED", GRAYISH_YELLOWRED);
        stringToColorMap.put("GRAYISH_YELLOW", GRAYISH_YELLOW);
        stringToColorMap.put("GRAYISH_GREENYELLOW", GRAYISH_GREENYELLOW);
        stringToColorMap.put("GRAYISH_GREEN", GRAYISH_GREEN);
        stringToColorMap.put("GRAYISH_BLUEGREEN", GRAYISH_BLUEGREEN);
        stringToColorMap.put("GRAYISH_BLUE", GRAYISH_BLUE);
        stringToColorMap.put("GRAYISH_PURPLEBLUE", GRAYISH_PURPLEBLUE);
        stringToColorMap.put("GRAYISH_PURPLE", GRAYISH_PURPLE);
        stringToColorMap.put("GRAYISH_REDPURPLE", GRAYISH_REDPURPLE);
        stringToColorMap.put("DULL_RED", DULL_RED);
        stringToColorMap.put("DULL_YELLOWRED", DULL_YELLOWRED);
        stringToColorMap.put("DULL_YELLOW", DULL_YELLOW);
        stringToColorMap.put("DULL_GREENYELLOW", DULL_GREENYELLOW);
        stringToColorMap.put("DULL_GREEN", DULL_GREEN);
        stringToColorMap.put("DULL_BLUEGREEN", DULL_BLUEGREEN);
        stringToColorMap.put("DULL_BLUE", DULL_BLUE);
        stringToColorMap.put("DULL_PURPLEBLUE", DULL_PURPLEBLUE);
        stringToColorMap.put("DULL_PURPLE", DULL_PURPLE);
        stringToColorMap.put("DULL_REDPURPLE", DULL_REDPURPLE);
        stringToColorMap.put("DEEP_RED", DEEP_RED);
        stringToColorMap.put("DEEP_YELLOWRED", DEEP_YELLOWRED);
        stringToColorMap.put("DEEP_YELLOW", DEEP_YELLOW);
        stringToColorMap.put("DEEP_GREENYELLOW", DEEP_GREENYELLOW);
        stringToColorMap.put("DEEP_GREEN", DEEP_GREEN);
        stringToColorMap.put("DEEP_BLUEGREEN", DEEP_BLUEGREEN);
        stringToColorMap.put("DEEP_BLUE", DEEP_BLUE);
        stringToColorMap.put("DEEP_PURPLEBLUE", DEEP_PURPLEBLUE);
        stringToColorMap.put("DEEP_PURPLE", DEEP_PURPLE);
        stringToColorMap.put("DEEP_REDPURPLE", DEEP_REDPURPLE);
        stringToColorMap.put("DARK_RED", DARK_RED);
        stringToColorMap.put("DARK_YELLOWRED", DARK_YELLOWRED);
        stringToColorMap.put("DARK_YELLOW", DARK_YELLOW);
        stringToColorMap.put("DARK_GREENYELLOW", DARK_GREENYELLOW);
        stringToColorMap.put("DARK_GREEN", DARK_GREEN);
        stringToColorMap.put("DARK_BLUEGREEN", DARK_BLUEGREEN);
        stringToColorMap.put("DARK_BLUE", DARK_BLUE);
        stringToColorMap.put("DARK_PURPLEBLUE", DARK_PURPLEBLUE);
        stringToColorMap.put("DARK_PURPLE", DARK_PURPLE);
        stringToColorMap.put("DARK_REDPURPLE", DARK_REDPURPLE);
        stringToColorMap.put("DARKGRAYISH_RED", DARKGRAYISH_RED);
        stringToColorMap.put("DARKGRAYISH_YELLOWRED", DARKGRAYISH_YELLOWRED);
        stringToColorMap.put("DARKGRAYISH_YELLOW", DARKGRAYISH_YELLOW);
        stringToColorMap.put("DARKGRAYISH_GREENYELLOW", DARKGRAYISH_GREENYELLOW);
        stringToColorMap.put("DARKGRAYISH_GREEN", DARKGRAYISH_GREEN);
        stringToColorMap.put("DARKGRAYISH_BLUEGREEN", DARKGRAYISH_BLUEGREEN);
        stringToColorMap.put("DARKGRAYISH_BLUE", DARKGRAYISH_BLUE);
        stringToColorMap.put("DARKGRAYISH_PURPLEBLUE", DARKGRAYISH_PURPLEBLUE);
        stringToColorMap.put("DARKGRAYISH_PURPLE", DARKGRAYISH_PURPLE);
        stringToColorMap.put("DARKGRAYISH_REDPURPLE", DARKGRAYISH_REDPURPLE);
        stringToColorMap.put("GRAY1", GRAY1);
        stringToColorMap.put("GRAY2", GRAY2);
        stringToColorMap.put("GRAY3", GRAY3);
        stringToColorMap.put("GRAY4", GRAY4);
        stringToColorMap.put("GRAY5", GRAY5);
        stringToColorMap.put("GRAY6", GRAY6);
        stringToColorMap.put("GRAY7", GRAY7);
        stringToColorMap.put("GRAY8", GRAY8);
        stringToColorMap.put("DB_PINK", DB_PINK);
        stringToColorMap.put("DB_ORANGE", DB_ORANGE);
        stringToColorMap.put("DB_GREEN", DB_GREEN);
        stringToColorMap.put("NO_FORMATTING", NO_FORMATTING);
    }

    public static Color valueOf(final String color) {
        if (color == null) {
            throw new IllegalArgumentException("Color may not be null");
        }

        return stringToColorMap.get(color.trim().toUpperCase());
    }

    public static Collection<Color> values() {
        return stringToColorMap.values();
    }
}
