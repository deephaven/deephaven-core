/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.gui.color;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Pre-defined {@link Color}s available by name.
 *
 * The colors are the named colors commonly supported by HTML browsers.
 *
 * Methods and colors moved from DBColorUtil to here. They are left in ColorUtil for backward compatibility. TODO
 * (deephaven/deephaven-core/issues/175): Move this to a new module and package
 */
public enum Colors {
    // @formatter:off
    //color constants from https://www.w3schools.com/colors/colorsnames.asp
    ALICEBLUE("#F0F8FF"),
    ANTIQUEWHITE("#FAEBD7"),
    AQUA("#00FFFF"),
    AQUAMARINE("#7FFFD4"),
    AZURE("#F0FFFF"),
    BEIGE("#F5F5DC"),
    BISQUE("#FFE4C4"),
    BLACK("#000000"),
    BLANCHEDALMOND("#FFEBCD"),
    BLUE("#0000FF"),
    BLUEVIOLET("#8A2BE2"),
    BROWN("#A52A2A"),
    BURLYWOOD("#DEB887"),
    CADETBLUE("#5F9EA0"),
    CHARTREUSE("#7FFF00"),
    CHOCOLATE("#D2691E"),
    CORAL("#FF7F50"),
    CORNFLOWERBLUE("#6495ED"),
    CORNSILK("#FFF8DC"),
    CRIMSON("#DC143C"),
    CYAN("#00FFFF"),
    DARKBLUE("#00008B"),
    DARKCYAN("#008B8B"),
    DARKGOLDENROD("#B8860B"),
    DARKGRAY("#A9A9A9"),
    DARKGREY("#A9A9A9"),
    DARKGREEN("#006400"),
    DARKKHAKI("#BDB76B"),
    DARKMAGENTA("#8B008B"),
    DARKOLIVEGREEN("#556B2F"),
    DARKORANGE("#FF8C00"),
    DARKORCHID("#9932CC"),
    DARKRED("#8B0000"),
    DARKSALMON("#E9967A"),
    DARKSEAGREEN("#8FBC8F"),
    DARKSLATEBLUE("#483D8B"),
    DARKSLATEGRAY("#2F4F4F"),
    DARKSLATEGREY("#2F4F4F"),
    DARKTURQUOISE("#00CED1"),
    DARKVIOLET("#9400D3"),
    DEEPPINK("#FF1493"),
    DEEPSKYBLUE("#00BFFF"),
    DIMGRAY("#696969"),
    DIMGREY("#696969"),
    DODGERBLUE("#1E90FF"),
    FIREBRICK("#B22222"),
    FLORALWHITE("#FFFAF0"),
    FORESTGREEN("#228B22"),
    FUCHSIA("#FF00FF"),
    GAINSBORO("#DCDCDC"),
    GHOSTWHITE("#F8F8FF"),
    GOLD("#FFD700"),
    GOLDENROD("#DAA520"),
    GRAY("#808080"),
    GREY("#808080"),
    GREEN("#008000"),
    GREENYELLOW("#ADFF2F"),
    HONEYDEW("#F0FFF0"),
    HOTPINK("#FF69B4"),
    INDIANRED("#CD5C5C"),
    INDIGO("#4B0082"),
    IVORY("#FFFFF0"),
    KHAKI("#F0E68C"),
    LAVENDER("#E6E6FA"),
    LAVENDERBLUSH("#FFF0F5"),
    LAWNGREEN("#7CFC00"),
    LEMONCHIFFON("#FFFACD"),
    LIGHTBLUE("#ADD8E6"),
    LIGHTCORAL("#F08080"),
    LIGHTCYAN("#E0FFFF"),
    LIGHTGOLDENRODYELLOW("#FAFAD2"),
    LIGHTGRAY("#D3D3D3"),
    LIGHTGREY("#D3D3D3"),
    LIGHTGREEN("#90EE90"),
    LIGHTPINK("#FFB6C1"),
    LIGHTSALMON("#FFA07A"),
    LIGHTSEAGREEN("#20B2AA"),
    LIGHTSKYBLUE("#87CEFA"),
    LIGHTSLATEGRAY("#778899"),
    LIGHTSLATEGREY("#778899"),
    LIGHTSTEELBLUE("#B0C4DE"),
    LIGHTYELLOW("#FFFFE0"),
    LIME("#00FF00"),
    LIMEGREEN("#32CD32"),
    LINEN("#FAF0E6"),
    MAGENTA("#FF00FF"),
    MAROON("#800000"),
    MEDIUMAQUAMARINE("#66CDAA"),
    MEDIUMBLUE("#0000CD"),
    MEDIUMORCHID("#BA55D3"),
    MEDIUMPURPLE("#9370DB"),
    MEDIUMSEAGREEN("#3CB371"),
    MEDIUMSLATEBLUE("#7B68EE"),
    MEDIUMSPRINGGREEN("#00FA9A"),
    MEDIUMTURQUOISE("#48D1CC"),
    MEDIUMVIOLETRED("#C71585"),
    MIDNIGHTBLUE("#191970"),
    MINTCREAM("#F5FFFA"),
    MISTYROSE("#FFE4E1"),
    MOCCASIN("#FFE4B5"),
    NAVAJOWHITE("#FFDEAD"),
    NAVY("#000080"),
    OLDLACE("#FDF5E6"),
    OLIVE("#808000"),
    OLIVEDRAB("#6B8E23"),
    ORANGE("#FFA500"),
    ORANGERED("#FF4500"),
    ORCHID("#DA70D6"),
    PALEGOLDENROD("#EEE8AA"),
    PALEGREEN("#98FB98"),
    PALETURQUOISE("#AFEEEE"),
    PALEVIOLETRED("#DB7093"),
    PAPAYAWHIP("#FFEFD5"),
    PEACHPUFF("#FFDAB9"),
    PERU("#CD853F"),
    PINK("#FFC0CB"),
    PLUM("#DDA0DD"),
    POWDERBLUE("#B0E0E6"),
    PURPLE("#800080"),
    REBECCAPURPLE("#663399"),
    RED("#FF0000"),
    ROSYBROWN("#BC8F8F"),
    ROYALBLUE("#4169E1"),
    SADDLEBROWN("#8B4513"),
    SALMON("#FA8072"),
    SANDYBROWN("#F4A460"),
    SEAGREEN("#2E8B57"),
    SEASHELL("#FFF5EE"),
    SIENNA("#A0522D"),
    SILVER("#C0C0C0"),
    SKYBLUE("#87CEEB"),
    SLATEBLUE("#6A5ACD"),
    SLATEGRAY("#708090"),
    SLATEGREY("#708090"),
    SNOW("#FFFAFA"),
    SPRINGGREEN("#00FF7F"),
    STEELBLUE("#4682B4"),
    TAN("#D2B48C"),
    TEAL("#008080"),
    THISTLE("#D8BFD8"),
    TOMATO("#FF6347"),
    TURQUOISE("#40E0D0"),
    VIOLET("#EE82EE"),
    WHEAT("#F5DEB3"),
    WHITE("#FFFFFF"),
    WHITESMOKE("#F5F5F5"),
    YELLOW("#FFFF00"),
    YELLOWGREEN("#9ACD32"),

    //Legacy non-html-standard colors.  Duplicates removed.  Conflicts resolved with a LEGACY_ prefix.
    VIVID_RED(231,  47,  39),
    VIVID_YELLOWRED(238,  113,  25),
    VIVID_YELLOW(255,  200,  8),
    VIVID_GREENYELLOW(170,  198,  27),
    VIVID_GREEN(19,  166,  50),
    VIVID_BLUEGREEN(4,  148,  87),
    VIVID_BLUE(1,  134,  141),
    VIVID_PURPLEBLUE(3,  86,  155),
    VIVID_PURPLE(46,  20,  141),
    VIVID_REDPURPLE(204,  63,  92),
    STRONG_RED(207,  46,  49),
    STRONG_YELLOWRED(226,  132,  45),
    STRONG_YELLOW(227,  189,  28),
    STRONG_GREENYELLOW(162,  179,  36),
    STRONG_GREEN(18,  154,  47),
    STRONG_BLUEGREEN(6,  134,  84),
    STRONG_BLUE(3,  130,  122),
    STRONG_PURPLEBLUE(6,  113,  148),
    STRONG_PURPLE(92,  104,  163),
    STRONG_REDPURPLE(175,  92,  87),
    BRIGHT_RED(231,  108,  86),
    BRIGHT_YELLOWRED(241,  176,  102),
    BRIGHT_YELLOW(255,  228,  15),
    BRIGHT_GREENYELLOW(169,  199,  35),
    BRIGHT_GREEN(88,  171,  45),
    BRIGHT_BLUEGREEN(43,  151,  89),
    BRIGHT_BLUE(0,  147,  159),
    BRIGHT_PURPLEBLUE(59,  130,  157),
    BRIGHT_PURPLE(178,  137,  166),
    BRIGHT_REDPURPLE(209,  100,  109),
    PALE_RED(233,  163,  144),
    PALE_YELLOWRED(242,  178,  103),
    PALE_YELLOW(255,  236,  79),
    PALE_GREENYELLOW(219,  220,  93),
    PALE_GREEN(155,  196,  113),
    PALE_BLUEGREEN(146,  198,  131),
    PALE_BLUE(126,  188,  209),
    PALE_PURPLEBLUE(147,  184,  213),
    PALE_PURPLE(197,  188,  213),
    PALE_REDPURPLE(218,  176,  176),
    VERYPALE_RED(236,  217,  202),
    VERYPALE_YELLOWRED(245,  223,  181),
    VERYPALE_YELLOW(249,  239,  189),
    VERYPALE_GREENYELLOW(228,  235,  191),
    VERYPALE_GREEN(221,  232,  207),
    VERYPALE_BLUEGREEN(209,  234,  211),
    VERYPALE_BLUE(194,  222,  242),
    VERYPALE_PURPLEBLUE(203,  215,  232),
    VERYPALE_PURPLE(224,  218,  230),
    VERYPALE_REDPURPLE(235,  219,  224),
    LIGHTGRAYISH_RED(213,  182,  166),
    LIGHTGRAYISH_YELLOWRED(218,  196,  148),
    LIGHTGRAYISH_YELLOW(233,  227,  143),
    LIGHTGRAYISH_GREENYELLOW(209,  213,  165),
    LIGHTGRAYISH_GREEN(179,  202,  157),
    LIGHTGRAYISH_BLUEGREEN(166,  201,  163),
    LIGHTGRAYISH_BLUE(127,  175,  166),
    LIGHTGRAYISH_PURPLEBLUE(165,  184,  199),
    LIGHTGRAYISH_PURPLE(184,  190,  189),
    LIGHTGRAYISH_REDPURPLE(206,  185,  179),
    LIGHT_RED(211,  142,  110),
    LIGHT_YELLOWRED(215,  145,  96),
    LIGHT_YELLOW(255,  203,  88),
    LIGHT_GREENYELLOW(195,  202,  101),
    LIGHT_GREEN(141,  188,  90),
    LIGHT_BLUEGREEN(140,  195,  110),
    LIGHT_BLUE(117,  173,  169),
    LIGHT_PURPLEBLUE(138,  166,  187),
    LIGHT_PURPLE(170,  165,  199),
    LIGHT_REDPURPLE(205,  154,  149),
    GRAYISH_RED(171,  131,  115),
    GRAYISH_YELLOWRED(158,  128,  110),
    GRAYISH_YELLOW(148,  133,  105),
    GRAYISH_GREENYELLOW(144,  135,  96),
    GRAYISH_GREEN(143,  162,  121),
    GRAYISH_BLUEGREEN(122,  165,  123),
    GRAYISH_BLUE(130,  154,  145),
    GRAYISH_PURPLEBLUE(133,  154,  153),
    GRAYISH_PURPLE(151,  150,  139),
    GRAYISH_REDPURPLE(160,  147,  131),
    DULL_RED(162,  88,  61),
    DULL_YELLOWRED(167,  100,  67),
    DULL_YELLOW(139,  117,  65),
    DULL_GREENYELLOW(109,  116,  73),
    DULL_GREEN(88,  126,  61),
    DULL_BLUEGREEN(39,  122,  62),
    DULL_BLUE(24,  89,  63),
    DULL_PURPLEBLUE(53,  109,  98),
    DULL_PURPLE(44,  77,  143),
    DULL_REDPURPLE(115,  71,  79),
    DEEP_RED(172,  36,  48),
    DEEP_YELLOWRED(169,  87,  49),
    DEEP_YELLOW(156,  137,  37),
    DEEP_GREENYELLOW(91,  132,  47),
    DEEP_GREEN(20,  114,  48),
    DEEP_BLUEGREEN(23,  106,  43),
    DEEP_BLUE(20,  88,  60),
    DEEP_PURPLEBLUE(8,  87,  107),
    DEEP_PURPLE(58,  55,  119),
    DEEP_REDPURPLE(111,  61,  56),
    DARK_RED(116,  47,  50),
    DARK_YELLOWRED(115,  63,  44),
    DARK_YELLOW(103,  91,  44),
    DARK_GREENYELLOW(54,  88,  48),
    DARK_GREEN(30,  98,  50),
    DARK_BLUEGREEN(27,  86,  49),
    DARK_BLUE(18,  83,  65),
    DARK_PURPLEBLUE(16,  76,  84),
    DARK_PURPLE(40,  57,  103),
    DARK_REDPURPLE(88,  60,  50),
    DARKGRAYISH_RED(79,  46,  43),
    DARKGRAYISH_YELLOWRED(85,  55,  43),
    DARKGRAYISH_YELLOW(75,  63,  45),
    DARKGRAYISH_GREENYELLOW(44,  60,  49),
    DARKGRAYISH_GREEN(34,  62,  51),
    DARKGRAYISH_BLUEGREEN(31,  56,  45),
    DARKGRAYISH_BLUE(29,  60,  47),
    DARKGRAYISH_PURPLEBLUE(25,  62,  63),
    DARKGRAYISH_PURPLE(34,  54,  68),
    DARKGRAYISH_REDPURPLE(53,  52,  48),
    GRAY1(28,  28,  28),
    GRAY2(56,  56,  56),
    GRAY3(84,  84,  84),
    GRAY4(112,  112,  112),
    GRAY5(140,  140,  140),
    GRAY6(168,  168,  168),
    GRAY7(196,  196,  196),
    GRAY8(224,  224,  224),
    LEGACY_PINK(255,  175,  175),
    LEGACY_ORANGE(255,  200,  0),
    LEGACY_GREEN(0,  255,  0),
    NO_FORMATTING(0, 0, 0);

    // @formatter:on

    private final Color color;
    private final long columnFormat;

    Colors(final int r, final int g, final int b) {
        this.color = Color.colorRGB(r, g, b);
        this.columnFormat = r + g + b == 0 ? 0 : backgroundForegroundAuto(r, g, b);
    }

    Colors(final String c) {
        this.color = Color.color(c);
        final int r = this.color.javaColor().getRed();
        final int g = this.color.javaColor().getGreen();
        final int b = this.color.javaColor().getBlue();
        this.columnFormat = backgroundForegroundAuto(r, g, b);
    }

    /**
     * Gets this name's defined {@link Color}
     *
     * @return this {@link Color}
     */
    public Color color() {
        return color;
    }

    /**
     * Gets this name's defined column format value
     *
     * @return this column format
     */
    public long format() {
        return columnFormat;
    }

    /**
     * Gets the formatting long of the input color
     *
     * @param color color
     * @return formatting long of the input color
     */
    public static long toLong(final Colors color) {
        return color == null ? 0 : color.format();
    }

    /**
     * Gets the formatting long of the input color
     *
     * @param color color
     * @return formatting long of the input color
     */
    public static long toLong(final long color) {
        return color == NULL_LONG ? 0 : color;
    }

    /**
     * Gets the formatting long of the input color
     *
     * @param color color
     * @return formatting long of the input color
     */
    public static long toLong(final Color color) {
        return color == null ? 0
                : backgroundForegroundAuto(color.javaColor().getRed(), color.javaColor().getGreen(),
                        color.javaColor().getBlue());
    }

    /**
     * Gets the formatting long of the input color
     *
     * @param color color
     * @return formatting long of the input color
     * @throws IllegalArgumentException If {@code color} is invalid
     */
    public static long toLong(final String color) {
        return color == null ? 0 : toLong(Color.color(color));
    }

    /**
     * Creates a table format encoding with background color equal to the input RGB and unformatted foreground.
     *
     * @param r red component
     * @param g green component
     * @param b blue component
     * @return table format encoding the RGB background color and an unformatted foreground.
     */
    public static long background(long r, long g, long b) {
        return (0x01L << 56) |
                (r << 48) |
                (g << 40) |
                (b << 32);
    }

    /**
     * Convenience method for {@link #background(long, long, long)}.
     */
    public static long bg(long r, long g, long b) {
        return background(r, g, b);
    }

    /**
     * Creates a table format encoding with foreground color equal to the input RGB and unformatted background.
     *
     * @param r red component
     * @param g green component
     * @param b blue component
     * @return table format encoding the RGB foreground color and an unformatted background.
     */
    public static long foreground(long r, long g, long b) {
        return (0x01L << 24) |
                (r << 16) |
                (g << 8) |
                (b);
    }

    /**
     * Convenience method for {@link #foreground(long, long, long)}.
     */
    public static long fg(long r, long g, long b) {
        return foreground(r, g, b);
    }

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
    public static long backgroundForeground(long bgr, long bgg, long bgb, long fgr, long fgg, long fgb) {
        return bg(bgr, bgg, bgb) | fg(fgr, fgg, fgb);
    }

    /**
     * Convenience method for {@link #backgroundForeground(long, long, long, long, long, long)}
     */
    public static long bgfg(long bgr, long bgg, long bgb, long fgr, long fgg, long fgb) {
        return backgroundForeground(bgr, bgg, bgb, fgr, fgg, fgb);
    }

    /**
     * Creates a table format encoding with specified background color and automatically chosen contrasting foreground
     * color.
     *
     * @param bgr red component of the background color
     * @param bgg green component of the background color
     * @param bgb blue component of the background color
     * @return table format encoding with background color and auto-generated foreground color
     */
    public static long backgroundForegroundAuto(long bgr, long bgg, long bgb) {
        final long bg = bg(bgr, bgg, bgb);
        final long fg = yiq(bgr, bgg, bgb) >= 128 ? 0x01000000L : 0x01e0e0e0L;
        return bg | fg;
    }

    /**
     * Convenience method for {@link #backgroundForegroundAuto(long, long, long)}
     */
    public static long bgfga(long bgr, long bgg, long bgb) {
        return backgroundForegroundAuto(bgr, bgg, bgb);
    }

    /**
     * Creates a table format encoding for the heat map at {@code value}. A contrasting foreground color is
     * automatically chosen.
     *
     * @param value determines the color used by its location in the heat map's range
     * @param min minimum value of the heat map range
     * @param max maximum value of the heat map range
     * @param bg1 background color at or below the minimum value of the heat map
     * @param bg2 background color at or above the maximum value of the heat map
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
     * Creates a table format encoding with specified foreground color and unformatted background.
     *
     * @param value determines the color used by its location in the heat map's range
     * @param min minimum value of the heat map range
     * @param max maximum value of the heat map range
     * @param fg1 foreground color at or below the minimum value of the heat map
     * @param fg2 foreground color at or above the maximum value of the heat map
     * @return table format encoding with foreground color determined by a heat map
     */
    public static long heatmapForeground(double value, double min, double max, long fg1, long fg2) {
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
     * Convenience method for {@link #heatmapForeground(double, double, double, long, long)}
     */
    public static long heatmapFg(double value, double min, double max, long fg1, long fg2) {
        return heatmapForeground(value, min, max, fg1, fg2);
    }

    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /**
     * Creates a table format encoding with specified background color and an unformatted foreground.
     *
     * @param color color encoding
     * @return table format encoding with specified background color and unformatted foreground
     */
    public static long background(long color) {
        return color & 0xffffffff00000000L;
    }

    /**
     * Convenience method for {@link #background(long)}
     */
    public static long bg(long color) {
        return background(color);
    }

    /**
     * Creates a table format encoding with specified foreground color and unformatted background.
     *
     * @param color color encoding
     * @return table format encoding with specified foreground color and unformatted background
     */
    public static long foreground(long color) {
        return (color & 0xffffffff00000000L) >>> 32;
    }

    /**
     * Convenience method for {@link #foreground(long)}
     */
    public static long fg(long color) {
        return foreground(color);
    }

    private static long yiq(long r, long g, long b) {
        return (r * 299 + g * 587 + b * 114) / 1000;
    }
}
