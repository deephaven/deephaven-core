#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module implements the Color class and provides a list of predefined colors that can be used to paint a plot.
"""

from __future__ import annotations

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper

_JColor = jpy.get_type("io.deephaven.gui.color.Color")


class Color(JObjectWrapper):
    """ A color. """

    j_object_type = _JColor

    @property
    def j_object(self) -> jpy.JType:
        return self.j_color

    def __init__(self, j_color: jpy.JType):
        self.j_color = j_color

    def to_hex(self) -> str:
        return self.j_color.toHex()

    @staticmethod
    def of_name(name: str) -> Color:
        """ Creates a Color instance represented by the name string.

        Colors are specified by name or hex value.
        Hex values are parsed as follows: first two digits set the Red component of the color; second two digits set the
        Green component; third two the Blue. Hex values must have a "#" in front, e.g. "#001122"

        Args:
            name (str): the name of the color

        Returns:
             a Color

        Raises:
            DHError
        """
        try:
            return Color(j_color=_JColor.color(name))
        except Exception as e:
            raise DHError(e, "failed to get a color by its name.") from e

    @staticmethod
    def of_rgb(r: int, g: int, b: int, alpha: int = 255) -> Color:
        """ Creates a Color with the specified red, green, blue, and alpha values.

        Args:
            r (int): the red component in the range (0 - 255)
            g (int): the green component in the range (0 - 255)
            b (int): the blue component in the range (0 - 255)
            alpha (int, optional): the alpha component in the range (0 - 255), default is 255. Lower alpha means more
                transparent for the color.

        Returns:
            a Color

        Raises:
            DHError
        """
        try:
            return Color(j_color=_JColor.colorRGB(r, g, b, alpha))
        except Exception as e:
            raise DHError(e, "failed to create a color from rgb values.")

    @staticmethod
    def of_rgb_f(r: float, g: float, b: float, alpha: float = 1.0) -> Color:
        """ Creates a Color with the specified red, green, blue, and alpha values expressed in floating numbers.

        Args:
            r (float): the red component in the range (0.0 - 1.0)
            g (float): the green component in the range (0.0 - 1.0)
            b (float): the blue component in the range (0.0 - 1.0)
            alpha (float, optional): the alpha component in the range (0.0 - 1.0), default is 1.0. Lower alpha means
                more transparent for the color.

        Returns:
            a color

        Raises:
            DHError
        """
        try:
            return Color(j_color=_JColor.colorRGB(r, g, b, alpha))
        except Exception as e:
            raise DHError(e, "failed to create a color from rgb values.") from e

    @staticmethod
    def of_hsl(h: float, s: float, l: float, alpha: float = 1.0) -> Color:
        """ Creates a Color with the specified hue, saturation, lightness, and alpha. The lower the alpha, the more
        transparent the color.

        Args:
            h (float): the hue component, as a degree on the color wheel in the range (0.0 - 360.0)
            s (float): the saturation component, as a percentage in the range (1.0 - 100.0)
            l (float): the lightness component, as a percentage in the range (1.0 - 100.0)
            alpha (float, optional): the alpha component in the range (0.0 - 1.0), default is 1.0. Lower alpha means
                more transparent for the color.

        Returns:
            a Color

        Raises:
            DHError
        """
        try:
            return Color(j_color=_JColor.colorHSL(h, s, l, alpha))
        except Exception as e:
            raise DHError(e, "failed to create a color from hue, saturation, lightness values.") from e


class Colors:
    """ The Colors class provides a namespace for all the predefined colors. """
    ALICEBLUE = Color.of_name("#F0F8FF")
    ANTIQUEWHITE = Color.of_name("#FAEBD7")
    AQUA = Color.of_name("#00FFFF")
    AQUAMARINE = Color.of_name("#7FFFD4")
    AZURE = Color.of_name("#F0FFFF")
    BEIGE = Color.of_name("#F5F5DC")
    BISQUE = Color.of_name("#FFE4C4")
    BLACK = Color.of_name("#000000")
    BLANCHEDALMOND = Color.of_name("#FFEBCD")
    BLUE = Color.of_name("#0000FF")
    BLUEVIOLET = Color.of_name("#8A2BE2")
    BROWN = Color.of_name("#A52A2A")
    BURLYWOOD = Color.of_name("#DEB887")
    CADETBLUE = Color.of_name("#5F9EA0")
    CHARTREUSE = Color.of_name("#7FFF00")
    CHOCOLATE = Color.of_name("#D2691E")
    CORAL = Color.of_name("#FF7F50")
    CORNFLOWERBLUE = Color.of_name("#6495ED")
    CORNSILK = Color.of_name("#FFF8DC")
    CRIMSON = Color.of_name("#DC143C")
    CYAN = Color.of_name("#00FFFF")
    DARKBLUE = Color.of_name("#00008B")
    DARKCYAN = Color.of_name("#008B8B")
    DARKGOLDENROD = Color.of_name("#B8860B")
    DARKGRAY = Color.of_name("#A9A9A9")
    DARKGREY = Color.of_name("#A9A9A9")
    DARKGREEN = Color.of_name("#006400")
    DARKKHAKI = Color.of_name("#BDB76B")
    DARKMAGENTA = Color.of_name("#8B008B")
    DARKOLIVEGREEN = Color.of_name("#556B2F")
    DARKORANGE = Color.of_name("#FF8C00")
    DARKORCHID = Color.of_name("#9932CC")
    DARKRED = Color.of_name("#8B0000")
    DARKSALMON = Color.of_name("#E9967A")
    DARKSEAGREEN = Color.of_name("#8FBC8F")
    DARKSLATEBLUE = Color.of_name("#483D8B")
    DARKSLATEGRAY = Color.of_name("#2F4F4F")
    DARKSLATEGREY = Color.of_name("#2F4F4F")
    DARKTURQUOISE = Color.of_name("#00CED1")
    DARKVIOLET = Color.of_name("#9400D3")
    DEEPPINK = Color.of_name("#FF1493")
    DEEPSKYBLUE = Color.of_name("#00BFFF")
    DIMGRAY = Color.of_name("#696969")
    DIMGREY = Color.of_name("#696969")
    DODGERBLUE = Color.of_name("#1E90FF")
    FIREBRICK = Color.of_name("#B22222")
    FLORALWHITE = Color.of_name("#FFFAF0")
    FORESTGREEN = Color.of_name("#228B22")
    FUCHSIA = Color.of_name("#FF00FF")
    GAINSBORO = Color.of_name("#DCDCDC")
    GHOSTWHITE = Color.of_name("#F8F8FF")
    GOLD = Color.of_name("#FFD700")
    GOLDENROD = Color.of_name("#DAA520")
    GRAY = Color.of_name("#808080")
    GREY = Color.of_name("#808080")
    GREEN = Color.of_name("#008000")
    GREENYELLOW = Color.of_name("#ADFF2F")
    HONEYDEW = Color.of_name("#F0FFF0")
    HOTPINK = Color.of_name("#FF69B4")
    INDIANRED = Color.of_name("#CD5C5C")
    INDIGO = Color.of_name("#4B0082")
    IVORY = Color.of_name("#FFFFF0")
    KHAKI = Color.of_name("#F0E68C")
    LAVENDER = Color.of_name("#E6E6FA")
    LAVENDERBLUSH = Color.of_name("#FFF0F5")
    LAWNGREEN = Color.of_name("#7CFC00")
    LEMONCHIFFON = Color.of_name("#FFFACD")
    LIGHTBLUE = Color.of_name("#ADD8E6")
    LIGHTCORAL = Color.of_name("#F08080")
    LIGHTCYAN = Color.of_name("#E0FFFF")
    LIGHTGOLDENRODYELLOW = Color.of_name("#FAFAD2")
    LIGHTGRAY = Color.of_name("#D3D3D3")
    LIGHTGREY = Color.of_name("#D3D3D3")
    LIGHTGREEN = Color.of_name("#90EE90")
    LIGHTPINK = Color.of_name("#FFB6C1")
    LIGHTSALMON = Color.of_name("#FFA07A")
    LIGHTSEAGREEN = Color.of_name("#20B2AA")
    LIGHTSKYBLUE = Color.of_name("#87CEFA")
    LIGHTSLATEGRAY = Color.of_name("#778899")
    LIGHTSLATEGREY = Color.of_name("#778899")
    LIGHTSTEELBLUE = Color.of_name("#B0C4DE")
    LIGHTYELLOW = Color.of_name("#FFFFE0")
    LIME = Color.of_name("#00FF00")
    LIMEGREEN = Color.of_name("#32CD32")
    LINEN = Color.of_name("#FAF0E6")
    MAGENTA = Color.of_name("#FF00FF")
    MAROON = Color.of_name("#800000")
    MEDIUMAQUAMARINE = Color.of_name("#66CDAA")
    MEDIUMBLUE = Color.of_name("#0000CD")
    MEDIUMORCHID = Color.of_name("#BA55D3")
    MEDIUMPURPLE = Color.of_name("#9370DB")
    MEDIUMSEAGREEN = Color.of_name("#3CB371")
    MEDIUMSLATEBLUE = Color.of_name("#7B68EE")
    MEDIUMSPRINGGREEN = Color.of_name("#00FA9A")
    MEDIUMTURQUOISE = Color.of_name("#48D1CC")
    MEDIUMVIOLETRED = Color.of_name("#C71585")
    MIDNIGHTBLUE = Color.of_name("#191970")
    MINTCREAM = Color.of_name("#F5FFFA")
    MISTYROSE = Color.of_name("#FFE4E1")
    MOCCASIN = Color.of_name("#FFE4B5")
    NAVAJOWHITE = Color.of_name("#FFDEAD")
    NAVY = Color.of_name("#000080")
    OLDLACE = Color.of_name("#FDF5E6")
    OLIVE = Color.of_name("#808000")
    OLIVEDRAB = Color.of_name("#6B8E23")
    ORANGE = Color.of_name("#FFA500")
    ORANGERED = Color.of_name("#FF4500")
    ORCHID = Color.of_name("#DA70D6")
    PALEGOLDENROD = Color.of_name("#EEE8AA")
    PALEGREEN = Color.of_name("#98FB98")
    PALETURQUOISE = Color.of_name("#AFEEEE")
    PALEVIOLETRED = Color.of_name("#DB7093")
    PAPAYAWHIP = Color.of_name("#FFEFD5")
    PEACHPUFF = Color.of_name("#FFDAB9")
    PERU = Color.of_name("#CD853F")
    PINK = Color.of_name("#FFC0CB")
    PLUM = Color.of_name("#DDA0DD")
    POWDERBLUE = Color.of_name("#B0E0E6")
    PURPLE = Color.of_name("#800080")
    REBECCAPURPLE = Color.of_name("#663399")
    RED = Color.of_name("#FF0000")
    ROSYBROWN = Color.of_name("#BC8F8F")
    ROYALBLUE = Color.of_name("#4169E1")
    SADDLEBROWN = Color.of_name("#8B4513")
    SALMON = Color.of_name("#FA8072")
    SANDYBROWN = Color.of_name("#F4A460")
    SEAGREEN = Color.of_name("#2E8B57")
    SEASHELL = Color.of_name("#FFF5EE")
    SIENNA = Color.of_name("#A0522D")
    SILVER = Color.of_name("#C0C0C0")
    SKYBLUE = Color.of_name("#87CEEB")
    SLATEBLUE = Color.of_name("#6A5ACD")
    SLATEGRAY = Color.of_name("#708090")
    SLATEGREY = Color.of_name("#708090")
    SNOW = Color.of_name("#FFFAFA")
    SPRINGGREEN = Color.of_name("#00FF7F")
    STEELBLUE = Color.of_name("#4682B4")
    TAN = Color.of_name("#D2B48C")
    TEAL = Color.of_name("#008080")
    THISTLE = Color.of_name("#D8BFD8")
    TOMATO = Color.of_name("#FF6347")
    TURQUOISE = Color.of_name("#40E0D0")
    VIOLET = Color.of_name("#EE82EE")
    WHEAT = Color.of_name("#F5DEB3")
    WHITE = Color.of_name("#FFFFFF")
    WHITESMOKE = Color.of_name("#F5F5F5")
    YELLOW = Color.of_name("#FFFF00")
    YELLOWGREEN = Color.of_name("#9ACD32")

    # Legacy non-html-standard colors.
    VIVID_RED = Color.of_rgb(231, 47, 39)
    VIVID_YELLOWRED = Color.of_rgb(238, 113, 25)
    VIVID_YELLOW = Color.of_rgb(255, 200, 8)
    VIVID_GREENYELLOW = Color.of_rgb(170, 198, 27)
    VIVID_GREEN = Color.of_rgb(19, 166, 50)
    VIVID_BLUEGREEN = Color.of_rgb(4, 148, 87)
    VIVID_BLUE = Color.of_rgb(1, 134, 141)
    VIVID_PURPLEBLUE = Color.of_rgb(3, 86, 155)
    VIVID_PURPLE = Color.of_rgb(46, 20, 141)
    VIVID_REDPURPLE = Color.of_rgb(204, 63, 92)
    STRONG_RED = Color.of_rgb(207, 46, 49)
    STRONG_YELLOWRED = Color.of_rgb(226, 132, 45)
    STRONG_YELLOW = Color.of_rgb(227, 189, 28)
    STRONG_GREENYELLOW = Color.of_rgb(162, 179, 36)
    STRONG_GREEN = Color.of_rgb(18, 154, 47)
    STRONG_BLUEGREEN = Color.of_rgb(6, 134, 84)
    STRONG_BLUE = Color.of_rgb(3, 130, 122)
    STRONG_PURPLEBLUE = Color.of_rgb(6, 113, 148)
    STRONG_PURPLE = Color.of_rgb(92, 104, 163)
    STRONG_REDPURPLE = Color.of_rgb(175, 92, 87)
    BRIGHT_RED = Color.of_rgb(231, 108, 86)
    BRIGHT_YELLOWRED = Color.of_rgb(241, 176, 102)
    BRIGHT_YELLOW = Color.of_rgb(255, 228, 15)
    BRIGHT_GREENYELLOW = Color.of_rgb(169, 199, 35)
    BRIGHT_GREEN = Color.of_rgb(88, 171, 45)
    BRIGHT_BLUEGREEN = Color.of_rgb(43, 151, 89)
    BRIGHT_BLUE = Color.of_rgb(0, 147, 159)
    BRIGHT_PURPLEBLUE = Color.of_rgb(59, 130, 157)
    BRIGHT_PURPLE = Color.of_rgb(178, 137, 166)
    BRIGHT_REDPURPLE = Color.of_rgb(209, 100, 109)
    PALE_RED = Color.of_rgb(233, 163, 144)
    PALE_YELLOWRED = Color.of_rgb(242, 178, 103)
    PALE_YELLOW = Color.of_rgb(255, 236, 79)
    PALE_GREENYELLOW = Color.of_rgb(219, 220, 93)
    PALE_GREEN = Color.of_rgb(155, 196, 113)
    PALE_BLUEGREEN = Color.of_rgb(146, 198, 131)
    PALE_BLUE = Color.of_rgb(126, 188, 209)
    PALE_PURPLEBLUE = Color.of_rgb(147, 184, 213)
    PALE_PURPLE = Color.of_rgb(197, 188, 213)
    PALE_REDPURPLE = Color.of_rgb(218, 176, 176)
    VERYPALE_RED = Color.of_rgb(236, 217, 202)
    VERYPALE_YELLOWRED = Color.of_rgb(245, 223, 181)
    VERYPALE_YELLOW = Color.of_rgb(249, 239, 189)
    VERYPALE_GREENYELLOW = Color.of_rgb(228, 235, 191)
    VERYPALE_GREEN = Color.of_rgb(221, 232, 207)
    VERYPALE_BLUEGREEN = Color.of_rgb(209, 234, 211)
    VERYPALE_BLUE = Color.of_rgb(194, 222, 242)
    VERYPALE_PURPLEBLUE = Color.of_rgb(203, 215, 232)
    VERYPALE_PURPLE = Color.of_rgb(224, 218, 230)
    VERYPALE_REDPURPLE = Color.of_rgb(235, 219, 224)
    LIGHTGRAYISH_RED = Color.of_rgb(213, 182, 166)
    LIGHTGRAYISH_YELLOWRED = Color.of_rgb(218, 196, 148)
    LIGHTGRAYISH_YELLOW = Color.of_rgb(233, 227, 143)
    LIGHTGRAYISH_GREENYELLOW = Color.of_rgb(209, 213, 165)
    LIGHTGRAYISH_GREEN = Color.of_rgb(179, 202, 157)
    LIGHTGRAYISH_BLUEGREEN = Color.of_rgb(166, 201, 163)
    LIGHTGRAYISH_BLUE = Color.of_rgb(127, 175, 166)
    LIGHTGRAYISH_PURPLEBLUE = Color.of_rgb(165, 184, 199)
    LIGHTGRAYISH_PURPLE = Color.of_rgb(184, 190, 189)
    LIGHTGRAYISH_REDPURPLE = Color.of_rgb(206, 185, 179)
    LIGHT_RED = Color.of_rgb(211, 142, 110)
    LIGHT_YELLOWRED = Color.of_rgb(215, 145, 96)
    LIGHT_YELLOW = Color.of_rgb(255, 203, 88)
    LIGHT_GREENYELLOW = Color.of_rgb(195, 202, 101)
    LIGHT_GREEN = Color.of_rgb(141, 188, 90)
    LIGHT_BLUEGREEN = Color.of_rgb(140, 195, 110)
    LIGHT_BLUE = Color.of_rgb(117, 173, 169)
    LIGHT_PURPLEBLUE = Color.of_rgb(138, 166, 187)
    LIGHT_PURPLE = Color.of_rgb(170, 165, 199)
    LIGHT_REDPURPLE = Color.of_rgb(205, 154, 149)
    GRAYISH_RED = Color.of_rgb(171, 131, 115)
    GRAYISH_YELLOWRED = Color.of_rgb(158, 128, 110)
    GRAYISH_YELLOW = Color.of_rgb(148, 133, 105)
    GRAYISH_GREENYELLOW = Color.of_rgb(144, 135, 96)
    GRAYISH_GREEN = Color.of_rgb(143, 162, 121)
    GRAYISH_BLUEGREEN = Color.of_rgb(122, 165, 123)
    GRAYISH_BLUE = Color.of_rgb(130, 154, 145)
    GRAYISH_PURPLEBLUE = Color.of_rgb(133, 154, 153)
    GRAYISH_PURPLE = Color.of_rgb(151, 150, 139)
    GRAYISH_REDPURPLE = Color.of_rgb(160, 147, 131)
    DULL_RED = Color.of_rgb(162, 88, 61)
    DULL_YELLOWRED = Color.of_rgb(167, 100, 67)
    DULL_YELLOW = Color.of_rgb(139, 117, 65)
    DULL_GREENYELLOW = Color.of_rgb(109, 116, 73)
    DULL_GREEN = Color.of_rgb(88, 126, 61)
    DULL_BLUEGREEN = Color.of_rgb(39, 122, 62)
    DULL_BLUE = Color.of_rgb(24, 89, 63)
    DULL_PURPLEBLUE = Color.of_rgb(53, 109, 98)
    DULL_PURPLE = Color.of_rgb(44, 77, 143)
    DULL_REDPURPLE = Color.of_rgb(115, 71, 79)
    DEEP_RED = Color.of_rgb(172, 36, 48)
    DEEP_YELLOWRED = Color.of_rgb(169, 87, 49)
    DEEP_YELLOW = Color.of_rgb(156, 137, 37)
    DEEP_GREENYELLOW = Color.of_rgb(91, 132, 47)
    DEEP_GREEN = Color.of_rgb(20, 114, 48)
    DEEP_BLUEGREEN = Color.of_rgb(23, 106, 43)
    DEEP_BLUE = Color.of_rgb(20, 88, 60)
    DEEP_PURPLEBLUE = Color.of_rgb(8, 87, 107)
    DEEP_PURPLE = Color.of_rgb(58, 55, 119)
    DEEP_REDPURPLE = Color.of_rgb(111, 61, 56)
    DARK_RED = Color.of_rgb(116, 47, 50)
    DARK_YELLOWRED = Color.of_rgb(115, 63, 44)
    DARK_YELLOW = Color.of_rgb(103, 91, 44)
    DARK_GREENYELLOW = Color.of_rgb(54, 88, 48)
    DARK_GREEN = Color.of_rgb(30, 98, 50)
    DARK_BLUEGREEN = Color.of_rgb(27, 86, 49)
    DARK_BLUE = Color.of_rgb(18, 83, 65)
    DARK_PURPLEBLUE = Color.of_rgb(16, 76, 84)
    DARK_PURPLE = Color.of_rgb(40, 57, 103)
    DARK_REDPURPLE = Color.of_rgb(88, 60, 50)
    DARKGRAYISH_RED = Color.of_rgb(79, 46, 43)
    DARKGRAYISH_YELLOWRED = Color.of_rgb(85, 55, 43)
    DARKGRAYISH_YELLOW = Color.of_rgb(75, 63, 45)
    DARKGRAYISH_GREENYELLOW = Color.of_rgb(44, 60, 49)
    DARKGRAYISH_GREEN = Color.of_rgb(34, 62, 51)
    DARKGRAYISH_BLUEGREEN = Color.of_rgb(31, 56, 45)
    DARKGRAYISH_BLUE = Color.of_rgb(29, 60, 47)
    DARKGRAYISH_PURPLEBLUE = Color.of_rgb(25, 62, 63)
    DARKGRAYISH_PURPLE = Color.of_rgb(34, 54, 68)
    DARKGRAYISH_REDPURPLE = Color.of_rgb(53, 52, 48)
    GRAY1 = Color.of_rgb(28, 28, 28)
    GRAY2 = Color.of_rgb(56, 56, 56)
    GRAY3 = Color.of_rgb(84, 84, 84)
    GRAY4 = Color.of_rgb(112, 112, 112)
    GRAY5 = Color.of_rgb(140, 140, 140)
    GRAY6 = Color.of_rgb(168, 168, 168)
    GRAY7 = Color.of_rgb(196, 196, 196)
    GRAY8 = Color.of_rgb(224, 224, 224)
    DB_PINK = Color.of_rgb(255, 175, 175)
    DB_ORANGE = Color.of_rgb(255, 200, 0)
    DB_GREEN = Color.of_rgb(0, 255, 0)
    NO_FORMATTING = Color.of_rgb(0, 0, 0)
