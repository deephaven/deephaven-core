#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" TODO """
from enum import Enum

import jpy

from deephaven2._wrapper_abc import JObjectWrapper

_JFont = jpy.get_type("io.deephaven.plot.Font")
_JFontStyle = jpy.get_type("io.deephaven.plot.Font$FontStyle")
_JPlottingConvenience = jpy.get_type("io.deephaven.plot.PlottingConvenience")

FontFamilyNames = list(_JPlottingConvenience.fontFamilyNames())
""" a list of supported font family names. """


class FontStyle(Enum):
    """ An enum of predefined font styles. """

    PLAIN = _JFontStyle.PLAIN
    """ Plain text. """

    BOLD = _JFontStyle.BOLD
    """ Bold text """

    ITALIC = _JFontStyle.ITALIC
    """ Italic text """

    BOLD_ITALIC = _JFontStyle.BOLD_ITALIC
    """ Bold and italic text """


class Font(JObjectWrapper):
    """ TODO """
    j_object_type = _JFont

    @property
    def j_object(self) -> jpy.JType:
        return self.j_font

    def __init__(self, family: str = 'Arial', style: FontStyle = FontStyle.PLAIN, size: int = 8):
        """ Inits a Font object.

        Args:
            family (str): the font family, defaults to 'Arial'
            style (FontStyle): the font style, defaults to FontStyle.PLAIN
            size (int): the point size of the Font, defaults to 8
        """
        self.j_font = _JFont.font(family, style.value, size)
