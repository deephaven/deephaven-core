#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module implements the Font class that can be used to set the fonts on a plot. """

from enum import Enum

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper

_JFont = jpy.get_type("io.deephaven.plot.Font")
_JFontStyle = jpy.get_type("io.deephaven.plot.Font$FontStyle")
_JPlottingConvenience = jpy.get_type("io.deephaven.plot.PlottingConvenience")


def font_family_names():
    """ A list of supported font family names. """
    return list(_JPlottingConvenience.fontFamilyNames())


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
    """  A specific font, defined in terms of family, style, and size. """

    j_object_type = _JFont

    @property
    def j_object(self) -> jpy.JType:
        return self.j_font

    def __init__(self, family: str = 'Arial', style: FontStyle = FontStyle.PLAIN, size: int = 8):
        """ Creates a Font object.

        Args:
            family (str): the font family, defaults to 'Arial'
            style (FontStyle): the font style, defaults to FontStyle.PLAIN
            size (int): the point size of the Font, defaults to 8
        """
        try:
            self.j_font = _JFont.font(family, style.value, size)
        except Exception as e:
            raise DHError(e, "failed to create a font.") from e
