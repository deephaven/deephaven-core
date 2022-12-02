#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module allows the user to configure if and how we use jedi to perform autocompletion.
See https://github.com/davidhalter/jedi for information on jedi.

# To disable autocompletion
from deephaven.completer import jedi_settings
jedi_settings.mode = 'off'

Valid options for completer_mode are one of: [off, safe, strong].
off: do not use any autocomplete
safe mode: uses static analysis of source files. Can't execute any code.
strong mode: looks in your globals() for answers to autocomplete and analyzes your runtime python objects
later, we may add slow mode, which uses both static and interpreted completion modes.
"""

from deephaven.completer._completer import Completer
from jedi import preload_module, Interpreter

jedi_settings = Completer()
# warm jedi up a little. We could probably off-thread this.
preload_module('deephaven')
Interpreter('', []).complete(1, 0)
