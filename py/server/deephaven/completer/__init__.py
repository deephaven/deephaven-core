#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module allows the user to configure if and how we use jedi to perform autocompletion.
See https://github.com/davidhalter/jedi for information on jedi.

# To disable autocompletion
from deephaven.completer import CompleterSettings, CompleterMode
CompletionSettings.mode = 'off'

Valid options for completer_mode are one of: [off, safe, strong].
off: do not use any autocomplete
safe mode: uses static analysis of source files. Can't execute any code.
strong mode: looks in your globals() for answers to autocomplete and analyzes your runtime python objects
later, we may add slow mode, which uses both static and interpreted completion modes.
"""

from enum import Enum


class CompleterMode(Enum):
    off = 'off'
    safe = 'safe'
    strong = 'strong'

    def __str__(self) -> str:
        return self.value


class Completer(object):

    def __init__(self):
        self._docs = {}
        self._versions = {}
        # might want to make this a {uri: []} instead of []
        self.pending = []
        try:
            import jedi
            self.__can_jedi = True
            self.mode = CompleterMode.strong
        except ImportError:
            self.__can_jedi = False
            self.mode = CompleterMode.off

    @property
    def mode(self) -> CompleterMode:
        return self.__mode

    @mode.setter
    def mode(self, mode) -> None:
        if type(mode) == 'str':
            mode = CompleterMode[mode]
        self.__mode = mode

    def open_doc(self, text: str, uri: str, version: int) -> None:
        self._docs[uri] = text
        self._versions[uri] = version

    def get_doc(self, uri: str) -> str:
        return self._docs[uri]

    def update_doc(self, text: str, uri: str, version: int) -> None:
        self._docs[uri] = text
        self._versions[uri] = version
        # any pending completions should stop running now. We use a list of Event to signal any running threads to stop
        for pending in self.pending:
            pending.set()

    def close_doc(self, uri: str) -> None:
        del self._docs[uri]
        del self._versions[uri]
        for pending in self.pending:
            pending.set()

    def is_enabled(self) -> bool:
        return self.__mode != CompleterMode.off

    def can_jedi(self) -> bool:
        return self.__can_jedi


jedi_settings = Completer()

if jedi_settings.mode == CompleterMode.off:
    print('No jedi library installed on system path, disabling autocomplete')

if jedi_settings.mode == CompleterMode.strong:
    # start a strong-mode completer thread in bg
    pass


if jedi_settings.mode == CompleterMode.safe:
    # start a safe-mode completer thread in bg
    pass