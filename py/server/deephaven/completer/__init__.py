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

# TODO: make this python <= 3.8
from __future__ import annotations
from enum import Enum
from typing import Any
from jedi import Interpreter

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

    def do_completion(self, uri: str, version: int, line: int, col: int) -> list[list[Any]]:
        if not self._versions[uri] == version:
            # if you aren't the newest completion, you get nothing, quickly
            return []

        # run jedi
        txt = self.get_doc(uri)
        completions = Interpreter(txt, [globals()]).complete(line, col)
        # for now, a simple sorting based on number of preceding _
        # we may want to apply additional sorting to each list before combining
        results: list = []
        results_: list = []
        results__: list = []
        for complete in completions:
            # keep checking the latest version as we run, so updated doc can cancel us
            if not self._versions[uri] == version:
                return []
            result: list = self.to_result(complete, col)
            if result[0].startswith('__'):
                results__.append(result)
            elif result[0].startswith('_'):
                results_.append(result)
            else:
                results.append(result)

        # put the results together in a better-than-nothing sorting
        return results + results_ + results__

    @staticmethod
    def to_result(complete: Any, col: int) -> list[Any]:
        name: str = complete.name
        prefix_length: int = complete.get_completion_prefix_length()
        start: int = col - prefix_length
        # all java needs to build a grpc response is completion text (name) and where the completion should start
        return [name, start]


jedi_settings = Completer()

if jedi_settings.mode == CompleterMode.off:
    print('No jedi library installed on system path, disabling autocomplete')

if jedi_settings.mode == CompleterMode.strong:
    # start a strong-mode completer thread in bg
    pass

if jedi_settings.mode == CompleterMode.safe:
    # start a safe-mode completer thread in bg
    pass