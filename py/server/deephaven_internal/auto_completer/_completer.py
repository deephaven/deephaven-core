# only python 3.8 needs this, but it must be the first expression in the file, so we can't predicate it
from __future__ import annotations
from enum import Enum
from typing import Any
from jedi import Interpreter, Script


class Mode(Enum):
    OFF = "OFF"
    SAFE = "SAFE"
    STRONG = "STRONG"

    def __str__(self) -> str:
        return self.value


class Settings:
    def __init__(self):
        self._mode = Mode.STRONG

    @property
    def mode(self) -> Mode:
        return self._mode

    @mode.setter
    def mode(self, mode: Mode):
        self._mode = mode


jedi_settings = Settings()


class Completer(object):
    def __init__(self, scope: dict):
        self._docs = {}
        self._versions = {}
        self.__scope = scope
        # might want to make this a {uri: []} instead of []
        self.pending = []

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

    def do_completion(
        self, uri: str, version: int, line: int, col: int
    ) -> list[list[Any]]:
        global jedi_settings
        if jedi_settings.mode == Mode.OFF:
            return []

        if not self._versions[uri] == version:
            # if you aren't the newest completion, you get nothing, quickly
            return []

        # run jedi
        txt = self.get_doc(uri)

        # The Script completer is static analysis only, so we should actually be feeding it a whole document at once.
        if jedi_settings.mode == Mode.SAFE:
            completions = Script(txt).complete(line, col)
        else:
            completions = Interpreter(txt, [self.__scope]).complete(line, col)

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
            if result[0].startswith("__"):
                results__.append(result)
            elif result[0].startswith("_"):
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
