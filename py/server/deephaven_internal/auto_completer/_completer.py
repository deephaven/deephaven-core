# only python 3.8 needs this, but it must be the first expression in the file, so we can't predicate it
from __future__ import annotations
from enum import Enum
from typing import Any, Union, List
from jedi import Interpreter, Script
from jedi.api.classes import Completion, Signature


class Mode(Enum):
    OFF = "OFF"
    SAFE = "SAFE"
    STRONG = "STRONG"

    def __str__(self) -> str:
        return self.value


"""
These options are from Jedi LSP implementation and LSP spec
https://github.com/pappasam/jedi-language-server/blob/6b064bca61a3e56535c27cc8c8b20f45a3fcbe47/jedi_language_server/type_map.py#L8
https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#completionItemKind
"""
_JEDI_COMPLETION_TYPE_MAP = {
    "module": 9,
    "class": 7,
    "instance": 6,
    "function": 3,
    "param": 6,
    "path": 17,
    "keyword": 14,
    "property": 10,
    "statement": 6,
    "text": 1,
}


def wrap_python(txt: str) -> str:
    """ Wraps a string in a Python fenced codeblock for markdown

    Args:
        txt (str): the string to wrap

    Returns:
        A markdown string wrapping the input in a Python codeblock
    """
    if txt:
        return f"```python\n{txt}\n```"
    return ''


def wrap_plaintext(txt: str) -> str:
    """ Wraps a string in a Python fenced codeblock for markdown

        Args:
            txt (str): the string to wrap

        Returns:
            A markdown string wrapping the input in a Python codeblock
        """
    if txt:
        return f"```plaintext\n{txt}\n```"
    return ''


class Completer:
    def __init__(self):
        self._docs = {}
        self._versions = {}
        # we will replace this w/ top-level globals() when we open the document
        self.__scope = globals()
        # might want to make this a {uri: []} instead of []
        self.pending = []
        try:
            import jedi

            self.__can_jedi = True
            self.mode = Mode.STRONG
        except ImportError:
            self.__can_jedi = False
            self.mode = Mode.OFF

    @property
    def mode(self) -> Mode:
        return self.__mode

    @mode.setter
    def mode(self, mode) -> None:
        if type(mode) == "str":
            mode = Mode[mode]
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
        return self.__mode != Mode.OFF

    def can_jedi(self) -> bool:
        return self.__can_jedi

    def set_scope(self, scope: dict) -> None:
        self.__scope = scope

    def get_completer(self, uri: str) -> Union[Interpreter, Script]:
        txt = self.get_doc(uri)
        if self.__mode == Mode.SAFE:
            # The Script completer is static analysis only, so we should actually be feeding it a whole document at once
            return Script(txt)
        return Interpreter(txt, [self.__scope])

    def do_completion(
        self, uri: str, version: int, line: int, col: int
    ) -> List[List[Any]]:
        """ Gets completion items at the position

        Modeled after Jedi language server
        https://github.com/pappasam/jedi-language-server/blob/main/jedi_language_server/server.py#L189
        """
        if not self._versions[uri] == version:
            # if you aren't the newest completion, you get nothing, quickly
            return []

        completer = self.get_completer(uri)
        # Path completions don't seem useful with our setup
        # It also doesn't suggest nested paths/files when the string is a parent path
        # Might just be a client issue, but either way not useful right now
        completions = [c for c in completer.complete(line, col) if c.type != "path"]

        # for now, a simple sorting based on number of preceding _
        # we may want to apply additional sorting to each list before combining
        results: List = []
        results_: List = []
        results__: List = []
        for completion in completions:
            # keep checking the latest version as we run, so updated doc can cancel us
            if not self._versions[uri] == version:
                return []
            result: list = self.to_completion_result(completion, col)
            if result[0].startswith("__"):
                results__.append(result)
            elif result[0].startswith("_"):
                results_.append(result)
            else:
                results.append(result)

        # put the results together in a better-than-nothing sorting
        return results + results_ + results__

    @staticmethod
    def to_completion_result(completion: Completion, col: int) -> List[Any]:
        name: str = completion.name
        prefix_length: int = completion.get_completion_prefix_length()
        start: int = col - prefix_length
        signatures: List[Signature] = completion.get_signatures()
        detail: str = signatures[0].to_string() if len(signatures) > 0 else completion.description

        return [
            name,
            start,
            detail,
            completion.docstring(raw=True),
            _JEDI_COMPLETION_TYPE_MAP.get(completion.type, _JEDI_COMPLETION_TYPE_MAP.get('text', 1))
        ]

    def do_signature_help(
            self, uri: str, version: int, line: int, col: int
    ) -> List[List[Any]]:
        """ Gets signature help at the position

        Modeled after Jedi language server
        https://github.com/pappasam/jedi-language-server/blob/main/jedi_language_server/server.py#L255
        """
        if not self._versions[uri] == version:
            # if you aren't the newest, you get nothing, quickly
            return []

        completer = self.get_completer(uri)
        signatures = completer.get_signatures(line, col)

        results: list = []
        for signature in signatures:
            # keep checking the latest version as we run, so updated doc can cancel us
            if not self._versions[uri] == version:
                return []

            result: list = [
                signature.to_string(),
                signature.docstring(raw=True),
                [[param.to_string().strip(), param.docstring(raw=True).strip()] for param in signature.params],
                signature.index if signature.index is not None else -1
            ]
            results.append(result)

        return results

    def do_hover(
        self, uri: str, version: int, line: int, col: int
    ) -> str:
        """ Gets hover help at the position

        Modeled after Jedi language server
        https://github.com/pappasam/jedi-language-server/blob/main/jedi_language_server/server.py#L366
        """
        if not self._versions[uri] == version:
            # if you aren't the newest, you get nothing, quickly
            return ''

        completer = self.get_completer(uri)
        hovers = completer.help(line, col)
        if not hovers or hovers[0].type == "keyword":
            return ''

        # LSP doesn't support multiple hovers really. Not sure if/when Jedi would return multiple either
        hover = hovers[0]
        signatures = hover.get_signatures()
        kind = hover.type

        header = ""
        if signatures:
            header = f"{'def' if kind == 'function' else kind} {signatures[0].to_string()}"
        else:
            if kind == "class":
                header = f"class {hover.name}()"
            elif kind == "function":
                header = f"def {hover.name}()"
            elif kind == "property":
                header = f"property {hover.name}"
            else:
                header = hover.description

        hoverstring = wrap_python(header)
        raw_docstring = hover.docstring(raw=True)
        if raw_docstring:
            hoverstring += '\n---\n' + wrap_plaintext(raw_docstring)

        return hoverstring.strip()
