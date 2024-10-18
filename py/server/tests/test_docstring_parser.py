#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
from functools import wraps
from typing import Callable

from docstring_parser import parse, Docstring
from jedi import Script, Interpreter
from jedi.api.classes import Signature

from deephaven_internal.auto_completer._signature_help import _get_params, _generate_description_markdown, _generate_display_sig
from tests.testbase import BaseTestCase

from .data.signatures import *


class DocstringParser(BaseTestCase):

    @staticmethod
    def create_test(name: str, code: str, func: Callable, func_call_append: str = ""):
        def decorator(f):
            @wraps(f)
            def wrapper(self):
                s = Script(f"{code}\n{name}({func_call_append}").get_signatures()
                self.assertIsInstance(s, list)
                self.assertEqual(len(s), 1)
                f(self, s[0], parse(s[0].docstring(raw=True)))
                
                i = Interpreter(f"{name}({func_call_append}", [{name: func}]).get_signatures()
                self.assertIsInstance(s, list)
                self.assertEqual(len(s), 1)
                f(self, i[0], parse(i[0].docstring(raw=True)))

            return wrapper
        return decorator

    @create_test("args", args_str, args)
    def test_args(self, signature: Signature, docs: Docstring):
        self.assertNotEqual(len(docs.meta), 0)
        self.assertEqual(
            _generate_description_markdown(docs, _get_params(signature, docs)),
            args_str_result
        )

    @create_test("args_no_docs", args_no_docs_str, args_no_docs)
    def test_args_no_docs(self, signature: Signature, docs: Docstring):
        self.assertNotEqual(len(docs.meta), 0)
        self.assertEqual(
            _generate_description_markdown(docs, _get_params(signature, docs)),
            args_no_docs_result
        )

    @create_test("raises_various", raises_various_str, raises_various)
    def test_raises_various(self, signature: Signature, docs: Docstring):
        self.assertNotEqual(len(docs.meta), 0)
        self.assertEqual(
            _generate_description_markdown(docs, _get_params(signature, docs)),
            raises_various_result
        )

    @create_test("returns_various", returns_various_str, returns_various)
    def test_returns_various(self, signature: Signature, docs: Docstring):
        self.assertNotEqual(len(docs.meta), 0)
        self.assertEqual(
            _generate_description_markdown(docs, _get_params(signature, docs)),
            returns_various_result
        )

    @create_test("original_signature", original_signature_str, original_signature)
    def test_original_signature(self, signature: Signature, docs: Docstring):
        self.assertNotEqual(len(docs.meta), 0)
        self.assertEqual(
            _generate_display_sig(signature), 
            "original_signature(aaaaaa00, aaaaaa01, aaaaaa02, aaaaaa03, aaaaaa04, aaaaaa05, aaaaaa06, aaaaaa07, aaaaaa08, aaaaaa09)"
        )

    @create_test("truncate_positional", truncate_positional_str, truncate_positional)
    def test_truncate_positional_0(self, signature: Signature, docs: Docstring):
        self.assertNotEqual(len(docs.meta), 0)
        self.assertEqual(_generate_display_sig(signature), "truncate_positional(aaaaaa00, aaaaaa01, aaaaaa02, ...)")

    @create_test("truncate_positional", truncate_positional_str, truncate_positional, "1, ")
    def test_truncate_positional_1(self, signature: Signature, docs: Docstring):
        self.assertNotEqual(len(docs.meta), 0)
        self.assertEqual(_generate_display_sig(signature), "truncate_positional(..., aaaaaa01, aaaaaa02, aaaaaa03, ...)")

    @create_test("truncate_positional", truncate_positional_str, truncate_positional, "1, 1, 1, 1, 1, 1, 1, 1, 1, 1, ")
    def test_truncate_positional_10(self, signature: Signature, docs: Docstring):
        self.assertNotEqual(len(docs.meta), 0)
        self.assertEqual(_generate_display_sig(signature), "truncate_positional(..., aaaaaa10, aaaaaa11, aaaaaa12)")

    @create_test("truncate_positional", truncate_positional_str, truncate_positional, "1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, ")
    def test_truncate_positional_11(self, signature: Signature, docs: Docstring):
        self.assertNotEqual(len(docs.meta), 0)
        self.assertEqual(_generate_display_sig(signature), "truncate_positional(..., aaaaaa10, aaaaaa11, aaaaaa12)")

    @create_test("truncate_positional", truncate_positional_str, truncate_positional, "1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, ")
    def test_truncate_positional_12(self, signature: Signature, docs: Docstring):
        self.assertNotEqual(len(docs.meta), 0)
        self.assertEqual(_generate_display_sig(signature), "truncate_positional(..., aaaaaa10, aaaaaa11, aaaaaa12)")

    @create_test("truncate_keyword", truncate_keyword_str, truncate_keyword)
    def test_truncate_keyword_0(self, signature: Signature, docs: Docstring):
        self.assertNotEqual(len(docs.meta), 0)
        self.assertEqual(_generate_display_sig(signature), "truncate_keyword(aaaaaa00, aaaaaa01=1, ...)")

    @create_test("truncate_keyword", truncate_keyword_str, truncate_keyword, "1, ")
    def test_truncate_keyword_1(self, signature: Signature, docs: Docstring):
        self.assertNotEqual(len(docs.meta), 0)
        self.assertEqual(_generate_display_sig(signature), "truncate_keyword(..., aaaaaa01=1, ...)")

    @create_test("truncate_keyword", truncate_keyword_str, truncate_keyword, "1, aaaaaa12=")
    def test_truncate_keyword_1(self, signature: Signature, docs: Docstring):
        self.assertNotEqual(len(docs.meta), 0)
        self.assertEqual(_generate_display_sig(signature), "truncate_keyword(..., aaaaaa12=1)")
