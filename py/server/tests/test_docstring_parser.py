#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
import inspect
from typing import Callable

from docstring_parser import parse, Docstring
from jedi import Script, Interpreter

from deephaven_internal.auto_completer._signature_help import _get_params, _generate_description_markdown, _generate_display_sig
from tests.testbase import BaseTestCase


class DocstringParser(BaseTestCase):

    def get_script_signature(self, func: Callable, func_call_append=""):
        """
        Get the signature of the function passed in. using Jedi Script.

        Args:
            func: the function object. Will be used with Jedi Interpreter, and source used for Jedi Script
            func_call_append: the string to append at the end of the function call
        """
        code = inspect.getsource(func)
        s = Script(f"{code}\n{func.__name__}({func_call_append}").get_signatures()
        self.assertIsInstance(s, list)
        self.assertEqual(len(s), 1)
        return s[0]

    def get_interpreter_signature(self, func: Callable, func_call_append=""):
        i = Interpreter(f"{func.__name__}({func_call_append}", [{func.__name__: func}]).get_signatures()
        self.assertIsInstance(i, list)
        self.assertEqual(len(i), 1)
        return i[0]

    def expect_description(self, func: Callable, expected_result: str, func_call_append =""):
        """
        Test whether the function passed in results in the expected markdown docs. Tests both interpreter and script.

        Args:
            func: the function object. Will be used with Jedi Interpreter, and source used for Jedi Script
            expected_result: the expected markdown result
            func_call_append: the string to append at the end of the function call
        """
        script_signature = self.get_script_signature(func, func_call_append)
        script_docstring = script_signature.docstring(raw=True)
        self.assertEqual(
            _generate_description_markdown(parse(script_docstring), _get_params(script_signature, parse(script_docstring))),
            expected_result
        )

        interpreter_signature = self.get_interpreter_signature(func, func_call_append)
        interpreter_docstring = interpreter_signature.docstring(raw=True)
        self.assertEqual(
            # Need to use _generate_display_sig for the original_signature ones, not this method... grr.
            _generate_description_markdown(parse(interpreter_docstring), _get_params(interpreter_signature, parse(interpreter_docstring))),
            expected_result
        )

    def expect_signature(self, func: Callable, expected_result: str, func_call_append = ""):
        """
        Test whether the function passed in results in the expected signature. Tests both interpreter and script.

        Args:
            func: the function object. Will be used with Jedi Interpreter, and source used for Jedi Script
            expected_result: the expected signature result
            func_call_append: the string to append at the end of the function call
        """
        script_signature = self.get_script_signature(func, func_call_append)
        self.assertEqual(_generate_display_sig(script_signature), expected_result)

        interpreter_signature = self.get_interpreter_signature(func, func_call_append)
        self.assertEqual(_generate_display_sig(interpreter_signature), expected_result)

    def test_args(self):
        def args(has_docs, has_type: str | int, *positional, has_default=1, has_type_default: str | int = 1, **keyword):
            """
            Description

            Args:
                has_docs: Arg has docs
                has_type: Arg has type
                not_real: Arg does not exist in signature
                *positional: Positional arg has docs
                has_default: Arg has default
                has_type_default: Arg has type and default
                **keyword: Keyword arg has docs
            """

        self.expect_description(args, """\
Description

#### **Parameters**

> **has_docs**  
> Arg has docs

> **has_type**: *str | int*  
> Arg has type

> ***positional**  
> Positional arg has docs

> **has_default** ⋅ (default: *1*)  
> Arg has default

> **has_type_default**: *str | int* ⋅ (default: *1*)  
> Arg has type and default

> ****keyword**  
> Keyword arg has docs""")

    def test_args_no_docs(self):
        def args_no_docs(no_docs, /, *, keyword_no_docs=None):
            """
            Description

            Args:
                not_real: Arg does not exist in signature
                /: Should not show
                *: Should not show
            """

        self.expect_description(args_no_docs, """\
Description

#### **Parameters**

> **no_docs**  


> **keyword_no_docs** ⋅ (default: *None*)""")

    def test_raises_various(self):
        def raises_various():
            """
            Description

            Raises:
                Exception: Exception description
                ValueError: ValueError description.
                  This is a continuation of ValueError
            """

        self.expect_description(raises_various, """\
Description

#### **Raises**

> **Exception**  
> Exception description

> **ValueError**  
> ValueError description.
This is a continuation of ValueError""")

    def test_returns_various(self):
        def returns_various():
            """
            :returns: Return has docs
            :returns foo: foo description
            :returns bar: bar description
            """

        self.expect_description(returns_various, """\
#### **Returns**

> Return has docs

> **foo**  
> foo description

> **bar**  
> bar description""")

    def test_example_string(self):
        def example_string():
            """
            Description

            Examples:
                Plain text
            """

        self.expect_description(example_string, """\
Description

#### **Examples**

Plain text""")

    def test_example_code(self):
        def example_code():
            """
            Description

            Examples:
                >>> Code
                Still code
            """

        self.expect_description(example_code, """\
Description

#### **Examples**

```
>>> Code
Still code
```""")

    def test_original_signature(self):
        def original_signature(aaaaaa00, aaaaaa01, aaaaaa02, aaaaaa03, aaaaaa04, aaaaaa05, aaaaaa06, aaaaaa07, aaaaaa08, aaaaaa09):
            """
            :returns a: b
            """

        self.expect_signature(original_signature, "original_signature(aaaaaa00, aaaaaa01, aaaaaa02, aaaaaa03, aaaaaa04, aaaaaa05, aaaaaa06, aaaaaa07, aaaaaa08, aaaaaa09)")

    def test_truncate_positional(self):
        def truncate_positional(aaaaaa00, aaaaaa01, aaaaaa02, aaaaaa03, aaaaaa04, aaaaaa05, aaaaaa06, aaaaaa07, aaaaaa08, aaaaaa09,
                                aaaaaa10, aaaaaa11, aaaaaa12):
            """
            :returns a: b
            """

        self.expect_signature(truncate_positional, "truncate_positional(aaaaaa00, aaaaaa01, aaaaaa02, ...)")
        self.expect_signature(truncate_positional, "truncate_positional(..., aaaaaa01, aaaaaa02, aaaaaa03, ...)", "1, ")
        self.expect_signature(truncate_positional, "truncate_positional(..., aaaaaa10, aaaaaa11, aaaaaa12)", "1, 1, 1, 1, 1, 1, 1, 1, 1, 1, ")
        self.expect_signature(truncate_positional, "truncate_positional(..., aaaaaa10, aaaaaa11, aaaaaa12)", "1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, ")

    def test_truncate_keyword(self):
        def truncate_keyword(aaaaaa00, *, aaaaaa01=1, aaaaaa02=1, aaaaaa03=1, aaaaaa04=1, aaaaaa05=1, aaaaaa06=1, aaaaaa07=1, aaaaaa08=1, aaaaaa09=1,
                             aaaaaa10=1, aaaaaa11=1, aaaaaa12=1):
            """
            :returns a: b
            """

        self.expect_signature(truncate_keyword, "truncate_keyword(aaaaaa00, aaaaaa01=1, ...)")
        self.expect_signature(truncate_keyword, "truncate_keyword(..., aaaaaa01=1, ...)", "1, ")
        self.expect_signature(truncate_keyword, "truncate_keyword(..., aaaaaa12=1)", "1, aaaaaa12=")