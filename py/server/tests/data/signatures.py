#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
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

args_str = """
def args(has_docs, has_type: str | int, *positional, has_default=1, has_type_default: str | int = 1, **keyword):
    \"\"\"
    Description

    Args:
        has_docs: Arg has docs
        has_type: Arg has type
        not_real: Arg does not exist in signature
        *positional: Positional arg has docs
        has_default: Arg has default
        has_type_default: Arg has type and default
        **keyword: Keyword arg has docs
    \"\"\"
"""

args_str_result = """\
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
> Keyword arg has docs"""

def args_no_docs(no_docs, /, *, keyword_no_docs=None):
    """
    Description

    Args:
        not_real: Arg does not exist in signature
        /: Should not show
        *: Should not show
    """

args_no_docs_str = """
def args_no_docs(no_docs, /, *, keyword_no_docs=None):
    \"\"\"
    Description

    Args:
        not_real: Arg does not exist in signature
        /: Should not show
        *: Should not show
    \"\"\"
"""

args_no_docs_result = """\
Description

#### **Parameters**

> **no_docs**  


> **keyword_no_docs** ⋅ (default: *None*)"""

def raises_various():
    """
    Description

    Raises:
        Exception: Exception description
        ValueError: ValueError description.
          This is a continuation of ValueError
    """

raises_various_str = """
def raises_various():
    \"\"\"
    Description

    Raises:
        Exception: Exception description
        ValueError: ValueError description.
          This is a continuation of ValueError
    \"\"\"
"""

raises_various_result = """\
Description

#### **Raises**

> **Exception**  
> Exception description

> **ValueError**  
> ValueError description.
This is a continuation of ValueError"""

def returns_various():
    """
    :returns: Return has docs
    :returns foo: foo description
    :returns bar: bar description
    """

returns_various_str = """
def returns_various():
    \"\"\"
    :returns: Return has docs
    :returns foo: foo description
    :returns bar: bar description
    \"\"\"
"""

returns_various_result = """\
#### **Returns**

> Return has docs

> **foo**  
> foo description

> **bar**  
> bar description"""

def example_string():
    """
    Description

    Examples:
        Plain text
    """

example_string_str = """
def example_string():
    \"\"\"
    Description

    Examples:
        Plain text
    \"\"\"
"""

example_string_result = """\
Description

#### **Examples**

Plain text"""

def example_code():
    """
    Description

    Examples:
        >>> Code
        Still code
    """

example_code_str = """
def example_code():
    \"\"\"
    Description

    Examples:
        >>> Code
        Still code
    \"\"\"
"""

example_code_result = """\
Description

#### **Examples**

```
>>> Code
Still code
```"""

def original_signature(aaaaaa00, aaaaaa01, aaaaaa02, aaaaaa03, aaaaaa04, aaaaaa05, aaaaaa06, aaaaaa07, aaaaaa08, aaaaaa09):
    """
    :returns a: b
    """

original_signature_str = """
def original_signature(aaaaaa00, aaaaaa01, aaaaaa02, aaaaaa03, aaaaaa04, aaaaaa05, aaaaaa06, aaaaaa07, aaaaaa08, aaaaaa09):
    \"\"\"
    :returns a: b
    \"\"\"
"""

def truncate_positional(aaaaaa00, aaaaaa01, aaaaaa02, aaaaaa03, aaaaaa04, aaaaaa05, aaaaaa06, aaaaaa07, aaaaaa08, aaaaaa09,
                        aaaaaa10, aaaaaa11, aaaaaa12):
    """
    :returns a: b
    """

truncate_positional_str = """
def truncate_positional(aaaaaa00, aaaaaa01, aaaaaa02, aaaaaa03, aaaaaa04, aaaaaa05, aaaaaa06, aaaaaa07, aaaaaa08, aaaaaa09,
                        aaaaaa10, aaaaaa11, aaaaaa12):
    \"\"\"
    :returns a: b
    \"\"\"
"""

def truncate_keyword(aaaaaa00, *, aaaaaa01=1, aaaaaa02=1, aaaaaa03=1, aaaaaa04=1, aaaaaa05=1, aaaaaa06=1, aaaaaa07=1, aaaaaa08=1, aaaaaa09=1,
                     aaaaaa10=1, aaaaaa11=1, aaaaaa12=1):
    """
    :returns a: b
    """

truncate_keyword_str = """
def truncate_keyword(aaaaaa00, *, aaaaaa01=1, aaaaaa02=1, aaaaaa03=1, aaaaaa04=1, aaaaaa05=1, aaaaaa06=1, aaaaaa07=1, aaaaaa08=1, aaaaaa09=1,
                     aaaaaa10=1, aaaaaa11=1, aaaaaa12=1):
    \"\"\"
    :returns a: b
    \"\"\"
"""
