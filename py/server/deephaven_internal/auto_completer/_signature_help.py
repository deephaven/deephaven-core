#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations
from inspect import Parameter
from typing import Any, TypedDict, Union
from docstring_parser import parse, Docstring
from jedi.api.classes import Signature


_IGNORE_PARAM_NAMES = ("", "/", "*")
_POSITIONAL_KINDS = (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD, Parameter.VAR_POSITIONAL)

# key: result from _hash
# value: another dictionary that has the following keys:
#   description: The markdown description (result from _generate_description_markdown)
#   param_docs: A list of param markdown descriptions (result from _generate_param_markdowns)
_result_cache = {}

class ParameterDetails(TypedDict):
    """
    Details of a parameter of a function
    """
    name: str
    """
    Name of the parameter
    """

    description: str
    """
    Description of the parameter
    """

    type: Union[str, None]
    """
    Type of the parameter
    """

    default: Union[str, None]
    """
    Default value of the parameter
    """


def _get_params(signature: Signature, docs: Docstring) -> list[ParameterDetails]:
    """
    Returns all available parameter information from the signature and docstring.

    Combines information from the docstring and signature.
    Uses the type in the signature if it exists, falls back to the type in the docstring if that exists.
    Also includes the default value if it exists in the signature.


    Args:
        signature: The signature from `jedi`
        docs: The parsed docstring from `docstring_parser`

    Returns:
        A list of dictionaries that contain the parameter name, description, type, and default value.
    """

    params = []
    params_info = {}

    # Take information from docs first
    for param in docs.params:
        params_info[param.arg_name.replace("*", "")] = {
            "description": param.description.strip(),
            "type": param.type_name,
        }

    for param in signature.params:
        param_str = param.to_string().strip()

        # Add back * or ** for display purposes only
        if param.kind == Parameter.VAR_POSITIONAL:
            name = f"*{param.name}"
        elif param.kind == Parameter.VAR_KEYWORD:
            name = f"**{param.name}"
        else:
            name = param.name

        # Use type in signature first, then type in docs, then None
        if ":" in param_str:
            type_ = param_str.split(":")[1].split("=")[0].strip()
        elif param.name in params_info:
            type_ = params_info[param.name]["type"]
        else:
            type_ = None
        
        params.append({
            "name": name,
            "description": params_info.get(param.name, {}).get("description"),
            "type": type_,
            "default": param_str.split("=")[1] if "=" in param_str else None,
        })

    return params


def _generate_description_markdown(docs: Docstring, params: list[ParameterDetails]) -> str:
    """
    Generate the description markdown for the signature help. This includes the description, parameters, returns, raises,
    and examples.

    Args:
        docs: The parsed docstring from `docstring_parser`
        params: The list of parameters from `_get_params`

    Returns:
        The markdown description
    """
    if docs.description is None:
        description = ""
    else:
        description = docs.description.strip().replace("\n", "  \n") + "\n\n"

    if len(params) > 0:
        description += "#### **Parameters**\n\n"
        for param in params:
            if param['name'] in _IGNORE_PARAM_NAMES:
                continue

            description += f"> **{param['name']}**"
            if param['type'] is not None:
                description += f": *{param['type']}*"
            if param['default'] is not None:
                description += f" ⋅ (default: *{param['default']}*)"
            description += "  \n"

            if param['description'] is not None:
                description += f"> {param['description']}".replace('\n\n', '\n\n> ')
            description += "\n\n"

    if len(docs.many_returns) > 0:
        description += "#### **Returns**\n\n"
        for return_ in docs.many_returns:
            if return_.type_name is not None:
                description += f"> **{return_.type_name}**  \n"
            if return_.description is not None:
                description += f"> {return_.description}"
            description += "\n\n"

    if len(docs.raises) > 0:
        description += "#### **Raises**\n\n"
        for raises_ in docs.raises:
            if raises_.type_name is not None:
                description += f"> **{raises_.type_name}**  \n"
            if raises_.description is not None:
                description += f"> {raises_.description}"
            description += "\n\n"

    if len(docs.examples) > 0:
        description += "#### **Examples**\n\n"
        for example in docs.examples:
            if example.description is not None and example.description.startswith(">>> "):
                description += f"```\n{example.description}\n```"
            else:
                description += example.description
            description += "\n\n"

    return description.strip()

def _generate_param_markdowns(signature: Signature, params: list[Any]) -> list[Any]:
    """
    Generate markdown for each parameter in the signature. This will be shown on top of the description markdown.

    Args:
        signature: The signature from `jedi`
        params: The list of parameters from `_get_params`

    Returns:
        A list of signature names and description pairs.
    """

    param_docs = []
    for i in range(len(signature.params)):
        if signature.params[i].to_string().strip() in _IGNORE_PARAM_NAMES:
            continue

        param = params[i]
        description = f"##### **{param['name']}**"
        if param['type'] is not None:
            description += f" : *{param['type']}*"
        description += "\n\n"
        if param['description'] is not None:
            description += f"{param['description']}\n\n"
        description += "---"

        param_docs.append([signature.params[i].to_string().strip(), description])

    return param_docs


def get_signature_help(signature: Signature) -> list[Any]:
    """ Gets the result of a signature to be used by `do_signature_help`

    If no docstring information is parsed, then the signature will be displayed in Markdown but with plaintext style
    whitespace. Any cached docstring must have some docstring information.

    Returns:
        A list that contains [signature name, docstring, param docstrings, index]
    """

    docstring = signature.docstring(raw=True)

    # The results are cached based on the signature and docstring
    # Even if there is another method in another package with the same signature,
    # if the docstring also matches it's fine to use the same cache, as the result will be the same.
    cache_key = f"{signature.to_string()}\n{docstring}"

    # Check cache
    if cache_key in _result_cache:
        result = _result_cache[cache_key]
        return [
            signature.to_string(),
            result["description"],
            result["param_docs"],
            signature.index if signature.index is not None else -1,
        ]


    # Parse the docstring to extract information
    docs = parse(docstring)
    # Nothing parsed, revert to plaintext
    # Based on code, the meta attribute seems to be a list of parsed items. Then, the parse function returns the
    #   style with the most amount of items in meta. If there are no items, that should be mean nothing was parsed.
    # https://github.com/rr-/docstring_parser/blob/4951137875e79b438d52a18ac971ec0c28ef269c/docstring_parser/parser.py#L46
    if len(docs.meta) == 0:
        return [
            signature.to_string(),
            # Since signature is a markdown, replace whitespace in a way to preserve how it originally looks
            docstring.replace(" ", "&nbsp;").replace("\n", "  \n"),
            [[param.to_string().strip(), ""] for param in signature.params],
            signature.index if signature.index is not None else -1,
        ]

    # Get params in this scope because it'll be used multiple times
    params = _get_params(signature, docs)
    description = _generate_description_markdown(docs, params)
    param_docs = _generate_param_markdowns(signature, params)
    _result_cache[cache_key] = {
        "description": description,
        "param_docs": param_docs,
    }

    return [
        signature.to_string(),
        description,
        param_docs,
        signature.index if signature.index is not None else -1,
    ]
