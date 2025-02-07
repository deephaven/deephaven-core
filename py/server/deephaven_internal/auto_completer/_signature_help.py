#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations
from inspect import Parameter
from typing import Any
from docstring_parser import parse, Docstring
from jedi.api.classes import Signature


IGNORE_PARAM_NAMES = ("", "/", "*")
MAX_DISPLAY_SIG_LEN = 128  # 3 lines is 150, but there could be overflow so 150 could result in 4 lines
POSITIONAL_KINDS = (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD, Parameter.VAR_POSITIONAL)

# key: result from _hash
# value: another dictionary that has the following keys:
#   description: The markdown description (result from _generate_description_markdown)
#   param_docs: A list of param markdown descriptions (result from _generate_param_markdowns)
result_cache = {}


def _hash(signature: Signature) -> str:
    """A simple way to identify signatures"""
    return f"{signature.to_string()}\n{signature.docstring(raw=True)}"


def _get_params(signature: Signature, docs: Docstring) -> list[Any]:
    """
    Combines all available parameter information from the signature and docstring.

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


def _generate_description_markdown(docs: Docstring, params: list[Any]) -> str:
    if docs.description is None:
        description = ""
    else:
        description = docs.description.strip().replace("\n", "  \n") + "\n\n"

    if len(params) > 0:
        description += "#### **Parameters**\n\n"
        for param in params:
            if param['name'] in IGNORE_PARAM_NAMES:
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


def _generate_display_sig(signature: Signature) -> str:
    """
    Generate the signature text for the signature help. Truncates the signature if it is too long. If the current
    argument is positional, it will display the next 2 arguments. If the current argument is keyword, it will only
    display the current argument.
    """

    if len(signature.to_string()) <= MAX_DISPLAY_SIG_LEN:
        return signature.to_string()
    
    # Use 0 as default to display start of signature
    index = signature.index if signature.index is not None else 0
    display_sig = f"{signature.name}("

    if index > 0:
        display_sig += "..., "

    # If current arg is positional, display next 2 args
    # If current arg is keyword, only display current args
    if signature.params[index].kind in POSITIONAL_KINDS:
        # Clamp index so that 3 args are shown, even at last index
        index = max(min(index, len(signature.params) - 3), 0)
        end_index = index + 3
        # If the next arg is not positional, do not show the one after it
        # Otherwise, this arg will show 2 ahead, and then next arg will show 0 ahead
        if signature.params[index + 1].kind not in POSITIONAL_KINDS:
            end_index -= 1
        display_sig += ", ".join([param.to_string() for param in signature.params[index:end_index]])
        if index + 3 < len(signature.params):
            display_sig += ", ..."
    else:
        display_sig += signature.params[index].to_string()
        if index + 1 < len(signature.params):
            display_sig += ", ..."

    return display_sig + ")"


def _generate_param_markdowns(signature: Signature, params: list[Any]) -> list[Any]:
    """
    Generate markdown for each parameter in the signature. This will be shown on top of the description markdown.
    """

    param_docs = []
    for i in range(len(signature.params)):
        if signature.params[i].to_string().strip() in IGNORE_PARAM_NAMES:
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


def _get_signature_help(signature: Signature) -> list[Any]:
    """ Gets the result of a signature to be used by `do_signature_help`

    If no docstring information is parsed, then the signature will be displayed in Markdown but with plaintext style
    whitespace. Any cached docstring must have some docstring information.

    Returns:
        A list that contains [signature name, docstring, param docstrings, index]
    """

    docstring = signature.docstring(raw=True)
    cache_key = _hash(signature)

    # Check cache
    if cache_key in result_cache:
        result = result_cache[cache_key]
        return [
            _generate_display_sig(signature),
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
            signature.docstring(raw=True).replace(" ", "&nbsp;").replace("\n", "  \n"),
            [[param.to_string().strip(), ""] for param in signature.params],
            signature.index if signature.index is not None else -1,
        ]

    # Get params in this scope because it'll be used multiple times
    params = _get_params(signature, docs)
    description = _generate_description_markdown(docs, params)
    param_docs = _generate_param_markdowns(signature, params)
    result_cache[cache_key] = {
        "description": description,
        "param_docs": param_docs,
    }

    return [
        _generate_display_sig(signature),
        description,
        param_docs,
        signature.index if signature.index is not None else -1,
    ]
