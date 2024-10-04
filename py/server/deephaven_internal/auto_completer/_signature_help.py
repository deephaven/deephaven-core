#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations
from inspect import Parameter
from typing import Any, List
from docstring_parser import parse, Docstring
from jedi.api.classes import Signature

from pprint import pprint

result_cache = {}


def _hash(signature: Signature) -> str:
    """A simple way to identify signatures"""
    return f"{signature.to_string()}\n{signature.docstring(raw=True)}"


def _generate_param_markdown(param: dict) -> List[Any]:
    description = f"##### **{param['name']}**"
    if param['type'] is not None:
        description += f": *{param['type']}*"
    description += "\n\n"

    if param['default'] is not None:
        description += f"Default: {param['default']}\n\n"

    if param['description'] is not None:
        description += f"{param['description']}\n\n"

    return description + "---"


def _get_params(signature: Signature, docs: Docstring) -> List[Any]:
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


def _get_raises(docs: Docstring) -> List[Any]:
    raises = []
    for raise_ in docs.raises:
        raises.append({
            "name": raise_.type_name,
            "description": raise_.description
        })
    
    return raises


def _get_returns(docs: Docstring) -> List[Any]:
    returns = []
    for return_ in docs.many_returns:
        returns.append({
            "name": return_.type_name,
            "description": return_.description
        })
    
    return returns


def _get_signature_result(signature: Signature) -> List[Any]:
    """ Gets the result of a signature to be used by `do_signature_help`

    Returns:
        A list that contains [signature name, docstring, param docstrings, index]
    """

    docstring = signature.docstring(raw=True)
    cache_key = _hash(signature)

    if cache_key in result_cache:
        result = result_cache[cache_key].copy()  # deep copy not needed since only index is different
        result.append(signature.index if signature.index is not None else -1)
        return result

    docs = parse(docstring)

    # Nothing parsed, revert to plaintext
    if docstring == docs.description:
        return [
            signature.to_string(),
            signature.docstring(raw=True).replace(" ", "&nbsp;").replace("\n", "  \n"),
            [[param.to_string().strip(), ""] for param in signature.params],
            signature.index if signature.index is not None else -1,
        ]

    params = _get_params(signature, docs)
    raises = _get_raises(docs)
    returns = _get_returns(docs)
    description = docs.description.strip().replace("\n", "  \n") + "\n\n"

    if len(params) > 0:
        description += "#### **Parameters**\n\n"
        for param in params:
            description += f"> **{param['name']}**"
            if param['type'] is not None:
                description += f": *{param['type']}*"
            if param['default'] is not None:
                description += f" ⋅ (default: *{param['default']}*)"
            description += "  \n"

            if param['description'] is not None:
                description += f"> {param['description']}"
            description += "\n\n"

    if len(returns) > 0:
        description += "#### **Returns**\n\n"
        for return_ in returns:
            if return_["name"] is not None:
                description += f"> **{return_['name']}**  \n"
            if return_["description"] is not None:
                description += f"> {return_['description']}"
            description += "\n\n"

    if len(raises) > 0:
        description += "#### **Raises**\n\n"
        for raises_ in raises:
            if raises_["name"] is not None:
                description += f"> **{raises_['name']}**  \n"
            if raises_["description"] is not None:
                description += f"> {raises_['description']}"
            description += "\n\n"

    result = [
        f"{signature.to_string().split('(')[0]}(...)" if len(signature.to_string()) > 20 else signature.to_string(),
        description.strip(),
        [[signature.params[i].to_string().strip(), _generate_param_markdown(params[i])] for i in range(len(signature.params))],
    ]
    result_cache[cache_key] = result.copy()  # deep copy not needed since only index is different
    result.append(signature.index if signature.index is not None else -1)

    return result
