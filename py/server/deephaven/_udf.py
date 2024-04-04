#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import inspect
import re
import sys
import typing
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from functools import wraps, partial
from typing import Callable, List, Any, Union, _GenericAlias, Optional, Sequence

import pandas as pd

from deephaven._dep import soft_dependency

numba = soft_dependency("numba")

import numpy
import numpy as np

from deephaven import DHError, dtypes
from deephaven.dtypes import _NUMPY_INT_TYPE_CODES, _NUMPY_FLOATING_TYPE_CODES, _PRIMITIVE_DTYPE_NULL_MAP, \
    _BUILDABLE_ARRAY_DTYPE_MAP, DType
from deephaven.jcompat import _j_array_to_numpy_array
from deephaven.time import to_np_datetime64

# For unittest vectorization
test_vectorization = False
vectorized_count = 0

_SUPPORTED_NP_TYPE_CODES = {"b", "h", "H", "i", "l", "f", "d", "?", "U", "M", "O"}


@dataclass
class _ParsedParam:
    name: Union[str, int] = field(init=True)
    orig_types: List[type] = field(default_factory=list)
    effective_types: List[type] = field(default_factory=list)
    encoded_types: List[str] = field(default_factory=list)
    none_allowed: bool = False
    has_array: bool = False
    arg_converter: Optional[Callable] = None

    def setup_arg_converter(self, arg_type_str: str) -> None:
        """ Set up the converter function for the parameter based on the encoded argument type string. """
        for param_type_str, effective_type in zip(self.encoded_types, self.effective_types):
            if param_type_str == arg_type_str:
                if arg_type_str.startswith("["):
                    dtype = dtypes.from_np_dtype(np.dtype(arg_type_str[1]))
                    self.arg_converter = partial(_j_array_to_numpy_array, dtype, conv_null=False,
                                                 type_promotion=False)
                else:
                    if effective_type in {object, str}:
                        self.arg_converter = None
                    elif effective_type == np.datetime64:
                        self.arg_converter = to_np_datetime64
                    else:
                        self.arg_converter = effective_type
                        if "N" in self.encoded_types:
                            if null_value := _PRIMITIVE_DTYPE_NULL_MAP.get(
                                    dtypes.from_np_dtype(np.dtype(param_type_str))):
                                self.arg_converter = partial(lambda nv, x: None if x == nv else effective_type(x),
                                                             null_value)
                        if self.arg_converter in {int, float, bool}:  # JPY does the conversion for these types
                            self.arg_converter = None

                        if isinstance(self.arg_converter, type) and issubclass(self.arg_converter, np.generic):
                            warnings.warn(
                                f"numpy scalar type {self.arg_converter} is used to annotate parameter '{self.name}'. Note that conversion of "
                                f"arguments to numpy scalar types is significantly slower than to Python built-in scalar "
                                f"types such as int, float, bool, etc. If possible, consider using Python built-in scalar types "
                                f"instead.")

                break
        else:  # if the loop didn't break, it means that the parameter is either not type hinted or has a generic object type
            self.arg_converter = None


@dataclass
class _ParsedReturnAnnotation:
    orig_type: type = None
    encoded_type: str = None
    none_allowed: bool = False
    has_array: bool = False
    ret_converter: Optional[Callable] = None


@dataclass
class _ParsedSignature:
    fn: Callable = None
    params: List[_ParsedParam] = field(default_factory=list)
    ret_annotation: _ParsedReturnAnnotation = None

    @property
    def encoded(self) -> str:
        """Encode the signature of a Python function by mapping the annotations of the parameter types and the return
        type to numpy dtype chars (i,l,h,f,d,b,?,U,M,O) and '[' for array, 'N' for NoneType. and pack them into a
        string with parameter type chars first, in their original order, followed by the delimiter string '->',
        then the return type char. If a parameter or the return of the function is not annotated,
        the default 'O' - object type, will be used.
        """
        param_str = ",".join([str(p.name) + ":" + "".join(p.encoded_types) for p in self.params])
        # ret_annotation has only one parsed annotation, and it might be Optional which means it contains 'N' in the
        # encoded type. We need to remove it.
        return_type_code = re.sub(r"[N]", "", self.ret_annotation.encoded_type)
        return param_str + "->" + return_type_code

    def prepare_auto_arg_conv(self, encoded_arg_types: str) -> bool:
        """ Determine whether the auto argument conversion should be used and set the converter functions for the
        parameters."""
        if not self.params or not encoded_arg_types:
            return False

        arg_conv_needed = True
        arg_type_strs = encoded_arg_types.split(",")
        if all([t == "O" for t in arg_type_strs]):
            arg_conv_needed = False

        for arg_type_str, param in zip(arg_type_strs, self.params):
            param.setup_arg_converter(arg_type_str)

        if all([param.arg_converter is None for param in self.params]):
            arg_conv_needed = False

        return arg_conv_needed

    def setup_return_converter(self) -> None:
        """ Set the converter function for the return value of UDF based on the return type annotation."""
        pra = self.ret_annotation
        t = pra.encoded_type
        if t == 'H':
            if pra.none_allowed:
                pra.ret_converter = lambda x: dtypes.Character(int(x)) if x is not None else None
            else:
                pra.ret_converter = lambda x: dtypes.Character(int(x))
        elif t in _NUMPY_INT_TYPE_CODES:
            if pra.none_allowed:
                null_value = _PRIMITIVE_DTYPE_NULL_MAP.get(dtypes.from_np_dtype(np.dtype(t)))
                pra.ret_converter = partial(lambda nv, x: nv if x is None else int(x), null_value)
            else:
                pra.ret_converter = int
        elif t in _NUMPY_FLOATING_TYPE_CODES:
            if pra.none_allowed:
                null_value = _PRIMITIVE_DTYPE_NULL_MAP.get(dtypes.from_np_dtype(np.dtype(t)))
                pra.ret_converter = partial(lambda nv, x: nv if x is None else float(x), null_value)
            else:
                pra.ret_converter = float
        elif t == '?':
            if pra.none_allowed:
                pra.ret_converter = lambda x: bool(x) if x is not None else None
            else:
                pra.ret_converter = bool
        elif t == 'U':
            pra.ret_converter = lambda x: x if x is None else str(x)
        elif t == 'M':
            from deephaven.time import to_j_instant
            pra.ret_converter = to_j_instant
        else:
            pra.ret_converter = None


def _encode_param_type(t: type) -> str:
    """Returns the numpy based char codes for the given type.
    If the type is a numpy ndarray, prefix the numpy dtype char with '[' using Java convention
    If the type is a NoneType (as in Optional or as None in Union), return 'N'
    """
    if t is type(None):
        return "N"

    # find the component type if it is numpy ndarray
    component_type = _component_np_dtype_char(t)
    if component_type:
        t = component_type

    tc = _np_dtype_char(t)
    tc = tc if tc in _SUPPORTED_NP_TYPE_CODES else "O"

    if component_type:
        tc = "[" + tc
    return tc


def _np_dtype_char(t: Union[type, str]) -> str:
    """Returns the numpy dtype character code for the given type."""
    try:
        np_dtype = np.dtype(t if t else "object")
        if np_dtype.kind == "O":
            if t in (datetime, pd.Timestamp):
                return "M"
    except TypeError:
        np_dtype = np.dtype("object")

    return np_dtype.char


def _component_np_dtype_char(t: type) -> Optional[str]:
    """Returns the numpy dtype character code for the given type's component type if the type is a Sequence type or
    numpy ndarray, otherwise return None. """
    component_type = None

    if sys.version_info > (3, 8):
        import types
        if isinstance(t, types.GenericAlias) and issubclass(t.__origin__, Sequence): # novermin
            component_type = t.__args__[0]

    if not component_type:
        if isinstance(t, _GenericAlias) and issubclass(t.__origin__, Sequence):
            component_type = t.__args__[0]
            # if the component type is a DType, get its numpy type
            if isinstance(component_type, DType):
                component_type = component_type.np_type

    if not component_type:
        if t == bytes or t == bytearray:
                return "b"

    if not component_type:
        component_type = _np_ndarray_component_type(t)

    if component_type:
        return _np_dtype_char(component_type)
    else:
        return None


def _np_ndarray_component_type(t: type) -> Optional[type]:
    """Returns the numpy ndarray component type if the type is a numpy ndarray, otherwise return None."""

    # Py3.8: npt.NDArray can be used in Py 3.8 as a generic alias, but a specific alias (e.g. npt.NDArray[np.int64])
    # is an instance of a private class of np, yet we don't have a choice but to use it. And when npt.NDArray is used,
    # the 1st argument is typing.Any, the 2nd argument is another generic alias of which the 1st argument is the
    # component type
    component_type = None
    if (3, 9) > sys.version_info >= (3, 8):
        if isinstance(t, np._typing._generic_alias._GenericAlias) and t.__origin__ == np.ndarray:
            component_type = t.__args__[1].__args__[0]
    # Py3.9+, np.ndarray as a generic alias is only supported in Python 3.9+, also npt.NDArray is still available but a
    # specific alias (e.g. npt.NDArray[np.int64]) now is an instance of typing.GenericAlias.
    # when npt.NDArray is used, the 1st argument is typing.Any, the 2nd argument is another generic alias of which
    # the 1st argument is the component type
    # when np.ndarray is used, the 1st argument is the component type
    if not component_type and sys.version_info >= (3, 9):
        import types
        if isinstance(t, types.GenericAlias) and t.__origin__ == np.ndarray: # novermin
            nargs = len(t.__args__)
            if nargs == 1:
                component_type = t.__args__[0]
            elif nargs == 2:  # for npt.NDArray[np.int64], etc.
                a0 = t.__args__[0]
                a1 = t.__args__[1]
                if a0 == typing.Any and isinstance(a1, types.GenericAlias): # novermin
                    component_type = a1.__args__[0]
    return component_type


def _is_union_type(t: type) -> bool:
    """Return True if the type is a Union type"""
    if sys.version_info.major == 3 and sys.version_info.minor >= 10:
        import types
        if isinstance(t, types.UnionType): # novermin
            return True

    return isinstance(t, _GenericAlias) and t.__origin__ == Union


def _parse_param(name: str, annotation: Union[type, dtypes.DType]) -> _ParsedParam:
    """ Parse a parameter annotation in a function's signature """
    p_param = _ParsedParam(name)

    if annotation is inspect._empty:
        p_param.effective_types.append(object)
        p_param.encoded_types.append("O")
        p_param.none_allowed = True
    elif _is_union_type(annotation):
        for t in annotation.__args__:
            _parse_type_no_nested(annotation, p_param, t)
    else:
        _parse_type_no_nested(annotation, p_param, annotation)
    return p_param


def _parse_type_no_nested(annotation: Any, p_param: _ParsedParam, t: Union[type, dtypes.DType]) -> None:
    """ Parse a specific type (top level or nested in a top-level Union annotation) without handling nested types
    (e.g. a nested Union). The result is stored in the given _ParsedAnnotation object.
    """
    p_param.orig_types.append(t)

    # if the annotation is a DH DType instance, we'll use its numpy type
    if isinstance(t, dtypes.DType):
        t = t.np_type
        p_param.effective_types.append(np.dtype(t).type)
    else:
        p_param.effective_types.append(t)

    tc = _encode_param_type(t)
    if "[" in tc:
        p_param.has_array = True
    if tc in {"N", "O"}:
        p_param.none_allowed = True
    p_param.encoded_types.append(tc)


def _parse_return_annotation(annotation: Any) -> _ParsedReturnAnnotation:
    """ Parse a function's return annotation

    The return annotation is treated differently from the parameter annotations. We don't apply the same check and are
    only interested in getting the array-like type right. Any nonsensical annotation will be treated as object type.
    This definitely can be improved in the future.
    """

    pra = _ParsedReturnAnnotation()

    t = annotation
    pra.orig_type = t
    if _is_union_type(annotation) and len(annotation.__args__) == 2:
        # if the annotation is a Union of two types, we'll use the non-None type
        if annotation.__args__[1] == type(None):  # noqa: E721
            pra.none_allowed = True
            t = annotation.__args__[0]
        elif annotation.__args__[0] == type(None):  # noqa: E721
            pra.none_allowed = True
            t = annotation.__args__[1]

    # if the annotation is a DH DType instance, we'll use its numpy type
    if isinstance(t, dtypes.DType):
        t = t.np_type

    component_char = _component_np_dtype_char(t)
    if component_char:
        pra.encoded_type = "[" + component_char
        pra.has_array = True
    else:
        pra.encoded_type = _np_dtype_char(t)

    return pra

if numba:
    def _parse_numba_signature(
            fn: Union[numba.np.ufunc.gufunc.GUFunc, numba.np.ufunc.dufunc.DUFunc]) -> _ParsedSignature:
        """ Parse a numba function's signature"""
        sigs = fn.types  # in the format of ll->l, ff->f,dd->d,OO->O, etc.
        if sigs:
            p_sig = _ParsedSignature(fn)

            # for now, we only support one signature for a numba function because the query engine is not ready to handle
            # multiple signatures for vectorization https://github.com/deephaven/deephaven-core/issues/4762
            sig = sigs[0]
            params, rt_char = sig.split("->")

            p_sig.params = []
            p_sig.ret_annotation = _ParsedReturnAnnotation()
            p_sig.ret_annotation.encoded_type = rt_char

            if isinstance(fn, numba.np.ufunc.dufunc.DUFunc):
                for i, p in enumerate(params):
                    pa = _ParsedParam(i + 1)
                    pa.encoded_types.append(p)
                    pa.effective_types.append(np.dtype(p).type)
                    p_sig.params.append(pa)
            else:  # GUFunc
                # An example: @guvectorize([(int64[:], int64[:], int64[:])], "(m),(n)->(n)"
                input_output_decl = fn.signature  # "(m),(n)->(n)" in the above example
                input_decl, output_decl = input_output_decl.split("->")
                # remove the parentheses so that empty string indicates no array, non-empty string indicates array
                input_decl = re.sub("[()]", "", input_decl).split(",")
                output_decl = re.sub("[()]", "", output_decl)

                for i, (p, d) in enumerate(zip(params, input_decl)):
                    pa = _ParsedParam(i + 1)
                    if d:
                        pa.encoded_types.append("[" + p)
                        pa.effective_types.append(np.dtype(p).type)
                        pa.has_array = True
                    else:
                        pa.encoded_types.append(p)
                        pa.effective_types.append(np.dtype(p).type)
                    p_sig.params.append(pa)

                if output_decl:
                    p_sig.ret_annotation.has_array = True
            return p_sig
        else:
            raise DHError(message=f"numba decorated functions must have an explicitly defined signature: {fn}")


def _parse_np_ufunc_signature(fn: numpy.ufunc) -> _ParsedSignature:
    """ Parse the signature of a numpy ufunc """

    # numpy ufuncs actually have signature encoded in their 'types' attribute, we want to better support
    # them in the future (https://github.com/deephaven/deephaven-core/issues/4762)
    p_sig = _ParsedSignature(fn)
    if fn.nin > 0:
        for i in range(fn.nin):
            pa = _ParsedParam(i + 1)
            pa.encoded_types.append("O")
            pa.effective_types.append(object)
            p_sig.params.append(pa)
    p_sig.ret_annotation = _ParsedReturnAnnotation()
    p_sig.ret_annotation.encoded_type = "O"

    return p_sig


def _parse_signature(fn: Callable) -> _ParsedSignature:
    """ Parse the signature of a function """

    if numba:
        if isinstance(fn, (numba.np.ufunc.gufunc.GUFunc, numba.np.ufunc.dufunc.DUFunc)):
            p_sig = _parse_numba_signature(fn)
            p_sig.setup_return_converter()
            return p_sig

    if isinstance(fn, numpy.ufunc):
        p_sig = _parse_np_ufunc_signature(fn)
    else:
        p_sig = _ParsedSignature(fn=fn)
        if sys.version_info.major == 3 and sys.version_info.minor >= 10:
            sig = inspect.signature(fn, eval_str=True) # novermin
        else:
            sig = inspect.signature(fn)

        for n, p in sig.parameters.items():
            # when from __future__ import annotations is used, the annotation is a string, we need to eval it to get the type
            # when the minimum Python version is bumped to 3.10, we'll always use eval_str in _parse_signature, so that
            # annotation is already a type, and we can skip this step.
            t = eval(p.annotation, fn.__globals__) if isinstance(p.annotation, str) else p.annotation
            p_sig.params.append(_parse_param(n, t))

        t = eval(sig.return_annotation, fn.__globals__) if isinstance(sig.return_annotation, str) else sig.return_annotation
        p_sig.ret_annotation = _parse_return_annotation(t)

    p_sig.setup_return_converter()
    return p_sig


def _udf_parser(fn: Callable):
    """A decorator that acts as a transparent translator for Python UDFs used in Deephaven query formulas between
    Python and Java. This decorator is intended for internal use by the Deephaven query engine and should not be used by
    users.

    It carries out two conversions:
    1. convert Python function return values to Java values.
        For properly annotated functions, including numba vectorized and guvectorized ones, this decorator inspects the
        signature of the function and determines its return type, including supported primitive types and arrays of
        the supported primitive types. It then converts the return value of the function to the corresponding Java value
        of the same type. For unsupported types, the decorator returns the original Python value which appears as
        org.jpy.PyObject in Java.
    2. convert Java function arguments to Python values based on the signature of the function.
    """
    if hasattr(fn, "return_type"):
        return fn
    p_sig = _parse_signature(fn)
    # build a signature string for vectorization by removing NoneType, array char '[', and comma from the encoded types
    # since vectorization only supports UDFs with a single signature and enforces an exact match, any non-compliant
    # signature (e.g. Union with more than 1 non-NoneType) will be rejected by the vectorizer.
    return_array = p_sig.ret_annotation.has_array
    ret_dtype = dtypes.from_np_dtype(np.dtype(p_sig.ret_annotation.encoded_type[-1]))

    @wraps(fn)
    def _udf_decorator(encoded_arg_types: str, for_vectorization: bool=False):
        arg_conv_needed = p_sig.prepare_auto_arg_conv(encoded_arg_types)

        if not for_vectorization:
            if not arg_conv_needed and p_sig.ret_annotation.encoded_type == "O":
                return fn

            def _wrapper(*args, **kwargs):
                if arg_conv_needed:
                    converted_args = [param.arg_converter(arg) if param.arg_converter else arg
                                      for param, arg in zip(p_sig.params, args)]

                    # if the number of arguments is more than the number of parameters, treat the last parameter as a
                    # vararg and use its arg_converter to convert the rest of the arguments
                    if len(args) > len(p_sig.params):
                        arg_converter = p_sig.params[-1].arg_converter
                        converted_args.extend([arg_converter(arg) if arg_converter else arg
                                               for arg in args[len(converted_args):]])
                else:
                    converted_args = args
                # kwargs are not converted because they are not used in the UDFs
                ret = fn(*converted_args, **kwargs)
                if return_array:
                    return dtypes.array(ret_dtype, ret)
                else:
                    return p_sig.ret_annotation.ret_converter(ret) if p_sig.ret_annotation.ret_converter else ret
            return _wrapper
        else: # for vectorization
            def _vectorization_wrapper(*args):
                if len(args) != len(p_sig.params) + 2:
                    raise ValueError(
                        f"The number of arguments doesn't match the function signature. {len(args) - 2}, {p_sig.encoded}")
                if args[0] <= 0:
                    raise ValueError(f"The chunk size argument must be a positive integer. {args[0]}")

                chunk_size = args[0]
                chunk_result = args[1]
                if args[2:]:
                    vectorized_args = zip(*args[2:])
                    for i in range(chunk_size):
                        scalar_args = next(vectorized_args)
                        if arg_conv_needed:
                            converted_args = [param.arg_converter(arg) if param.arg_converter else arg
                                              for param, arg in zip(p_sig.params, scalar_args)]

                            # if the number of arguments is more than the number of parameters, treat the last parameter
                            # as a vararg and use its arg_converter to convert the rest of the arguments
                            if len(args) > len(p_sig.params):
                                arg_converter = p_sig.params[-1].arg_converter
                                converted_args.extend([arg_converter(arg) if arg_converter else arg
                                                       for arg in scalar_args[len(converted_args):]])
                        else:
                            converted_args = scalar_args

                        ret = fn(*converted_args)
                        if return_array:
                            chunk_result[i] = dtypes.array(ret_dtype, ret)
                        else:
                            chunk_result[i] = p_sig.ret_annotation.ret_converter(ret) if p_sig.ret_annotation.ret_converter else ret
                else:
                    for i in range(chunk_size):
                        ret = fn()
                        if return_array:
                            chunk_result[i] = dtypes.array(ret_dtype, ret)
                        else:
                            chunk_result[i] = p_sig.ret_annotation.ret_converter(ret) if p_sig.ret_annotation.ret_converter else ret
                return chunk_result

            if test_vectorization:
                global vectorized_count
                vectorized_count += 1
            return _vectorization_wrapper


    _udf_decorator.j_name = ret_dtype.j_name
    real_ret_dtype = _BUILDABLE_ARRAY_DTYPE_MAP.get(ret_dtype, dtypes.PyObject) if return_array else ret_dtype

    if hasattr(ret_dtype.j_type, 'jclass'):
        j_class = real_ret_dtype.j_type.jclass
    else:
        j_class = real_ret_dtype.qst_type.clazz()

    _udf_decorator.return_type = j_class
    _udf_decorator.signature = p_sig.encoded

    return _udf_decorator
