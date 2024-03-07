#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

import inspect
import re
import sys
from dataclasses import dataclass, field
from functools import wraps
from typing import Callable, List, Any, Union, Tuple, _GenericAlias, Set

from deephaven._dep import soft_dependency

numba = soft_dependency("numba")

import numpy
import numpy as np

from deephaven import DHError, dtypes
from deephaven.dtypes import _np_ndarray_component_type, _np_dtype_char, _NUMPY_INT_TYPE_CODES, \
    _NUMPY_FLOATING_TYPE_CODES, _component_np_dtype_char, _J_ARRAY_NP_TYPE_MAP, _PRIMITIVE_DTYPE_NULL_MAP, _scalar, \
    _BUILDABLE_ARRAY_DTYPE_MAP
from deephaven.jcompat import _j_array_to_numpy_array
from deephaven.time import to_np_datetime64

# For unittest vectorization
test_vectorization = False
vectorized_count = 0


_SUPPORTED_NP_TYPE_CODES = {"b", "h", "H", "i", "l", "f", "d", "?", "U", "M", "O"}


@dataclass
class _ParsedParam:
    name: Union[str, int] = field(init=True)
    orig_types: Set[type] = field(default_factory=set)
    encoded_types: Set[str] = field(default_factory=set)
    none_allowed: bool = False
    has_array: bool = False
    int_char: str = None
    floating_char: str = None


@dataclass
class _ParsedReturnAnnotation:
    orig_type: type = None
    encoded_type: str = None
    none_allowed: bool = False
    has_array: bool = False


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
        param_str = ",".join(["".join(p.encoded_types) for p in self.params])
        # ret_annotation has only one parsed annotation, and it might be Optional which means it contains 'N' in the
        # encoded type. We need to remove it.
        return_type_code = re.sub(r"[N]", "", self.ret_annotation.encoded_type)
        return param_str + "->" + return_type_code


def _encode_param_type(t: type) -> str:
    """Returns the numpy based char codes for the given type.
    If the type is a numpy ndarray, prefix the numpy dtype char with '[' using Java convention
    If the type is a NoneType (as in Optional or as None in Union), return 'N'
    """
    if t is type(None):
        return "N"

    # find the component type if it is numpy ndarray
    component_type = _np_ndarray_component_type(t)
    if component_type:
        t = component_type

    tc = _np_dtype_char(t)
    tc = tc if tc in _SUPPORTED_NP_TYPE_CODES else "O"

    if component_type:
        tc = "[" + tc
    return tc


def _parse_param(name: str, annotation: Any) -> _ParsedParam:
    """ Parse a parameter annotation in a function's signature """
    p_param = _ParsedParam(name)

    if annotation is inspect._empty:
        p_param.encoded_types.add("O")
        p_param.none_allowed = True
    elif isinstance(annotation, _GenericAlias) and annotation.__origin__ == Union:
        for t in annotation.__args__:
            _parse_type_no_nested(annotation, p_param, t)
    else:
        _parse_type_no_nested(annotation, p_param, annotation)
    return p_param


def _parse_type_no_nested(annotation: Any, p_param: _ParsedParam, t: Union[type, str]) -> None:
    """ Parse a specific type (top level or nested in a top-level Union annotation) without handling nested types
    (e.g. a nested Union). The result is stored in the given _ParsedAnnotation object.
    """
    p_param.orig_types.add(t)

    # if the annotation is a DH DType instance, we'll use its numpy type
    if isinstance(t, dtypes.DType):
        t = t.np_type

    tc = _encode_param_type(t)
    if "[" in tc:
        p_param.has_array = True
    if tc in {"N", "O"}:
        p_param.none_allowed = True
    if tc in _NUMPY_INT_TYPE_CODES:
        if p_param.int_char and p_param.int_char != tc:
            raise DHError(message=f"multiple integer types in annotation: {annotation}, "
                                  f"types: {p_param.int_char}, {tc}. this is not supported because it is not "
                                  f"clear which Deephaven null value to use when checking for nulls in the argument")
        p_param.int_char = tc
    if tc in _NUMPY_FLOATING_TYPE_CODES:
        if p_param.floating_char and p_param.floating_char != tc:
            raise DHError(message=f"multiple floating types in annotation: {annotation}, "
                                  f"types: {p_param.floating_char}, {tc}. this is not supported because it is not "
                                  f"clear which Deephaven null value to use when checking for nulls in the argument")
        p_param.floating_char = tc
    p_param.encoded_types.add(tc)


def _parse_return_annotation(annotation: Any) -> _ParsedReturnAnnotation:
    """ Parse a function's return annotation

    The return annotation is treated differently from the parameter annotations. We don't apply the same check and are
    only interested in getting the array-like type right. Any nonsensical annotation will be treated as object type.
    This definitely can be improved in the future.
    """

    pra = _ParsedReturnAnnotation()

    t = annotation
    pra.orig_type = t
    if isinstance(annotation, _GenericAlias) and annotation.__origin__ == Union and len(annotation.__args__) == 2:
        # if the annotation is a Union of two types, we'll use the non-None type
        if annotation.__args__[1] == type(None):  # noqa: E721
            t = annotation.__args__[0]
        elif annotation.__args__[0] == type(None):  # noqa: E721
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
    def _parse_numba_signature(fn: Union[numba.np.ufunc.gufunc.GUFunc, numba.np.ufunc.dufunc.DUFunc]) -> _ParsedSignature:
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
                    pa.encoded_types.add(p)
                    if p in _NUMPY_INT_TYPE_CODES:
                        pa.int_char = p
                    if p in _NUMPY_FLOATING_TYPE_CODES:
                        pa.floating_char = p
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
                        pa.encoded_types.add("[" + p)
                        pa.has_array = True
                    else:
                        pa.encoded_types.add(p)
                        if p in _NUMPY_INT_TYPE_CODES:
                            pa.int_char = p
                        if p in _NUMPY_FLOATING_TYPE_CODES:
                            pa.floating_char = p
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
            pa.encoded_types.add("O")
            p_sig.params.append(pa)
    p_sig.ret_annotation = _ParsedReturnAnnotation()
    p_sig.ret_annotation.encoded_type = "O"
    return p_sig


def _parse_signature(fn: Callable) -> _ParsedSignature:
    """ Parse the signature of a function """

    if numba:
        if isinstance(fn, (numba.np.ufunc.gufunc.GUFunc, numba.np.ufunc.dufunc.DUFunc)):
            return _parse_numba_signature(fn)

    if isinstance(fn, numpy.ufunc):
        return _parse_np_ufunc_signature(fn)
    else:
        p_sig = _ParsedSignature(fn=fn)
        if sys.version_info.major == 3 and sys.version_info.minor >= 10:
            sig = inspect.signature(fn, eval_str=True)
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
        return p_sig


def _is_from_np_type(param_types: Set[type], np_type_char: str) -> bool:
    """ Determine if the given numpy type char comes for a numpy type in the given set of parameter type annotations"""
    for t in param_types:
        if issubclass(t, np.generic) and np.dtype(t).char == np_type_char:
            return True
    return False


def _convert_arg(param: _ParsedParam, arg: Any) -> Any:
    """ Convert a single argument to the type specified by the annotation """
    if arg is None:
        if not param.none_allowed:
            raise TypeError(f"Argument {param.name!r}: {arg} is not compatible with annotation {param.orig_types}")
        else:
            return None

    # if the arg is a Java array
    if np_dtype := _J_ARRAY_NP_TYPE_MAP.get(type(arg)):
        encoded_type = "[" + np_dtype.char
        # if it matches one of the encoded types, convert it
        if encoded_type in param.encoded_types:
            dtype = dtypes.from_np_dtype(np_dtype)
            try:
                return _j_array_to_numpy_array(dtype, arg, conv_null=True, type_promotion=False)
            except Exception as e:
                raise TypeError(f"Argument {param.name!r}: {arg} is not compatible with annotation"
                                f" {param.encoded_types}"
                                f"\n{str(e)}") from e
        # if the annotation is missing, or it is a generic object type, return the arg
        elif "O" in param.encoded_types:
            return arg
        else:
            raise TypeError(f"Argument {param.name!r}: {arg} is not compatible with annotation {param.encoded_types}")
    else:  # if the arg is not a Java array
        specific_types = param.encoded_types - {"N", "O"}  # remove NoneType and object type
        if specific_types:
            for t in specific_types:
                if t.startswith("["):
                    if isinstance(arg, np.ndarray) and arg.dtype.char == t[1]:
                        return arg
                    continue

                dtype = dtypes.from_np_dtype(np.dtype(t))
                dh_null = _PRIMITIVE_DTYPE_NULL_MAP.get(dtype)

                if param.int_char and isinstance(arg, int):
                    if arg == dh_null:
                        if param.none_allowed:
                            return None
                        else:
                            raise DHError(f"Argument {param.name!r}: {arg} is not compatible with annotation"
                                          f" {param.orig_types}")
                    else:
                        # return a numpy integer instance only if the annotation is a numpy type
                        if _is_from_np_type(param.orig_types, param.int_char):
                            return np.dtype(param.int_char).type(arg)
                        else:
                            return arg
                elif param.floating_char and isinstance(arg, float):
                    if isinstance(arg, float):
                        if arg == dh_null:
                            return np.nan if "N" not in param.encoded_types else None
                        else:
                            # return a numpy floating instance only if the annotation is a numpy type
                            if _is_from_np_type(param.orig_types, param.floating_char):
                                return np.dtype(param.floating_char).type(arg)
                            else:
                                return arg
                elif t == "?" and isinstance(arg, bool):
                    return arg
                elif t == "M":
                    try:
                        return to_np_datetime64(arg)
                    except Exception as e:
                        # don't raise an error, if this is the only annotation, the else block of the for loop will
                        # catch it and raise a TypeError
                        pass
                elif t == "U" and isinstance(arg, str):
                    return arg
            else:  # didn't return from inside the for loop
                if "O" in param.encoded_types:
                    return arg
                else:
                    raise TypeError(f"Argument {param.name!r}: {arg} is not compatible with annotation"
                                    f" {param.orig_types}")
        else:  # if no annotation or generic object, return arg
            return arg


def _convert_args(p_sig: _ParsedSignature, args: Tuple[Any, ...]) -> List[Any]:
    """ Convert all arguments to the types specified by the annotations.
    Given that the number of arguments and the number of parameters may not match (in the presence of keyword,
    var-positional, or var-keyword parameters), we have the following rules:
     If the number of arguments is less than the number of parameters, the remaining parameters are left as is.
     If the number of arguments is greater than the number of parameters, the extra arguments are left as is.

    Python's function call mechanism will raise an exception if it can't resolve the parameters with the arguments.
    """
    converted_args = [_convert_arg(param, arg) for param, arg in zip(p_sig.params, args)]
    converted_args.extend(args[len(converted_args):])
    return converted_args


def _py_udf(fn: Callable):
    """A decorator that acts as a transparent translator for Python UDFs used in Deephaven query formulas between
    Python and Java. This decorator is intended for use by the Deephaven query engine and should not be used by
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
    sig_str_vectorization = re.sub(r"[\[N,]", "", p_sig.encoded)
    return_array = p_sig.ret_annotation.has_array
    ret_dtype = dtypes.from_np_dtype(np.dtype(p_sig.ret_annotation.encoded_type[-1]))

    @wraps(fn)
    def wrapper(*args, **kwargs):
        converted_args = _convert_args(p_sig, args)
        # kwargs are not converted because they are not used in the UDFs
        ret = fn(*converted_args, **kwargs)
        if return_array:
            return dtypes.array(ret_dtype, ret)
        elif ret_dtype == dtypes.PyObject:
            return ret
        else:
            return _scalar(ret, ret_dtype)

    wrapper.j_name = ret_dtype.j_name
    real_ret_dtype = _BUILDABLE_ARRAY_DTYPE_MAP.get(ret_dtype, dtypes.PyObject) if return_array else ret_dtype

    if hasattr(ret_dtype.j_type, 'jclass'):
        j_class = real_ret_dtype.j_type.jclass
    else:
        j_class = real_ret_dtype.qst_type.clazz()

    wrapper.return_type = j_class
    wrapper.signature = sig_str_vectorization

    return wrapper


def _dh_vectorize(fn):
    """A decorator to vectorize a Python function used in Deephaven query formulas and invoked on a row basis.

    If this annotation is not used on a query function, the Deephaven query engine will make an effort to vectorize
    the function. If vectorization is not possible, the query engine will use the original, non-vectorized function.
    If this annotation is used on a function, the Deephaven query engine will use the vectorized function in a query,
    or an error will result if the function can not be vectorized.

    When this decorator is used on a function, the number and type of input and output arguments are changed.
    These changes are only intended for use by the Deephaven query engine. Users are discouraged from using
    vectorized functions in non-query code, since the function signature may change in future versions.

    The current vectorized function signature includes (1) the size of the input arrays, (2) the output array,
    and (3) the input arrays.
    """
    p_sig = _parse_signature(fn)
    return_array = p_sig.ret_annotation.has_array
    ret_dtype = dtypes.from_np_dtype(np.dtype(p_sig.ret_annotation.encoded_type[-1]))

    @wraps(fn)
    def wrapper(*args):
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
                converted_args = _convert_args(p_sig, scalar_args)
                ret = fn(*converted_args)
                if return_array:
                    chunk_result[i] = dtypes.array(ret_dtype, ret)
                else:
                    chunk_result[i] = _scalar(ret, ret_dtype)
        else:
            for i in range(chunk_size):
                ret = fn()
                if return_array:
                    chunk_result[i] = dtypes.array(ret_dtype, ret)
                else:
                    chunk_result[i] = _scalar(ret, ret_dtype)

        return chunk_result

    wrapper.callable = fn
    wrapper.dh_vectorized = True

    if test_vectorization:
        global vectorized_count
        vectorized_count += 1

    return wrapper