import inspect
import numba
import numpy

from .. import java

# note: these numba "types" are not basic python types
__java_type_to_numba_type = {
    java.boolean: numba.boolean,
    java.byte: numba.byte,
    java.short: numba.int16,
    java.int: numba.int32,
    java.long: numba.int64,
    java.float: numba.float32,
    java.double: numba.float64
}

# note: these numba "types" are not basic python types
__numpy_type_to_numba_type = {
    numpy.bool_: numba.boolean,
    numpy.int8: numba.byte,
    numpy.int16: numba.int16,
    numpy.int32: numba.int32,
    numpy.int64: numba.int64,
    numpy.float32: numba.float32,
    numpy.float64: numba.float64
}


def is_numpy_type(x):
    return isinstance(x, type) and issubclass(x, numpy.generic)


def get_numba_type(x):
    if java.is_primitive_type(x):
        return_type = __java_type_to_numba_type[x]
        if not return_type:
            raise TypeError(f"Unable to find numba type for java primitive {x}")
        return return_type

    if is_numpy_type(x):
        return_type = __numpy_type_to_numba_type[x]
        if not return_type:
            raise TypeError(f"Unable to find numba type for numpy type {x}")
        return return_type

    return x


def create_numba_signature(signature: inspect.Signature) -> numba.core.typing.templates.Signature:
    '''
    Uses annotation hints to create a numba signature.

    :param signature: the signature
    :return: the numba signature
    '''
    arg_annotations = [get_numba_type(param.annotation) for param in signature.parameters.values()]
    # this creates a "strongly-typed" signature like numba.int32(numba.int64,numba.float64)
    return get_numba_type(signature.return_annotation)(*arg_annotations)


def vectorize(_func=None, *, identity=None, nopython=True, target='cpu', cache=False):
    '''
    Returns a new Deephaven-compatible function (or decorator) based off of
    numba.vectorize.

    :param _func: the function
    :return: the Deephaven-compatible function
    '''

    def inner_decorator(func):
        signature = inspect.signature(func)
        numba_signature = create_numba_signature(signature)
        numba_vectorized = numba.vectorize([numba_signature], identity=identity, nopython=nopython, target=target,
                                           cache=cache)(func)
        numba_vectorized.__dh_return_type__ = signature.return_annotation
        numba_vectorized.__dh_is_vectorized__ = True
        return numba_vectorized

    if _func is None:
        return inner_decorator
    else:
        return inner_decorator(_func)


def njit(_func=None, *, nopython=True, cache=False, parallel=False, fastmath=False):
    '''
    Returns a new Deephaven-compatible function (or decorator) based off of
    numba.jit.

    :param _func: the function
    :return: the Deephaven-compatible function
    '''

    def inner_decorator(func):
        signature = inspect.signature(func)
        numba_signature = create_numba_signature(signature)
        numba_jitted = numba.jit(numba_signature, nopython=nopython, cache=cache, parallel=parallel, fastmath=fastmath)(
            func)
        numba_jitted.__dh_return_type__ = signature.return_annotation
        numba_jitted.__dh_is_vectorized__ = False
        return numba_jitted

    if _func is None:
        return inner_decorator
    else:
        return inner_decorator(_func)
